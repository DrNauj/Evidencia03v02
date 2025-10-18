import os
import sys
import time
import shutil
from django.utils import timezone
import findspark

# Inicializar entorno Spark (spark_utils maneja paths/venv)
from .spark_utils import configure_spark_env, get_venv_python
configure_spark_env()
venv_py = get_venv_python()
if venv_py:
    os.environ.setdefault('PYSPARK_PYTHON', venv_py)
    os.environ.setdefault('PYSPARK_DRIVER_PYTHON', venv_py)
findspark.init()

# Componentes Spark
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col, when
from pyspark import StorageLevel
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from .monitor import log_job_status, log_batch_result, log_error


def cleanup_spark_resources(spark, dataframes=None):
    if dataframes:
        for df in dataframes:
            if df is not None:
                try:
                    df.unpersist()
                except Exception:
                    pass
    if spark is not None:
        try:
            spark.stop()
        except Exception:
            pass


def update_job_status(job, status, message=None, progress=None):
    from django.db import transaction
    job.refresh_from_db()
    try:
        with transaction.atomic():
            job.status = status
            if message:
                job.error_message = message
            if progress is not None:
                try:
                    if getattr(job, 'total_records', 0):
                        pct = min(progress, 100) / 100.0
                        job.processed_records = int(pct * int(job.total_records))
                except Exception as e:
                    log_error(getattr(job, 'id', None), f"Error al actualizar progreso: {str(e)}", e)
            job.save()
        log_job_status(getattr(job, 'id', None), status, progress, message)
    except Exception as e:
        log_error(getattr(job, 'id', None), f"Error al actualizar estado del job: {str(e)}", e)
        print(f"Error crítico actualizando job {getattr(job, 'id', None)}: {e}")


def _create_spark_session():
    # Configuración básica y segura para tests locales
    return (
        SparkSession.builder
        .appName("BankDataProcessor")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.local.dir", os.path.abspath(os.path.join(os.getcwd(), "spark_tmp")))
        .master("local[1]")
        .getOrCreate()
    )


def _ensure_max_bins(classifier, bins):
    # Forzar y verificar maxBins de manera robusta
    try:
        try:
            classifier = classifier.set(maxBins=bins)
        except Exception:
            try:
                classifier.setMaxBins(bins)
            except Exception:
                pass
        try:
            actual = classifier.getMaxBins()
        except Exception:
            return classifier
        if actual != bins:
            raise ValueError(f"No se pudo establecer maxBins={bins} (actual={actual})")
    except Exception:
        raise
    return classifier


def process_bank_data(data_path, job):
    """Procesar datos bancarios usando PySpark MLlib"""
    update_job_status(job, 'processing', progress=0)

    if not os.path.exists(data_path):
        msg = f"Archivo de datos no encontrado: {data_path}"
        update_job_status(job, 'error', msg)
        job.completed_at = timezone.now()
        job.save()
        return

    try:
        from django.db import connections
        connections.close_all()
    except Exception:
        pass

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    local_hadoop = os.path.join(project_root, 'hadoop')
    if os.name == 'nt' and os.path.exists(local_hadoop):
        os.environ['HADOOP_HOME'] = local_hadoop
        os.environ['PATH'] = os.path.join(local_hadoop, 'bin') + os.pathsep + os.environ.get('PATH', '')

    spark = None
    try:
        os.makedirs(os.path.join(project_root, 'spark_tmp'), exist_ok=True)
    except Exception:
        pass

    try:
        spark = _create_spark_session()
        spark.sparkContext.setLogLevel('ERROR')
    except Exception as e:
        update_job_status(job, 'error', f"No se pudo inicializar Spark: {e}")
        job.completed_at = timezone.now()
        job.save()
        raise

    df = None
    df_with_idx = None

    try:
        schema = StructType([
            StructField("age", IntegerType(), True),
            StructField("job", StringType(), True),
            StructField("marital", StringType(), True),
            StructField("education", StringType(), True),
            StructField("default", StringType(), True),
            StructField("balance", IntegerType(), True),
            StructField("housing", StringType(), True),
            StructField("loan", StringType(), True),
            StructField("contact", StringType(), True),
            StructField("day", IntegerType(), True),
            StructField("month", StringType(), True),
            StructField("duration", IntegerType(), True),
            StructField("campaign", IntegerType(), True),
            StructField("pdays", IntegerType(), True),
            StructField("previous", IntegerType(), True),
            StructField("poutcome", StringType(), True),
            StructField("y", StringType(), True)
        ])

        delim = ','
        try:
            with open(data_path, 'r', encoding='utf-8', errors='ignore') as fh:
                first = fh.readline()
                if ';' in first and first.count(';') > first.count(','):
                    delim = ';'
        except Exception:
            delim = ','

        df = spark.read.option('header', 'true').option('delimiter', delim).schema(schema).csv(data_path)
        raw_df = df

        update_job_status(job, 'processing', "Contando registros...", 5)
        total_records = df.count()
        job.total_records = total_records
        job.save()

        update_job_status(job, 'processing', "Iniciando preprocesamiento...", 10)
        categorical_columns = ['job', 'marital', 'education', 'default', 'housing', 'loan', 'contact', 'month', 'poutcome']
        numeric_columns = ['age', 'balance', 'day', 'duration', 'campaign', 'pdays', 'previous']

        for c in categorical_columns:
            df = df.withColumn(c, col(c).cast('string'))
            df = df.withColumn(c, when(col(c).isNull() | (col(c) == ''), 'missing').otherwise(col(c)))
        for n in numeric_columns:
            df = df.withColumn(n, when(col(n).isNull(), 0).otherwise(col(n)))

        update_job_status(job, 'processing', "Preparando características...", 30)
        df = df.fillna({'y': 'no'})

        indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_index", handleInvalid='keep') for c in categorical_columns]
        df_indexed = df
        fitted_indexers = []
        for idx in indexers:
            try:
                m = idx.fit(df_indexed)
                df_indexed = m.transform(df_indexed)
                # Cast indexed column to double to remove categorical metadata
                out_col = idx.getOutputCol()
                try:
                    df_indexed = df_indexed.withColumn(out_col, col(out_col).cast('double'))
                except Exception:
                    pass
                fitted_indexers.append(m)
            except Exception as e:
                print(f"DEBUG: Error ajustando indexer {idx.getInputCol()}: {e}")

        label_indexer = StringIndexer(inputCol='y', outputCol='label', handleInvalid='keep')
        try:
            df_indexed = label_indexer.fit(df_indexed).transform(df_indexed)
        except Exception:
            df_indexed = df_indexed.withColumn('label', when(col('y') == 'yes', 1.0).otherwise(0.0))

        categorical_index_cols = [f"{c}_index" for c in categorical_columns]
        max_categories = 32
        for idx_col in categorical_index_cols:
            try:
                cnt = df_indexed.select(idx_col).distinct().count()
                if cnt > max_categories:
                    max_categories = cnt
            except Exception:
                continue
        needed_maxBins = max(max_categories, 32)
        safe_bins = max(needed_maxBins * 4, 1024)
        print(f"DEBUG: maxBins calculado: {safe_bins} (necesario: {needed_maxBins})")

        feature_columns = numeric_columns + [f"{c}_index" for c in categorical_columns]
        assembler = VectorAssembler(inputCols=feature_columns, outputCol='features')
        df_indexed = assembler.transform(df_indexed)
        df_indexed.persist(StorageLevel.MEMORY_AND_DISK)
        df = df_indexed

        # Debug: imprimir metadata de la columna 'features' (ayuda a diagnosticar maxBins/feature metadata)
        try:
            features_meta = df_indexed.schema['features'].metadata
            print(f"DEBUG: features metadata keys: {list(features_meta.keys())}")
            # Si existe ml_attr, imprimir resumen de atributos
            if 'ml_attr' in features_meta:
                print(f"DEBUG: ml_attr keys: {list(features_meta['ml_attr'].keys())}")
        except Exception as e:
            print(f"DEBUG: no se pudo leer metadata de features: {e}")

        try:
            update_job_status(job, 'processing', "Entrenando modelo global (train/test)...", 35)
            assembler_stage = VectorAssembler(inputCols=feature_columns, outputCol='features')

            df_for_training = df_indexed
            train_df, test_df = df_for_training.randomSplit([0.8, 0.2], seed=42)

            rf_params = {
                'labelCol': 'label',
                'featuresCol': 'features',
                'numTrees': 20,
                'maxDepth': 6,
                'maxBins': safe_bins,
                'impurity': 'gini',
                'seed': 42,
            }
            rf_final = RandomForestClassifier(**rf_params)
            try:
                rf_final = _ensure_max_bins(rf_final, safe_bins)
            except Exception as e:
                print(f"DEBUG: No se pudo forzar maxBins en global: {e}")

            # Debug: confirmar valor de maxBins antes de fit
            try:
                print(f"DEBUG: rf_final.getMaxBins() = {rf_final.getMaxBins()}")
            except Exception:
                pass

            rf_model = None
            try:
                rf_model = rf_final.fit(train_df)
                print("DEBUG: Modelo global entrenado")
            except Exception as e:
                print(f"DEBUG: Error entrenando modelo global: {e}")

            if rf_model is not None:
                try:
                    preds = rf_model.transform(test_df)
                    evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='accuracy')
                    acc = evaluator.evaluate(preds)
                    print(f"DEBUG: Precisión del modelo global: {acc:.4f}")
                except Exception as e:
                    print(f"DEBUG: Error evaluando modelo global: {e}")

        except Exception as e:
            print(f"DEBUG: Error en sección de entrenamiento global: {e}")

        update_job_status(job, 'processing', "Preparando procesamiento por lotes...", 40)
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number

        order_col = job.batch_key if job.batch_key in numeric_columns else f"{job.batch_key}_index"
        if order_col not in df.columns:
            error_msg = f"Columna de ordenamiento '{order_col}' no encontrada"
            update_job_status(job, 'error', error_msg)
            raise RuntimeError(error_msg)

        window = Window.orderBy(order_col)
        df_with_idx = df.withColumn('__row_num', row_number().over(window))
        df_with_idx.persist(StorageLevel.MEMORY_AND_DISK)

        total_batches = (total_records + job.batch_size - 1) // job.batch_size
        evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction')

        for batch_num in range(total_batches):
            progress = 50 + (batch_num / max(1, total_batches) * 45)
            update_job_status(job, 'processing', f"Procesando lote {batch_num + 1} de {total_batches}...", progress)

            start_idx = batch_num * job.batch_size + 1
            end_idx = min((batch_num + 1) * job.batch_size, total_records)
            batch_df = df_with_idx.filter((col('__row_num') >= start_idx) & (col('__row_num') <= end_idx)).drop('__row_num')

            try:
                if batch_df.rdd.isEmpty():
                    continue
            except Exception as e:
                print(f"DEBUG: Error verificando si batch está vacío: {e}")
                continue

            batch_max = 1024
            try:
                for idx_col in categorical_index_cols:
                    try:
                        ccount = batch_df.select(idx_col).distinct().count()
                        if ccount > batch_max:
                            batch_max = ccount
                    except Exception:
                        continue
                batch_max = max(batch_max * 2, 1024)
                print(f"DEBUG: Lote {batch_num + 1} - calculado batch_max={batch_max}")
            except Exception as e:
                print(f"DEBUG: Error calculando batch_max: {e}")
                batch_max = 4096

            rf = RandomForestClassifier(
                labelCol='label',
                featuresCol='features',
                numTrees=10,
                maxDepth=4,
                maxBins=batch_max,
                impurity='gini',
                seed=42,
            )
            try:
                rf = _ensure_max_bins(rf, batch_max)
            except Exception as e:
                print(f"DEBUG: No se pudo forzar maxBins en lote {batch_num + 1}: {e}")

            # Debug: imprimir maxBins y metadata del batch antes de fit
            try:
                print(f"DEBUG: batch rf.getMaxBins() = {rf.getMaxBins()}")
            except Exception:
                pass
            try:
                bmeta = batch_df.schema['features'].metadata
                print(f"DEBUG: batch features metadata keys: {list(bmeta.keys())}")
            except Exception as e:
                print(f"DEBUG: no se pudo leer metadata de features del batch: {e}")

            start_time = time.time()
            try:
                model = rf.fit(batch_df)
                predictions = model.transform(batch_df)

                accuracy = evaluator.evaluate(predictions, {evaluator.metricName: 'accuracy'})
                precision = evaluator.evaluate(predictions, {evaluator.metricName: 'weightedPrecision'})
                recall = evaluator.evaluate(predictions, {evaluator.metricName: 'weightedRecall'})
                f1 = evaluator.evaluate(predictions, {evaluator.metricName: 'f1'})

                confusion_matrix = predictions.groupBy('label', 'prediction').count().collect()
                conf_matrix_dict = {f"{int(r['label'])}-{int(r['prediction'])}": r['count'] for r in confusion_matrix}

                processed = batch_df.count()
                # Guardar resultados del lote de forma robusta frente a race/IntegrityError
                from processor.models import BatchResult
                from django.db import transaction
                from django.db.utils import IntegrityError

                defaults = {
                    'records_processed': processed,
                    'accuracy': float(accuracy),
                    'precision': float(precision),
                    'recall': float(recall),
                    'f1_score': float(f1),
                    'confusion_matrix': conf_matrix_dict,
                    'processing_time': time.time() - start_time,
                }

                def _save_batch_result_safe(job, batch_number, defaults):
                    """Try to atomically create or update a BatchResult. On IntegrityError
                    (e.g., UNIQUE constraint), attempt a safe recovery: fetch and update
                    existing row, or retry creation once more.
                    """
                    try:
                        with transaction.atomic():
                            # Prefer update_or_create inside an atomic block
                            obj, created = BatchResult.objects.update_or_create(
                                job=job,
                                batch_number=batch_number,
                                defaults=defaults,
                            )
                            return obj
                    except IntegrityError:
                        # Broken by concurrent insert: try to recover by fetching existing
                        try:
                            existing = BatchResult.objects.get(job=job, batch_number=batch_number)
                            # Use queryset.update to avoid further transactional issues
                            BatchResult.objects.filter(pk=existing.pk).update(**defaults)
                            return BatchResult.objects.get(pk=existing.pk)
                        except BatchResult.DoesNotExist:
                            # If it does not exist (race), attempt a non-atomic create and ignore duplicate errors
                            try:
                                return BatchResult.objects.create(job=job, batch_number=batch_number, **defaults)
                            except IntegrityError:
                                # Give up gracefully
                                return None
                    except Exception:
                        # Re-raise unexpected exceptions for higher-level handling
                        raise

                _save_batch_result_safe(job, batch_num + 1, defaults)
                job.processed_records += processed
                job.save()
                print(f"Lote {batch_num + 1} completado: {processed} registros")

            except Exception as e:
                print(f"DEBUG: Error en lote {batch_num + 1}: {e}")
                continue

        update_job_status(job, 'completed', "Procesamiento completado", 100)
        job.completed_at = timezone.now()
        job.save()

    except Exception as e:
        error_msg = f"Error en el procesamiento: {e}"
        update_job_status(job, 'error', error_msg)
        job.completed_at = timezone.now()
        job.save()
        raise

    finally:
        cleanup_spark_resources(spark, [df, df_with_idx])
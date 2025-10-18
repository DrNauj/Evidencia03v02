"""Módulo de monitoreo y logging unificado

Proporciona funcionalidades para:
- Monitoreo de rendimiento y recursos
- Logging de jobs y procesos
- Métricas de Spark
"""

import logging
import time
import os
from functools import wraps
from datetime import datetime
from django.utils import timezone

# Configurar logging principal
log_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
log_file = os.path.join(log_dir, 'logs', 'processing.log')

# Crear directorio de logs si no existe
os.makedirs(os.path.dirname(log_file), exist_ok=True)

# Logger para procesamiento general
logger = logging.getLogger('processing')
logger.setLevel(logging.DEBUG)

# Handler para archivo
fh = logging.FileHandler(log_file)
fh.setLevel(logging.DEBUG)

# Handler para consola
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# Formato unificado
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)

# Logger específico para Spark
spark_logger = logging.getLogger('spark')
spark_handler = logging.FileHandler('spark_performance.log')
spark_handler.setFormatter(formatter)
spark_logger.addHandler(spark_handler)
spark_logger.addHandler(logging.StreamHandler())

def monitor_performance(func):
    """Decorador para monitorear el rendimiento de funciones"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        
        # Obtener memoria inicial
        try:
            import psutil
        except Exception:
            logger.warning(
                "psutil no está disponible. Se usará un fallback que devuelve 0 MB. "
                "Instale psutil (`pip install psutil`) y añádalo a requirements.txt "
                "para obtener mediciones de memoria reales."
            )

            class _DummyMI:
                def __init__(self):
                    self.rss = 0

            class _DummyProcess:
                def __init__(self, pid):
                    self.pid = pid

                def memory_info(self):
                    return _DummyMI()

            class _DummyPsutilModule:
                Process = _DummyProcess

            psutil = _DummyPsutilModule()

        process = psutil.Process(os.getpid())
        mem_before = process.memory_info().rss / 1024 / 1024  # MB
        
        try:
            result = func(*args, **kwargs)
            success = True
        except Exception as e:
            success = False
            logger.error(f"Error en {func.__name__}: {str(e)}")
            raise
        finally:
            end_time = time.time()
            duration = end_time - start_time
            mem_after = process.memory_info().rss / 1024 / 1024
            mem_diff = mem_after - mem_before
            
            logger.info(
                f"Función: {func.__name__}\n"
                f"Duración: {duration:.2f} segundos\n"
                f"Memoria inicial: {mem_before:.1f} MB\n"
                f"Memoria final: {mem_after:.1f} MB\n"
                f"Diferencia memoria: {mem_diff:+.1f} MB\n"
                f"Estado: {'Éxito' if success else 'Error'}"
            )
        
        return result
    return wrapper

def log_spark_metrics(spark, stage_name):
    """Registra métricas de Spark"""
    try:
        metrics = {
            "Ejecutores activos": spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size(),
            "Tareas pendientes": len(spark.sparkContext._jsc.sc().dagScheduler().waitingStages()),
            "Tareas activas": len(spark.sparkContext._jsc.sc().dagScheduler().runningStages()),
        }
        
        spark_logger.info(f"Métricas Spark - {stage_name}:")
        for metric, value in metrics.items():
            spark_logger.info(f"{metric}: {value}")
    except Exception as e:
        spark_logger.warning(f"No se pudieron obtener métricas de Spark: {str(e)}")

def log_job_status(job_id, status, progress=None, message=None):
    """Registra actualizaciones de estado del job"""
    logger.info(f"Job {job_id} - Status: {status} - Progress: {progress}% - Message: {message}")

def log_batch_result(job_id, batch_number, metrics):
    """Registra métricas de un lote procesado"""
    logger.info(f"Job {job_id} - Batch {batch_number} - Metrics: {metrics}")

def log_error(job_id, error_message, exception=None):
    """Registra errores en el procesamiento"""
    logger.error(f"Job {job_id} - Error: {error_message}", exc_info=exception)

def update_job_progress(job, current, total, stage=None):
    """Actualiza el progreso del trabajo"""
    try:
        progress = (current / total) * 100 if total > 0 else 0
        job.processed_records = int(current)
        if stage:
            job.current_stage = stage
        job.last_update = timezone.now()
        job.save()
        
        log_job_status(job.id, job.status, progress, stage)
        
    except Exception as e:
        log_error(job.id, f"Error actualizando progreso: {str(e)}", e)
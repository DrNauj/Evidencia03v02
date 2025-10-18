from django.core.management.base import BaseCommand, CommandError
import os
try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - optional in some envs
    pd = None

class Command(BaseCommand):
    help = 'Load persisted PipelineModel and predict for a given CSV input path'

    def add_arguments(self, parser):
        parser.add_argument('csv_path', type=str)

    def handle(self, *args, **options):
        csv_path = options['csv_path']
        if not os.path.exists(csv_path):
            raise CommandError(f'File not found: {csv_path}')

        try:
            from pyspark.sql import SparkSession  # type: ignore
            from pyspark.ml import PipelineModel  # type: ignore
        except Exception:
            raise CommandError('PySpark not available in this environment')

        spark = SparkSession.builder.master('local[1]').appName('PredictInput').getOrCreate()
        model_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'models', 'pipeline_model'))
        if not os.path.exists(model_dir):
            raise CommandError('Persisted model not found at %s' % model_dir)

        model = PipelineModel.load(model_dir)

        # read csv with header
        df = spark.read.option('header', 'true').csv(csv_path)
        preds = model.transform(df)
        # collect predictions
        rows = preds.select('prediction').toPandas()
        for i, row in rows.iterrows():
            print(row['prediction'])

        spark.stop()

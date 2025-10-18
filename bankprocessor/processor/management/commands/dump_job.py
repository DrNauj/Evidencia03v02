"""Comando para volcar información de un job"""

import os
from django.core.management.base import BaseCommand
from processor.models import ProcessingJob

class Command(BaseCommand):
    help = 'Muestra información detallada del último job procesado'

    def handle(self, *args, **kwargs):
        j = ProcessingJob.objects.order_by('-started_at').first()
        
        self.stdout.write('=== ENVIRONMENT ===')
        self.stdout.write(f'JAVA_HOME={os.environ.get("JAVA_HOME")}')
        self.stdout.write(f'HADOOP_HOME={os.environ.get("HADOOP_HOME")}')
        self.stdout.write(f'PYSPARK_PYTHON={os.environ.get("PYSPARK_PYTHON")}')
        self.stdout.write(f'PYSPARK_DRIVER_PYTHON={os.environ.get("PYSPARK_DRIVER_PYTHON")}')
        
        data_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'data', 'bank.csv')
        self.stdout.write(f'DATA EXISTS: {os.path.exists(data_path)}')
        self.stdout.write('')
        
        if not j:
            self.stdout.write(self.style.WARNING('No ProcessingJob found'))
            return

        self.stdout.write('=== LAST JOB ===')
        self.stdout.write(f'id: {j.id}')
        self.stdout.write(f'status: {j.status}')
        self.stdout.write(f'started_at: {j.started_at}')
        self.stdout.write(f'processed_records: {j.processed_records}')
        self.stdout.write(f'total_records: {j.total_records}')
        self.stdout.write('error_message (repr):')
        self.stdout.write('---START---')
        self.stdout.write(repr(j.error_message))
        self.stdout.write('---END---')
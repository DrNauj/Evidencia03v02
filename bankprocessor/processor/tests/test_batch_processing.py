"""
Tests para el procesamiento por lotes y streaming según la documentación.
"""
import unittest
from django.test import TestCase
from processor.streaming import StreamProcessor
from processor.models import ProcessingJob, BatchResult
import pandas as pd
import time
import os

class TestBatchProcessing(TestCase):
    def setUp(self):
        self.stream_processor = StreamProcessor()
        # Crear un conjunto de datos de prueba de 1000 registros
        self.test_data = pd.DataFrame({
            'age': [30] * 1000,
            'job': ['admin'] * 1000,
            'marital': ['single'] * 1000,
            'education': ['university.degree'] * 1000,
            'default': ['no'] * 1000,
            'balance': [1500] * 1000,
            'housing': ['yes'] * 1000,
            'loan': ['no'] * 1000,
            'contact': ['cellular'] * 1000,
            'day': [15] * 1000,
            'month': ['may'] * 1000,
            'duration': [180] * 1000,
            'campaign': [3] * 1000,
            'pdays': [-1] * 1000,
            'previous': [0] * 1000,
            'poutcome': ['unknown'] * 1000
        })

    def test_batch_processing_performance(self):
        """
        Test que verifica el rendimiento del procesamiento por lotes según documentación.
        """
        # Crear un archivo CSV temporal con 1000 registros
        temp_csv = 'test_batch.csv'
        self.test_data.to_csv(temp_csv, index=False)
        
        # Crear un job de procesamiento
        job = ProcessingJob.objects.create(
            input_file=temp_csv,
            status='PENDING'
        )
        
        # Medir el tiempo de procesamiento
        start_time = time.time()
        self.stream_processor.process_job(job.id)
        end_time = time.time()
        
        processing_time = end_time - start_time
        
        # Limpiar archivo temporal
        os.remove(temp_csv)
        
        # Verificar que el procesamiento cumple con el criterio de tiempo
        self.assertLess(processing_time, 30, "El procesamiento de 1000 registros debe tomar menos de 30 segundos")
        
        # Verificar que todos los registros fueron procesados
        results = BatchResult.objects.filter(job=job)
        self.assertEqual(results.count(), 1000, "Todos los registros deben ser procesados")
"""
Tests para las funcionalidades web y API según la documentación.
"""
import unittest
from django.test import TestCase, Client
from django.urls import reverse
from processor.models import ProcessingJob
import json
import time

class TestWebInterface(TestCase):
    def setUp(self):
        self.client = Client()
        self.predict_url = reverse('predict')
        self.process_data_url = reverse('process_data')
        
        # Datos de prueba para el formulario
        self.form_data = {
            'age': '30',
            'job': 'admin',
            'marital': 'single',
            'education': 'university.degree',
            'default': 'no',
            'balance': '1500',
            'housing': 'yes',
            'loan': 'no',
            'contact': 'cellular',
            'day': '15',
            'month': 'may',
            'duration': '180',
            'campaign': '3',
            'pdays': '-1',
            'previous': '0',
            'poutcome': 'unknown'
        }

    def test_individual_prediction_latency(self):
        """
        Test que verifica la latencia de predicción individual según documentación.
        """
        start_time = time.time()
        response = self.client.post(self.predict_url, self.form_data)
        end_time = time.time()
        
        latency = end_time - start_time
        self.assertLess(latency, 5, "La latencia de predicción individual debe ser menor a 5 segundos")
        self.assertEqual(response.status_code, 200)
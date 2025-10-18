"""
Tests para el componente de Machine Learning según la documentación.
"""
import unittest
from django.test import TestCase
from processor.ml_processor import MLProcessor
from processor.models import ProcessingJob
import pandas as pd
import os

class TestMLModel(TestCase):
    def setUp(self):
        # Configurar el entorno de pruebas
        self.ml_processor = MLProcessor()
        self.test_data = pd.DataFrame({
            'age': [30],
            'job': ['admin'],
            'marital': ['single'],
            'education': ['university.degree'],
            'default': ['no'],
            'balance': [1500],
            'housing': ['yes'],
            'loan': ['no'],
            'contact': ['cellular'],
            'day': [15],
            'month': ['may'],
            'duration': [180],
            'campaign': [3],
            'pdays': [-1],
            'previous': [0],
            'poutcome': ['unknown']
        })

    def test_model_accuracy(self):
        """
        Test que verifica la precisión del modelo según documentación.
        """
        # Cargar datos de prueba del dataset bank.csv
        data_path = os.path.join('data', 'bank.csv')
        test_data = pd.read_csv(data_path, sep=';')
        
        # Entrenar y evaluar el modelo
        accuracy = self.ml_processor.evaluate_model(test_data)
        
        # Verificar que cumple con el criterio de aceptación
        self.assertGreaterEqual(accuracy, 0.80, "La precisión del modelo debe ser mayor o igual a 80%")
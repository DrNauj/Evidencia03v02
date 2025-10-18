from django.db import models
from django.utils import timezone

class ProcessingJob(models.Model):
    """Modelo para rastrear el estado del procesamiento por lotes"""
    
    STATUS_CHOICES = [
        ('pending', 'Pendiente'),
        ('processing', 'Procesando'),
        ('completed', 'Completado'),
        ('error', 'Error')
    ]
    
    batch_key = models.CharField(max_length=50, help_text='Columna usada para fragmentación')
    batch_size = models.IntegerField(help_text='Número de registros por lote')
    total_records = models.IntegerField(default=0)
    processed_records = models.IntegerField(default=0)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    error_message = models.TextField(blank=True, null=True)
    started_at = models.DateTimeField(default=timezone.now)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['-started_at']

    def __str__(self):
        return f'Job {self.id} - {self.get_status_display()} ({self.batch_key})'

    @property
    def progress(self):
        """Calcula el progreso como porcentaje"""
        if self.total_records == 0:
            return 0
        return round((self.processed_records / self.total_records) * 100, 2)

class BatchResult(models.Model):
    """Modelo para almacenar resultados de cada lote procesado"""
    
    job = models.ForeignKey(ProcessingJob, on_delete=models.CASCADE, related_name='results')
    batch_number = models.IntegerField(default=0)  # Valor predeterminado para evitar errores NOT NULL
    records_processed = models.IntegerField(default=0)  # Valor predeterminado para evitar error NOT NULL
    accuracy = models.FloatField(null=True, blank=True)  # Permitir nulos para casos de error
    precision = models.FloatField(null=True, blank=True)
    recall = models.FloatField(null=True, blank=True)
    f1_score = models.FloatField(null=True, blank=True)
    confusion_matrix = models.JSONField(null=True, blank=True)
    processing_time = models.FloatField(help_text='Tiempo de procesamiento en segundos', null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    # Campos adicionales para tests
    batch_key = models.CharField(max_length=50, blank=True, null=True)
    batch_value = models.CharField(max_length=100, blank=True, null=True)
    total_records = models.IntegerField(default=0)
    positive_records = models.IntegerField(default=0)
    features = models.JSONField(null=True, blank=True)

    class Meta:
        ordering = ['batch_number']
        unique_together = ['job', 'batch_number']

    def __str__(self):
        return f'Batch {self.batch_number} de Job {self.job_id}'


class StreamingPrediction(models.Model):
    """Almacena una predicción realizada por el worker de streaming.

    - input_json: JSON con las features originales
    - prediction: etiqueta predicha (float o int)
    - probability: array/JSON con probabilidades (si disponible)
    - source_file: nombre del archivo csv que generó la predicción (opcional)
    """
    input_json = models.JSONField(default=dict)  # Valor predeterminado para evitar error NOT NULL
    prediction = models.CharField(max_length=10, blank=True, null=True)  # Cambiado a CharField para almacenar 'yes'/'no'
    probability = models.JSONField(null=True, blank=True)
    source_file = models.CharField(max_length=255, blank=True, null=True)
    
    # Campos adicionales requeridos por los tests
    input_file = models.CharField(max_length=255, blank=True, null=True)  
    features = models.JSONField(null=True, blank=True)
    processed_at = models.DateTimeField(default=timezone.now)
    created_at = models.DateTimeField(auto_now_add=True)

    # Campos adicionales para test_prediction_storage
    input_file = models.CharField(max_length=255, blank=True, null=True)  
    features = models.JSONField(null=True, blank=True)
    processed_at = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f'StreamPred {self.id} - pred={self.prediction} at {self.created_at}'

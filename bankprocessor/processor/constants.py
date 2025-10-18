"""Constantes usadas en el proyecto"""

import os

# Estados de procesamiento
JOB_STATUS_CHOICES = [
    ('pending', 'Pendiente'),
    ('running', 'En ejecución'),
    ('completed', 'Completado'),
    ('failed', 'Fallido'),
    ('terminated', 'Terminado'),
]

# Directorios de streaming (ubicados bajo la carpeta `media/` según documentación)
INPUT_STREAMING_DIR = os.path.join('media', 'input_streaming')
OUTPUT_STREAMING_DIR = os.path.join('media', 'output_streaming')
STREAM_CHECKPOINT_DIR = os.path.join('media', 'stream_checkpoint')

# Directorios de modelos
MODELS_DIR = 'models'
PIPELINE_MODEL_DIR = 'pipeline_model'

# Configuración de batch
DEFAULT_BATCH_SIZE = 1000
MAX_BATCH_SIZE = 10000

# Configuración de streaming
STREAMING_TRIGGER_INTERVAL = '10 seconds'
MAX_FILES_PER_TRIGGER = 1
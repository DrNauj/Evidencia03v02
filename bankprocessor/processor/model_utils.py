"""Utilidades para serialización/deserialización de modelos"""

import os
import pickle
import threading
import numpy as np
from pyspark.sql.types import _parse_datatype_json_string
from pyspark.ml.util import MLReader, MLWriter

class SafeUnpickler:
    """Permite deserializar objetos pickle de forma segura"""
    ALLOWED_TYPES = {
        'numpy.dtype',
        'numpy.core.multiarray._reconstruct',
        'numpy.ndarray',
        'collections.OrderedDict',
        '_collections_abc.dict_keys',
    }
    
    @classmethod
    def loads(cls, data):
        """Deserializa datos asegurando que sólo se cargan tipos permitidos"""
        def find_class(module, name):
            # Sólo permitir tipos seguros conocidos
            fullname = f"{module}.{name}"
            if any(fullname.startswith(allowed) for allowed in cls.ALLOWED_TYPES):
                if module == "numpy":
                    return getattr(np, name)
                # Añadir más módulos permitidos según necesidad
            raise pickle.UnpicklingError(f"Tipo global no permitido: {module}.{name}")
            
        unpickler = pickle.Unpickler(data)
        unpickler.find_class = find_class
        return unpickler.load()

def clean_thread_locks(obj, _seen=None):
    """
    Limpia objetos thread.Lock(), thread-locals, sockets y otros objetos no serializables 
    de un objeto antes de serializarlo.
    Maneja recursión con un conjunto de objetos ya vistos.
    """
    # Inicializar conjunto de objetos vistos si es la primera llamada
    if _seen is None:
        _seen = set()

    # Si el objeto ya fue procesado, retornarlo como está para evitar recursión
    obj_id = id(obj)
    if obj_id in _seen:
        return obj
    
    # Lista de nombres de tipos que no deben tocarse o deben eliminarse
    SKIP_TYPES = {
        'Lock', 'RLock', '_RLock', '_local', '_thread._local',  # Thread-related
        'socket', '_socket.socket',  # Socket-related
        'module', 'function', 'builtin_function_or_method',  # Module/function types
        'SparkContext', 'py4j.java_gateway',  # Spark-related
    }
    
    def is_skip_type(obj):
        """Comprueba si el objeto es de un tipo que debe omitirse"""
        obj_type = type(obj)
        type_name = obj_type.__name__
        type_module = getattr(obj_type, '__module__', '')
        type_fullname = f"{type_module}.{type_name}"
        
        # Verificar nombres de tipo y módulos específicos
        return (type_name in SKIP_TYPES or 
                type_fullname in SKIP_TYPES or 
                'thread' in type_fullname.lower() or
                'socket' in type_fullname.lower() or
                'py4j' in type_fullname.lower() or
                isinstance(obj, (type, type(clean_thread_locks))))
    
    # Marcar objeto como visto
    _seen.add(obj_id)
    
    try:
        # Manejar tipos específicos que necesitan atención especial
        if isinstance(obj, (str, int, float, bool, type(None), bytes, bytearray)):
            return obj
        
        # Si es un tipo que debe omitirse, retornar None
        if is_skip_type(obj):
            return None
            
        # Manejar colecciones
        if isinstance(obj, (list, tuple)):
            return type(obj)(clean_thread_locks(x, _seen) for x in obj)
        if isinstance(obj, dict):
            return {k: clean_thread_locks(v, _seen) for k, v in obj.items()}
        if isinstance(obj, set):
            return {clean_thread_locks(x, _seen) for x in obj}
            
        # Manejar objetos con __dict__
        if hasattr(obj, '__dict__'):
            try:
                # Intentar crear una copia limpia del objeto
                from copy import copy
                new_obj = copy(obj)
                
                # Limpiar atributos
                new_dict = {}
                for k, v in vars(obj).items():
                    if not is_skip_type(v):
                        try:
                            cleaned_value = clean_thread_locks(v, _seen)
                            if cleaned_value is not None:
                                new_dict[k] = cleaned_value
                        except:
                            continue
                new_obj.__dict__ = new_dict
                return new_obj
            except:
                # Si la copia falla, intentar crear un dict con los atributos limpios
                return {k: clean_thread_locks(v, _seen) 
                       for k, v in vars(obj).items() 
                       if not is_skip_type(v)}
                
        # Para cualquier otro tipo de objeto, retornar una versión string si es posible
        try:
            return str(obj)
        except:
            return None
            
    except Exception as e:
        # Si algo falla completamente, retornar None
        return None

def save_pipeline_model(model, path):
    """
    Guarda un PipelineModel limpiando locks y usando pickle
    """
    # Limpiar locks antes de serializar
    clean_model = clean_thread_locks(model)
    
    # Guardar usando pickle
    os.makedirs(path, exist_ok=True)
    model_path = os.path.join(path, 'pipeline_model.pkl')
    with open(model_path, 'wb') as f:
        pickle.dump(clean_model, f)

def load_pipeline_model(path):
    """
    Carga un PipelineModel guardado
    """
    model_path = os.path.join(path, 'pipeline_model.pkl')
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"No se encontró el modelo en {model_path}")
        
    with open(model_path, 'rb') as f:
        model = SafeUnpickler.loads(f)
        
    return model

# Exponer PipelineModel como referencia para que tests puedan parchearlo
# En runtime normalmente se usa pyspark.ml.PipelineModel, pero aquí
# dejamos un alias que los tests pueden mockear: `processor.model_utils.PipelineModel`.
PipelineModel = None

def load_pipeline_model_safe(path):
    """Wrapper que intenta cargar con load_pipeline_model y captura FileNotFoundError"""
    try:
        return load_pipeline_model(path)
    except FileNotFoundError:
        return None
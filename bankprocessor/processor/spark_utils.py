"""Utilidades para configuración de entorno Spark"""

import os
import sys
"""Utilidades para configuración de entorno Spark"""

import os
import sys
import shutil
import findspark
findspark.init()
from pyspark.sql import SparkSession


def get_spark(app_name='BankProcessor'):
    """Obtiene una sesión de Spark con la configuración estándar"""
    # Diagnóstico de Java/Hadoop antes de crear SparkSession
    print("DEBUG: java.library.path antes de SparkSession:", os.environ.get('HADOOP_OPTS', ''))
    print("DEBUG: HADOOP_HOME:", os.environ.get('HADOOP_HOME', ''))
    # Intentar localizar hadoop bin para pasarle a la JVM
    hadoop_home = os.environ.get('HADOOP_HOME') or os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'hadoop'))
    hadoop_bin = os.path.join(hadoop_home, 'bin') if hadoop_home else None

    extra_java_opts = os.environ.get('HADOOP_OPTS', '')
    if hadoop_bin and os.path.exists(hadoop_bin):
        # Asegurar PATH y opciones JVM para que la JVM encuentre winutils/native libs
        paths = os.environ.get('PATH', '').split(os.pathsep)
        if hadoop_bin not in paths:
            os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')
        # Wrap path in quotes to prevent Windows command-line splitting on spaces
        extra_java_opts = f"-Djava.library.path=\"{hadoop_bin}\""

    # También pasar la ruta nativa a los procesos driver/executor
    driver_java_opts = extra_java_opts
    exec_java_opts = extra_java_opts

    spark = SparkSession.builder \
        .master('local[1]') \
        .appName(app_name) \
        .config('spark.python.worker.reuse', 'false') \
        .config('spark.driver.host', 'localhost') \
        .config('spark.driver.bindAddress', 'localhost') \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.sql.legacy.timeParserPolicy', 'LEGACY') \
        .config('spark.sql.execution.arrow.enabled', 'true') \
        .config('spark.driver.extraClassPath', os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'hadoop', 'lib', '*'))) \
        .config('spark.driver.extraJavaOptions', driver_java_opts) \
        .config('spark.executor.extraJavaOptions', exec_java_opts) \
        .getOrCreate()
    return spark


def get_spark_with_retry(app_name='BankProcessor', retries=2, delay=0.5):
    """Intentar obtener SparkSession con reintentos para mitigar fallos del gateway Java."""
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return get_spark(app_name)
        except Exception as e:
            last_exc = e
            try:
                import time
                time.sleep(delay)
            except Exception:
                pass
    # If still failing, re-raise the last exception
    raise last_exc


def ensure_directory_exists(path):
    """Crea un directorio si no existe y asegura permisos en Windows"""
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
        if os.name == 'nt':  # Windows
            import subprocess
            try:
                subprocess.run(['icacls', path, '/grant', 'Everyone:(OI)(CI)F', '/T'], capture_output=True)
            except Exception:
                pass


def configure_spark_env():
    """Configura variables de entorno para Spark en Windows.

    Esta función establece HADOOP_HOME y JAVA_HOME básicos y garantiza que
    PYSPARK_PYTHON/PYSPARK_DRIVER_PYTHON apunten al intérprete adecuado.
    
    También asegura la existencia de directorios críticos con permisos apropiados.
    """
    # Crear directorios críticos con permisos apropiados
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    try:
        from .constants import INPUT_STREAMING_DIR, OUTPUT_STREAMING_DIR, STREAM_CHECKPOINT_DIR
        critical_dirs = [
            os.path.join(project_root, INPUT_STREAMING_DIR),
            os.path.join(project_root, OUTPUT_STREAMING_DIR),
            os.path.join(project_root, STREAM_CHECKPOINT_DIR),
            os.path.join(project_root, 'models'),
            os.path.join(project_root, 'data'),
        ]
    except Exception:
        critical_dirs = [
            os.path.join(project_root, 'input_streaming'),
            os.path.join(project_root, 'output_streaming'),
            os.path.join(project_root, 'stream_checkpoint'),
            os.path.join(project_root, 'models'),
            os.path.join(project_root, 'data'),
        ]
    for d in critical_dirs:
        ensure_directory_exists(d)
    if os.name == 'nt':  # Windows
        # 1. Configurar HADOOP_HOME y PATH
        # Buscar hadoop en varias ubicaciones (env, repo root, bankprocessor/hadoop)
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        candidate_root_hadoop = os.path.join(repo_root, 'hadoop')
        candidate_bp_hadoop = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'hadoop'))
        hadoop_home = os.environ.get('HADOOP_HOME') or (candidate_root_hadoop if os.path.exists(candidate_root_hadoop) else candidate_bp_hadoop)
        os.environ['HADOOP_HOME'] = hadoop_home
        hadoop_bin = os.path.join(hadoop_home, 'bin')

        if not os.path.exists(hadoop_bin):
            os.makedirs(hadoop_bin, exist_ok=True)

        # Asegurarse que winutils.exe existe. Buscamos en dos ubicaciones posibles
        # dentro del proyecto: `bankprocessor/hadoop/bin` y `bankprocessor/processor/hadoop/bin`.
        winutils_path = os.path.join(hadoop_bin, 'winutils.exe')
        possible_winutils = [
            os.path.join(hadoop_home, 'bin', 'winutils.exe'),
            os.path.join(os.path.dirname(__file__), '..', 'hadoop', 'bin', 'winutils.exe'),
            os.path.join(os.path.dirname(__file__), 'hadoop', 'bin', 'winutils.exe'),
        ]
        found = None
        for pw in possible_winutils:
            if os.path.exists(pw):
                found = pw
                break
        if found:
            # Copiar a hadoop_bin si no existe allí (evita problemas con rutas relativas)
            try:
                if not os.path.exists(winutils_path):
                    shutil.copy2(found, winutils_path)
            except Exception:
                # Si no se puede copiar, seguir adelante; al menos HADOOP_HOME apunta a una ruta válida
                pass
        else:
            # Intentar buscar winutils también en la carpeta raíz del repo: ../hadoop/bin
            root_hadoop = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'hadoop', 'bin'))
            root_winutils = os.path.join(root_hadoop, 'winutils.exe')
            try:
                if os.path.exists(root_winutils) and not os.path.exists(winutils_path):
                    shutil.copy2(root_winutils, winutils_path)
                    found = winutils_path
            except Exception:
                pass

        # Configurar PATH para incluir winutils.exe
        # Asegurar que hadoop_bin está al inicio del PATH
        paths = os.environ['PATH'].split(';')
        paths = [p for p in paths if hadoop_bin.lower() not in p.lower()]  # Remover existente
        paths.insert(0, hadoop_bin)  # Añadir al principio
        os.environ['PATH'] = ';'.join(paths)

        # 2. Configurar permisos especiales de Hadoop en Windows
        # Quote the library path so JVM receives it as a single argument even if it contains spaces
        os.environ['HADOOP_OPTS'] = f"-Djava.library.path=\"{hadoop_bin}\""

        # Forzar configuración hadoop.home.dir a PATH absoluto
        os.environ['hadoop.home.dir'] = hadoop_home

        # 3. Detectar y configurar JAVA_HOME si no está configurado
        if 'JAVA_HOME' not in os.environ:
            possible_paths = [
                r"C:\Program Files\Eclipse Adoptium\jre-17.0.16.8-hotspot",
                r"C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot",
                r"C:\Program Files\BellSoft\LibericaJDK-17",
                r"C:\Program Files\Eclipse Adoptium\jdk-17",
                r"C:\Program Files\Eclipse Adoptium\jdk-11",
            ]
            for path in possible_paths:
                if os.path.exists(path):
                    os.environ['JAVA_HOME'] = path
                    os.environ['PATH'] = f"{os.path.join(path, 'bin')};{os.environ['PATH']}"
                    break

    # Asegurar que PySpark use el mismo intérprete para driver y workers.
    venv_python = get_venv_python()
    if venv_python:
        os.environ['PYSPARK_PYTHON'] = venv_python
        os.environ['PYSPARK_DRIVER_PYTHON'] = venv_python
    else:
        os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
        os.environ.setdefault('PYSPARK_DRIVER_PYTHON', sys.executable)


# Ejecutar configuración al importar el módulo para garantizar HADOOP_HOME/PATH
try:
    configure_spark_env()
except Exception:
    # No bloquear la importación si la configuración falla; los tests pueden ajustar.
    pass


def get_venv_python():
    """Detecta un intérprete Python dentro de un virtualenv del proyecto.

    Prioriza `venv` y `venv310` en el root del repositorio. Devuelve la ruta absoluta
    al `python.exe` si existe, o None si no se encuentra ninguno.
    """
    # Asumimos que la estructura es: <workspace-root>/bankprocessor/processor
    project_root_parent = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    candidates = ['venv', 'venv310']
    for name in candidates:
        candidate = os.path.join(project_root_parent, name, 'Scripts', 'python.exe')
        if os.path.exists(candidate):
            return candidate
    return None
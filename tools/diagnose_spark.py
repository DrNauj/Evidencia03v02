import os
import sys
import traceback

# Adjust path to import project modules
proj_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'bankprocessor'))
if proj_root not in sys.path:
    sys.path.insert(0, proj_root)

print('PWD:', os.getcwd())
print('Python executable:', sys.executable)

print('\nEnvironment variables:')
for v in ['JAVA_HOME', 'HADOOP_HOME', 'PATH']:
    print(f'{v} =', os.environ.get(v))

hadoop_bin = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'bankprocessor', 'hadoop', 'bin'))
print('\nExpected hadoop bin:', hadoop_bin)
if os.path.exists(hadoop_bin):
    print('Contents of hadoop/bin:')
    for f in os.listdir(hadoop_bin):
        print(' -', f)
else:
    print('hadoop/bin not found')

print('\njava -version output:')
try:
    import subprocess
    out = subprocess.run(['java', '-version'], capture_output=True, text=True)
    print('returncode', out.returncode)
    print('stdout:\n', out.stdout)
    print('stderr:\n', out.stderr)
except Exception as e:
    print('Failed to run java -version:', e)

# Try to import get_spark_with_retry
try:
    from processor.spark_utils import get_spark_with_retry
    print('\nImported get_spark_with_retry from processor.spark_utils')
except Exception as e:
    print('\nFailed to import get_spark_with_retry:', e)
    traceback.print_exc()

# Try to create Spark session
try:
    print('\nAttempting to create Spark session (get_spark_with_retry)...')
    spark = get_spark_with_retry('DiagTest', retries=1)
    print('SparkSession created: version', spark.version)
    spark.stop()
except Exception as e:
    print('SparkSession creation failed:')
    traceback.print_exc()

print('\nDone')

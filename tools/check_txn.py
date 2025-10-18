import os
import django
import traceback

import sys
import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'bankprocessor.settings')
# Add project root (workspace root) so 'bankprocessor' package is importable
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)
# Also ensure that the folder that contains the 'bankprocessor' package is on sys.path
bankprocessor_parent = os.path.abspath(os.path.join(repo_root, '..'))
if bankprocessor_parent not in sys.path:
    sys.path.insert(0, bankprocessor_parent)

try:
    django.setup()
    from processor.models import ProcessingJob
    from django.db import transaction

    # Clean any existing jobs
    ProcessingJob.objects.all().delete()

    job1 = ProcessingJob.objects.create(status='pending', batch_key='age', batch_size=1000)
    print('Created job1 id', job1.id)

    try:
        with transaction.atomic():
            try:
                job2 = ProcessingJob.objects.create(status='pending', batch_key='age', batch_size=1000)
                print('Created job2 id', job2.id)
                raise Exception('Forced error')
            except Exception as e:
                print('Inner exception raised:', e)
                raise
    except Exception as e:
        print('Outer caught exception:', e)

    print('Jobs count after atomic block:', ProcessingJob.objects.filter(batch_size=1000).count())
    for j in ProcessingJob.objects.filter(batch_size=1000):
        print('Job:', j.id, j.status)
except Exception:
    traceback.print_exc()

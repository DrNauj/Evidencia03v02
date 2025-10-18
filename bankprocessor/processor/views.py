# Imports de Django
from django.shortcuts import render, get_object_or_404, redirect
from django.contrib import messages
from django.views.generic import ListView, DetailView
from django.http import JsonResponse
from django.utils import timezone
from django.views.decorators.http import require_POST
from django.conf import settings
from django.urls import reverse_lazy

# Imports del sistema
import os
import sys
import time
import uuid
import csv
import subprocess
import threading

# Imports locales
from .streaming import StreamingWorker, get_spark, streaming_schema
from .model_utils import load_pipeline_model
from .forms import ProcessingConfigForm, CompareResultsForm, StreamingInputForm 
from .models import ProcessingJob, BatchResult, StreamingPrediction

# Error handlers
def handler404(request, exception=None):
    return render(request, 'processor/404.html', status=404)

def handler500(request, exception=None):
    return render(request, 'processor/500.html', status=500)

def job_status(request):
    """API endpoint para obtener estado de jobs en JSON"""
    jobs = ProcessingJob.objects.all().order_by('-id')[:10]
    data = {
        'jobs': [{
            'id': job.id,
            'status': job.status,
            'status_display': job.get_status_display(),
            'progress': job.progress,
            'processed_records': job.processed_records,
            'total_records': job.total_records,
        } for job in jobs]
    }
    return JsonResponse(data)

def predict_success(request):
    """Vista de éxito después de una predicción"""
    return render(request, 'processor/predict_success.html')

def home(request):
    """Vista principal que muestra un resumen del sistema"""
    context = {
        'total_jobs': ProcessingJob.objects.count(),
        'active_jobs': ProcessingJob.objects.filter(status='running').count(),
        'recent_results': BatchResult.objects.select_related().order_by('-created_at')[:5],
        'streaming_active': StreamingWorker.is_running()
    }
    return render(request, 'processor/home.html', context)

class JobListView(ListView):
    """Vista para listar todos los jobs de procesamiento"""
    model = ProcessingJob
    template_name = 'processor/job_list.html'
    context_object_name = 'jobs'
    ordering = ['-pk']  # Ordenamos por ID en orden descendente
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['form'] = ProcessingConfigForm()
        return context

class JobDetailView(DetailView):
    """Vista para ver detalles de un job específico"""
    model = ProcessingJob
    template_name = 'processor/job_detail.html'
    context_object_name = 'job'
    
@require_POST
def job_delete(request, job_id):
    """Vista para eliminar un job"""
    job = get_object_or_404(ProcessingJob, pk=job_id)
    job.delete()
    messages.success(request, f'Job #{job_id} eliminado')
    return redirect('processor:job_list')

@require_POST
def job_terminate(request, job_id):
    """Vista para terminar un job en ejecución"""
    job = get_object_or_404(ProcessingJob, pk=job_id, status='running')
    job.status = 'terminated'
    job.save()
    messages.warning(request, f'Job #{job_id} terminado')
    return redirect('processor:job_detail', pk=job_id)

@require_POST
def job_resume(request, job_id):
    """Vista para reanudar un job terminado"""
    job = get_object_or_404(ProcessingJob, pk=job_id, status='terminated')
    job.status = 'pending'
    job.save()
    messages.info(request, f'Job #{job_id} reanudado')
    return redirect('processor:job_detail', pk=job_id)

@require_POST
def job_clear_history(request):
    """Vista para limpiar el historial de jobs"""
    ProcessingJob.objects.all().delete()
    messages.success(request, 'Historial de jobs limpiado')
    return redirect('processor:job_list')

def create_job(request):
    """Vista para crear un nuevo job de procesamiento"""
    if request.method == 'POST':
        form = ProcessingConfigForm(request.POST)
        if form.is_valid():
            # Crear el job y guardarlo
            # El modelo ProcessingJob tiene campos explícitos, no un campo 'config'
            job = ProcessingJob.objects.create(
                status='pending',
                batch_key=form.cleaned_data['batch_key'],
                batch_size=form.cleaned_data['batch_size'],
                total_records=0,
                processed_records=0,
            )

            # Ejecutar el job en segundo plano
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            python_exe = get_python_executable(project_root)
            cmd = [
                python_exe,
                'manage.py',
                'run_job',
                str(job.id)
            ]

            # Configura entorno y ejecuta
            env = os.environ.copy()
            env['PYTHONPATH'] = project_root
            subprocess.Popen(
                cmd,
                cwd=project_root,
                env=env,
                stdout=open(os.path.join('job_logs', f'run_job_{job.id}.log'), 'w'),
                stderr=subprocess.STDOUT
            )

            messages.success(request, f'Job #{job.id} creado y en ejecución')
            return redirect('processor:job_detail', pk=job.id)
    else:
        form = ProcessingConfigForm()
        
    return render(request, 'processor/job_create.html', {'form': form})

def get_python_executable(project_root):
    """Helper para encontrar el ejecutable de Python del virtualenv"""
    venv_candidates = [
        os.path.join(project_root, 'venv', 'Scripts', 'python.exe'),
    ]
    python_exe = sys.executable
    for p in venv_candidates:
        p_abs = os.path.abspath(p)
        if os.path.exists(p_abs):
            python_exe = p_abs
            break
    return python_exe

def view_results(request, job_id):
    """Vista para ver resultados detallados de un job"""
    job = get_object_or_404(ProcessingJob, pk=job_id)
    results = job.results.first()
    
    if not results:
        messages.warning(request, 'No hay resultados disponibles para este job')
        return redirect('processor:job_detail', pk=job_id)
        
    context = {
        'job': job,
        'results': results,
    }
    return render(request, 'processor/job_results.html', context)

def predict(request):
    """Vista para realizar predicciones individuales

    POST: valida el formulario `StreamingInputForm`, carga el PipelineModel
    persistido (models/pipeline_model), crea un DataFrame con Spark y aplica
    `model.transform` para obtener la predicción. Renderiza `predict.html`
    con `prediction` o `error` en el contexto.
    """
    context = {}
    if request.method == 'POST':
        form = StreamingInputForm(request.POST)
        if form.is_valid():
            row = form.cleaned_stream_row()
            model_dir = os.path.join(settings.BASE_DIR, 'models', 'pipeline_model')
            try:
                # Cargar modelo (puede lanzar FileNotFoundError)
                model = load_pipeline_model(model_dir)

                # Inicializar spark local y crear DF de una fila
                spark = get_spark()
                schema = streaming_schema()
                try:
                    df = spark.createDataFrame([row], schema=schema)
                except Exception:
                    # Fallback: crear DataFrame sin schema
                    df = spark.createDataFrame([row])

                preds = model.transform(df)
                try:
                    pdf = preds.toPandas()
                    if not pdf.empty:
                        first = pdf.iloc[0]
                        prediction = first.get('prediction') if 'prediction' in first else None
                        probability = first.get('probability') if 'probability' in first else None
                        # Map numeric prediction to human-readable label
                        human_pred = None
                        try:
                            if prediction is not None:
                                # En training usamos 0/1 para no/yes; asumimos 1 = apto
                                pred_val = float(prediction)
                                human_pred = 'Apto' if pred_val == 1.0 else 'No apto'
                        except Exception:
                            human_pred = None

                        context.update({'prediction': prediction, 'probability': probability, 'human_prediction': human_pred, 'form': form})
                    else:
                        context.update({'error': 'No se obtuvo predicción del modelo', 'form': form})
                except Exception:
                    context.update({'error': 'Error al convertir predicción a pandas', 'form': form})
            except FileNotFoundError:
                context.update({'error': 'Modelo no encontrado. Ejecuta un job de entrenamiento primero.', 'form': form})
            except Exception as e:
                # Log minimal; en producción registrar con logger
                context.update({'error': f'Error durante la predicción: {str(e)}', 'form': form})
        else:
            context.update({'error': 'Formulario inválido', 'form': form})
    else:
        context['form'] = StreamingInputForm()

    return render(request, 'processor/predict.html', context)

def streaming_dashboard(request):
    """Vista del dashboard de streaming"""
    context = {
        'form': StreamingInputForm(),
        'is_active': StreamingWorker.is_running()
    }
    return render(request, 'processor/streaming.html', context)

@require_POST
def start_stream(request):
    """Inicia el procesamiento de streaming"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Método no permitido'}, status=405)
        
    from .constants import INPUT_STREAMING_DIR, OUTPUT_STREAMING_DIR, STREAM_CHECKPOINT_DIR
    input_dir = os.path.join(settings.BASE_DIR, INPUT_STREAMING_DIR)
    output_dir = os.path.join(settings.BASE_DIR, OUTPUT_STREAMING_DIR)
    checkpoint_dir = os.path.join(settings.BASE_DIR, STREAM_CHECKPOINT_DIR)
    
    # Asegurar que los directorios existen
    for d in [input_dir, output_dir, checkpoint_dir]:
        os.makedirs(d, exist_ok=True)
    
    worker = StreamingWorker.get_instance(input_dir=input_dir, output_dir=output_dir, checkpoint_dir=checkpoint_dir)
    success = worker.start()
    
    if success:
        messages.success(request, 'Procesamiento streaming iniciado')
    else:
        messages.error(request, 'Error al iniciar streaming')
        
    return redirect('processor:streaming_dashboard')

@require_POST
def stop_stream(request):
    """Detiene el procesamiento de streaming"""
    worker = StreamingWorker.get_instance()
    if worker and worker.stop():
        messages.success(request, 'Procesamiento streaming detenido')
    else:
        messages.error(request, 'Error al detener streaming')
    return redirect('processor:streaming_dashboard')

def processing_status(request):
    """Endpoint para consultar estado del procesamiento"""
    worker = StreamingWorker.get_instance()
    is_active = StreamingWorker.is_running()
    
    status = {
        'is_active': is_active,
        'records_processed': worker.query.lastProgress.numInputRows if is_active and worker and worker.query else 0
    }
    return JsonResponse(status)

def processing_results_json(request):
    """Endpoint para obtener resultados en formato JSON"""
    # Obtiene todos los resultados, ordenados por fecha de creación descendente
    batch_results = BatchResult.objects.select_related('job').order_by('-created_at')
    
    results = [
        {
            'job_id': result.job.id,
            'batch_number': result.batch_number,
            'accuracy': result.accuracy,
            'precision': result.precision,
            'recall': result.recall,
            'f1_score': result.f1_score,
            'records_processed': result.records_processed,
            'processing_time': result.processing_time,
            'confusion_matrix': result.confusion_matrix
        }
        for result in batch_results
    ]
    return JsonResponse({'results': results})
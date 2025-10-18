from django.urls import path
from . import views

app_name = 'processor'

urlpatterns = [
    path('', views.home, name='home'),
    
    # Jobs
    path('jobs/', views.JobListView.as_view(), name='job_list'),
    path('jobs/create/', views.create_job, name='create_job'),
    path('jobs/<int:pk>/', views.JobDetailView.as_view(), name='job_detail'),
    path('jobs/<int:job_id>/delete/', views.job_delete, name='job_delete'),
    path('jobs/<int:job_id>/terminate/', views.job_terminate, name='job_terminate'),
    path('jobs/<int:job_id>/resume/', views.job_resume, name='job_resume'),
    path('jobs/clear/', views.job_clear_history, name='job_clear_history'),
    path('jobs/<int:job_id>/results/', views.view_results, name='view_results_job'),
    # Alias con nombre esperado por los tests y enlaces internos
    path('jobs/<int:job_id>/results/view/', views.view_results, name='view_results'),
    
    # Streaming
    path('streaming/', views.streaming_dashboard, name='streaming_dashboard'),
    path('streaming/start/', views.start_stream, name='start_stream'),
    path('streaming/stop/', views.stop_stream, name='stop_stream'),
    path('streaming/status/', views.processing_status, name='streaming_status'),
    
    # API endpoints
    path('api/results/', views.processing_results_json, name='processing_results_json'),
    path('api/jobs/status/', views.job_status, name='job_status'),
    
    # Utility views
    path('predict/', views.predict, name='predict'),
    path('predict/success/', views.predict_success, name='predict_success'),
    
    # Error handlers
    path('404/', views.handler404, name='404'),
    path('500/', views.handler500, name='500'),
]
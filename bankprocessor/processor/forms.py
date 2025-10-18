from django import forms

class ProcessingConfigForm(forms.Form):
    """Formulario para configurar el procesamiento por lotes"""
    
    BATCH_KEY_CHOICES = [
        ('age', 'Edad'),
        ('job', 'Trabajo'),
        ('marital', 'Estado Civil'),
        ('education', 'Educación'),
        ('balance', 'Balance'),
        ('housing', 'Préstamo Vivienda'),
        ('loan', 'Préstamo Personal'),
        ('contact', 'Contacto'),
        ('day', 'Día del Mes'),
        ('month', 'Mes'),
        ('duration', 'Duración'),
        ('campaign', 'Campaña'),
        ('pdays', 'Días desde última campaña'),
        ('previous', 'Contactos previos'),
        ('poutcome', 'Resultado previo')
    ]
    
    batch_key = forms.ChoiceField(
        choices=BATCH_KEY_CHOICES,
        label='Clave de Fragmentación',
        help_text='Selecciona la columna por la cual se dividirán los datos en lotes'
    )
    
    batch_size = forms.IntegerField(
        min_value=100,
        max_value=5000,
        initial=1000,
        label='Tamaño del Lote',
        help_text='Número de registros por lote (entre 100 y 5000)'
    )
    
    use_sample = forms.BooleanField(
        required=False,
        initial=False,
        label='Usar muestra de datos',
        help_text='Si se marca, se usará solo una muestra del dataset para pruebas rápidas'
    )
    
    def clean_batch_size(self):
        batch_size = self.cleaned_data['batch_size']
        if batch_size < 100 or batch_size > 5000:
            raise forms.ValidationError('El tamaño del lote debe estar entre 100 y 5000 registros')
        return batch_size

class CompareResultsForm(forms.Form):
    """Formulario para comparar resultados de procesamiento batch y streaming"""

    # Campos para resultados batch
    batch_job_id = forms.IntegerField(
        required=False,
        widget=forms.Select,
        label='Job de Batch'
    )
    
    batch_file = forms.FileField(
        required=False,
        label='Archivo de Resultados Batch',
        help_text='CSV con resultados de procesamiento batch'
    )

    # Campos para resultados streaming
    stream_start = forms.DateTimeField(
        required=False,
        label='Desde',
        widget=forms.DateTimeInput(attrs={'type': 'datetime-local'}),
        help_text='Inicio del rango para resultados streaming'
    )
    
    stream_end = forms.DateTimeField(
        required=False,
        label='Hasta',
        widget=forms.DateTimeInput(attrs={'type': 'datetime-local'}),
        help_text='Fin del rango para resultados streaming'
    )
    
    stream_file = forms.FileField(
        required=False,
        label='Archivo de Resultados Streaming',
        help_text='CSV con resultados de streaming'
    )

    def clean(self):
        cleaned_data = super().clean()
        batch_job_id = cleaned_data.get('batch_job_id')
        batch_file = cleaned_data.get('batch_file')
        stream_start = cleaned_data.get('stream_start')
        stream_end = cleaned_data.get('stream_end')
        stream_file = cleaned_data.get('stream_file')
        
        # Validar que se proporcione al menos una fuente para batch
        if not batch_job_id and not batch_file:
            self.add_error(None, 'Debe seleccionar un job o subir un archivo de resultados batch')
            
        # Validar que se proporcione al menos una fuente para streaming
        if not (stream_start and stream_end) and not stream_file:
            self.add_error(None, 'Debe especificar un rango de fechas o subir un archivo de resultados streaming')
            
        # Si se especifica rango de fechas, validar que sea coherente
        if stream_start and stream_end and stream_start >= stream_end:
            self.add_error('stream_end', 'La fecha de fin debe ser posterior a la fecha de inicio')
            
        return cleaned_data


class StreamingInputForm(forms.Form):
    """Formulario para que el usuario ingrese features para streaming.
    
    Los campos coinciden con el schema del modelo entrenado y tienen
    validaciones específicas para cada tipo de dato.
    """
    first_name = forms.CharField(required=False, label='Nombre')
    last_name = forms.CharField(required=False, label='Apellido')

    age = forms.IntegerField(
        label='Edad', 
        min_value=18, 
        max_value=100,
        required=True,
        help_text='Edad del cliente (18-100 años)'
    )
    
    job = forms.ChoiceField(
        label='Trabajo',
        required=True,
        choices=[
            ('admin.', 'Administrativo'),
            ('blue-collar', 'Trabajador manual'),
            ('entrepreneur', 'Empresario'),
            ('housemaid', 'Ama de casa'),
            ('management', 'Gerencia'),
            ('retired', 'Jubilado'),
            ('self-employed', 'Autónomo'),
            ('services', 'Servicios'),
            ('student', 'Estudiante'),
            ('technician', 'Técnico'),
            ('unemployed', 'Desempleado'),
            ('unknown', 'Desconocido')
        ]
    )
    
    marital = forms.ChoiceField(
        label='Estado Civil',
        required=True,
        choices=[
            ('divorced', 'Divorciado'),
            ('married', 'Casado'),
            ('single', 'Soltero')
        ]
    )
    
    education = forms.ChoiceField(
        label='Educación',
        required=True,
        choices=[
            ('primary', 'Primaria'),
            ('secondary', 'Secundaria'),
            ('tertiary', 'Superior'),
            ('unknown', 'Desconocido')
        ]
    )
    
    default = forms.ChoiceField(
        label='Historial de Default',
        required=True,
        choices=[
            ('yes', 'Sí'),
            ('no', 'No')
        ]
    )
    
    balance = forms.IntegerField(
        label='Balance / Saldo',
        required=True,
        help_text='Saldo promedio en cuenta'
    )
    
    housing = forms.ChoiceField(
        label='Préstamo Vivienda',
        required=True,
        choices=[
            ('yes', 'Sí'),
            ('no', 'No')
        ]
    )
    
    loan = forms.ChoiceField(
        label='Préstamo Personal',
        required=True,
        choices=[
            ('yes', 'Sí'),
            ('no', 'No')
        ]
    )
    
    contact = forms.ChoiceField(
        label='Método de Contacto',
        required=True,
        choices=[
            ('cellular', 'Celular'),
            ('telephone', 'Teléfono'),
            ('unknown', 'Desconocido')
        ]
    )
    
    day = forms.IntegerField(
        label='Día del Mes',
        required=True,
        min_value=1,
        max_value=31,
        help_text='Día del último contacto (1-31)'
    )
    
    month = forms.ChoiceField(
        label='Mes',
        required=True,
        choices=[
            ('jan', 'Enero'), ('feb', 'Febrero'), ('mar', 'Marzo'),
            ('apr', 'Abril'), ('may', 'Mayo'), ('jun', 'Junio'),
            ('jul', 'Julio'), ('aug', 'Agosto'), ('sep', 'Septiembre'),
            ('oct', 'Octubre'), ('nov', 'Noviembre'), ('dec', 'Diciembre')
        ]
    )
    
    duration = forms.IntegerField(
        label='Duración',
        required=True,
        min_value=0,
        help_text='Duración del último contacto en segundos'
    )
    
    campaign = forms.IntegerField(
        label='Contactos en Campaña',
        required=True,
        min_value=1,
        help_text='Número de contactos realizados en esta campaña'
    )
    
    pdays = forms.IntegerField(
        label='Días desde Campaña Anterior',
        required=True,
        help_text='Días desde el último contacto en campaña anterior. 999 significa nunca contactado.'
    )
    
    previous = forms.IntegerField(
        label='Contactos Previos',
        required=True,
        min_value=0,
        help_text='Número de contactos antes de esta campaña'
    )
    
    poutcome = forms.ChoiceField(
        label='Resultado Anterior',
        required=True,
        choices=[
            ('failure', 'Fallido'),
            ('success', 'Exitoso'),
            ('other', 'Otro'),
            ('unknown', 'Desconocido')
        ],
        help_text='Resultado de la campaña de marketing anterior'
    )

    def cleaned_stream_row(self):
        """Return a dict with only the streaming schema columns (strings or ints)."""
        cd = self.cleaned_data
        # Ensure columns exist with fallback empty values
        row = {
            'age': cd.get('age') or '',
            'job': cd.get('job') or '',
            'marital': cd.get('marital') or '',
            'education': cd.get('education') or '',
            'default': cd.get('default') or '',
            'balance': cd.get('balance') if cd.get('balance') is not None else '',
            'housing': cd.get('housing') or '',
            'loan': cd.get('loan') or '',
            'contact': cd.get('contact') or '',
            'day': cd.get('day') if cd.get('day') is not None else '',
            'month': cd.get('month') or '',
            'duration': cd.get('duration') if cd.get('duration') is not None else '',
            'campaign': cd.get('campaign') if cd.get('campaign') is not None else '',
            'pdays': cd.get('pdays') if cd.get('pdays') is not None else '',
            'previous': cd.get('previous') if cd.get('previous') is not None else '',
            'poutcome': cd.get('poutcome') or '',
        }
        return row
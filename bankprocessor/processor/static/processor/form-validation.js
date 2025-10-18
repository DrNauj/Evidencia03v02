// Validación en tiempo real para formularios
document.addEventListener('DOMContentLoaded', function() {
    const form = document.querySelector('form');
    if (!form) return;

    // Validar tamaño de lote
    const batchSizeInput = form.querySelector('input[name="batch_size"]');
    if (batchSizeInput) {
        batchSizeInput.addEventListener('input', function(e) {
            const value = parseInt(e.target.value);
            const feedback = this.nextElementSibling;
            
            if (value < 50 || value > 1000) {
                this.classList.add('is-invalid');
                if (!feedback) {
                    const div = document.createElement('div');
                    div.className = 'invalid-feedback';
                    div.textContent = 'El tamaño debe estar entre 50 y 1000';
                    this.parentNode.appendChild(div);
                }
            } else {
                this.classList.remove('is-invalid');
                if (feedback) feedback.remove();
            }
        });
    }

    // Deshabilitar botón submit mientras hay errores
    form.addEventListener('input', function() {
        const submitBtn = this.querySelector('button[type="submit"]');
        const hasErrors = this.querySelector('.is-invalid');
        if (submitBtn) {
            submitBtn.disabled = !!hasErrors;
        }
    });
});
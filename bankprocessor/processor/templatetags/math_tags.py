from django import template

register = template.Library()

@register.filter(name='multiply')
def multiply(value, arg):
    """Multiplica value por arg en plantillas: {{ value|multiply:factor }}"""
    try:
        return float(value) * float(arg)
    except Exception:
        return value

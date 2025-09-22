import json

class BadJSON(Exception):
    """Se lanza cuando el cuerpo no es JSON válido."""
    pass


class InvalidStatus(Exception):
    """Se lanza cuando la transición de estado no es válida."""
    pass


def parse_json_body(request):
    """
    Intenta decodificar el body del request como JSON y retorna un dict.
    Lanza BadJSON si falla.
    """
    try:
        body = request.body.decode("utf-8") if request.body else "{}"
        return json.loads(body or "{}")
    except Exception as e:
        raise BadJSON(f"JSON inválido: {e}")


# Reglas simples de transición de estados
_ALLOWED_TRANSITIONS = {
    "CREATED":   {"UPDATED", "CANCELLED", "SHIPPED"},
    "UPDATED":   {"SHIPPED", "CANCELLED"},
    "SHIPPED":   {"DELIVERED"},
    "DELIVERED": set(),
    "CANCELLED": set(),
}


def validate_status_transition(current_status: str | None, new_status: str) -> bool:
    """
    Valida que el cambio de estado sea válido.
    - Si current_status es None, siempre permite (caso de creación).
    - Lanza InvalidStatus si la transición no está permitida.
    """
    if current_status is None:
        return True

    allowed = _ALLOWED_TRANSITIONS.get(current_status, set())
    if new_status not in allowed:
        raise InvalidStatus(f"No permitido pasar de {current_status} a {new_status}")
    return True

from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_POST, require_http_methods
from django.db import transaction, models
from django.http import JsonResponse, HttpResponseBadRequest, HttpResponseNotFound
from django.views.decorators.http import require_http_methods

from .models import Order
from .publisher import publish_order_status_updated
from .validators import parse_json_body, validate_status_transition, BadJSON, InvalidStatus

def _json(data, status=200):
    return JsonResponse(data, status=status, json_dumps_params={"ensure_ascii": False})


@require_GET
def get_order(request, order_id: str):
    try:
        o = Order.objects.get(pk=order_id)
        return _json({"id": o.id, "status": o.status, "version": o.version})
    except Order.DoesNotExist:
        return HttpResponseNotFound("order not found")

@require_POST
def create_order(request):
    try:
        body = parse_json_body(request)
        oid = body["id"]; status = body.get("status", "CREATED")
    except (BadJSON, KeyError):
        return HttpResponseBadRequest("invalid payload")

    obj, created = Order.objects.get_or_create(id=oid, defaults={"status": status})
    return _json({"created": created, "id": obj.id, "status": obj.status, "version": obj.version}, 201 if created else 200)

@require_http_methods(["PUT", "PATCH"])
def update_status(request, order_id: str):
    """
    Ruta crítica del ASR:
    - Actualiza status por PK en una transacción corta (SELECT ... FOR UPDATE + UPDATE).
    - Maneja control optimista opcional por 'version'.
    - Publica evento EDA fuera de la transacción (no afecta la latencia).
    - Códigos: 200 OK, 404 si no existe, 409 si hay conflicto de versión, 400 si payload inválido.
    """
    try:
        body = parse_json_body(request)
        new_status = body["status"]
        expected   = body.get("version")   # int opcional para control optimista
        meta       = body.get("meta", {})  # opcional: quién actualiza, timestamp cliente, etc.
    except (BadJSON, KeyError):
        return HttpResponseBadRequest("invalid payload")

    # Transacción lo más pequeña posible
    with transaction.atomic():
        # Lock de fila para evitar ‘lost updates’
        q = Order.objects.select_for_update().filter(id=order_id)
        if expected is not None:
            q = q.filter(version=expected)  # control optimista (si cliente envía version)
        row = q.first()

        if row is None:
            # Distinguir entre no existe vs. conflicto de versión
            exists = Order.objects.filter(id=order_id).exists()
            if exists and expected is not None:
                return _json({"ok": False, "conflict": True, "reason": "version mismatch"}, 409)
            return HttpResponseNotFound("order not found")

        try:
            validate_status_transition(row.status, new_status)
        except InvalidStatus as e:
            return HttpResponseBadRequest(str(e))

        # UPDATE atómico (sin re-consultar toda la fila)
        updated = Order.objects.filter(pk=row.pk).update(
            status=new_status,
            version=models.F("version") + 1,
        )

        # Releer solo los campos necesarios para responder y publicar
        row.refresh_from_db(fields=["status", "version"])

    # Publicar evento EDA fuera de la transacción
    publish_order_status_updated(row.id, row.status, row.version, meta=meta)

    # Respuesta JSON (rápida, sin incluir datos pesados)
    return _json({"ok": True, "id": row.id, "status": row.status, "version": row.version})
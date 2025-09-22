from django.urls import path
from .views import get_order, create_order, update_status

urlpatterns = [
    path("orders/<str:order_id>", get_order),
    path("orders/<str:order_id>/status", update_status),
    path("orders", create_order),
]

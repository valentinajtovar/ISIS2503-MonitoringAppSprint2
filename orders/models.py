from django.db import models

class Order(models.Model):
    id = models.CharField(primary_key=True, max_length=64)
    status = models.CharField(max_length=32)
    updated_at = models.DateTimeField(auto_now=True)
    version = models.IntegerField(default=0)  # control optimista

    def __str__(self):
        return f"{self.id}:{self.status}:{self.version}"

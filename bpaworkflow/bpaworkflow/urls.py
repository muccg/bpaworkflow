from django.conf.urls import url
from django.contrib import admin
from django.conf import settings
from django.conf.urls.static import static
from . import views

admin.autodiscover()

urlpatterns = [
    url(r'^$', views.WorkflowIndex.as_view()),
    url(r'^private/api/v1/metadata$', views.metadata, name="taxonomy_options"),
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)

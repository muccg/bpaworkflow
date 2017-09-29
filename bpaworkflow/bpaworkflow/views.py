import logging
from bpaingest.projects import PROJECTS

from django.conf import settings
from django.views.decorators.http import require_http_methods
from django.views.generic import TemplateView
from django.http import JsonResponse

logger = logging.getLogger("rainbow")


class WorkflowIndex(TemplateView):
    template_name = 'bpaworkflow/index.html'
    ckan_base_url = settings.CKAN_SERVERS[0]['base_url']

    def get_context_data(self, **kwargs):
        context = super(WorkflowIndex, self).get_context_data(**kwargs)
        context['ckan_base_url'] = settings.CKAN_SERVERS[0]['base_url']
        return context


@require_http_methods(["GET"])
def metadata(request):
    """
    private API: given taxonomy constraints, return the possible options
    """
    return JsonResponse({
        'projects': PROJECTS
    })

import logging
from collections import defaultdict
from django.conf import settings
from django.views.decorators.http import require_http_methods
from django.views.generic import TemplateView
from django.http import JsonResponse

from bpaingest.projects import ProjectInfo
from bpaingest.organizations import ORGANIZATIONS


logger = logging.getLogger("rainbow")
project_info = ProjectInfo()


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
    by_organization = defaultdict(list)
    for info in project_info.metadata_info:
        obj = dict((t, info[t]) for t in ('slug', 'omics', 'technology', 'analysed', 'pool'))
        by_organization[info['organization']].append(obj)
    return JsonResponse({
        'importers': by_organization,
        'projects': dict((t['name'], dict((s, t[s]) for s in ('name', 'title'))) for t in ORGANIZATIONS if t['name'] in by_organization)
    })

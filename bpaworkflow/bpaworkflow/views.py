import re
import csv
import json
import logging
import zipstream
import datetime
from collections import defaultdict

from django.conf import settings
from django.views.decorators.http import require_http_methods
from django.views.generic import TemplateView
from django.http import JsonResponse, StreamingHttpResponse
from io import StringIO
import traceback

logger = logging.getLogger("rainbow")


class WorkflowIndex(TemplateView):
    template_name = 'bpaworkflow/index.html'
    ckan_base_url = settings.CKAN_SERVERS[0]['base_url']

    def get_context_data(self, **kwargs):
        context = super(WorkflowIndex, self).get_context_data(**kwargs)
        context['ckan_base_url'] = settings.CKAN_SERVERS[0]['base_url']
        return context

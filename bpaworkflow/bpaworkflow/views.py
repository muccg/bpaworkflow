import logging
from collections import defaultdict
from django.conf import settings
from django.views.decorators.http import require_http_methods
from django.views.generic import TemplateView
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse

from bpaingest.projects import ProjectInfo
from bpaingest.organizations import ORGANIZATIONS
from . import tasks
from .models import VerificationJob

logger = logging.getLogger("rainbow")
project_info = ProjectInfo()


# convenience method
def has_its_own_active_ingest(cls):
    return is_active_project(cls) and metadata_verifyable(cls)


def metadata_verifyable(cls):
    """
    `cls` uses common constructs which allow us to verify
    the spreadsheet and MD5 file
    """
    return hasattr(cls, "spreadsheet") and hasattr(cls, "md5")


def is_active_project(cls):
    return getattr(cls, "organization", "") not in [
        "bpa-sepsis",
        "bpa-great-barrier-reef",
        "bpa-stemcells",
    ]


class WorkflowIndex(TemplateView):
    template_name = "bpaworkflow/index.html"
    ckan_base_url = settings.CKAN_SERVER["base_url"]

    def get_context_data(self, **kwargs):
        context = super(WorkflowIndex, self).get_context_data(**kwargs)
        context["ckan_base_url"] = settings.CKAN_SERVER["base_url"]
        return context


@require_http_methods(["GET"])
def metadata(request):
    """
    private API: given taxonomy constraints, return the possible options
    """
    by_organization = defaultdict(list)
    logger.info(f"at beginning by_organization is: {by_organization}")
    for info in filter(
            lambda x: has_its_own_active_ingest(x["cls"]), project_info.metadata_info
    ):
        obj = dict(
            (t, info[t])
            for t in ("slug", "omics", "technology", "analysed", "pool", "project")
        )
        by_organization[info["organization"]].append(obj)
    logger.info("have by organization...")
    logger.info(f"{by_organization}")
    return JsonResponse(
        {
            "importers": by_organization,
            "projects": dict(
                (t["name"], dict((s, t[s]) for s in ("name", "title")))
                for t in ORGANIZATIONS
                if t["name"] in by_organization
            ),
        }
    )


@require_http_methods(["POST"])
def validate(request):
    """
    private API: validate MD5 file, XLSX file for a given importer
    """

    importer = request.POST["importer"]
    cls = project_info.cli_options().get(importer)
    if not cls or not metadata_verifyable(cls):
        return JsonResponse({"error": "invalid submission"})

    submission_id = tasks.invoke_validation(importer, request.FILES)
    return JsonResponse({"submission_id": submission_id})


@csrf_exempt
@require_http_methods(["POST"])
def status(request):
    """
    private API: get the current status of a validation task
    """
    job_uuid = request.POST["submission_id"]
    job = VerificationJob.objects.get(uuid=job_uuid)
    return JsonResponse(
        {
            "submission_id": job.uuid,
            "complete": job.state.get("complete"),
            "md5": job.state.get("md5"),
            "xlsx": job.state.get("xlsx"),
            "diff": job.state.get("diff"),
        }
    )

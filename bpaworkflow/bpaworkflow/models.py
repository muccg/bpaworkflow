from django.contrib.postgres.fields import JSONField
from django.db import models
from bpaingest.projects import ProjectInfo
import uuid
import logging

logger = logging.getLogger("rainbow")
project_info = ProjectInfo()


class VerificationJob(models.Model):
    """
    track the state of a verification task.
    we persist this in the database to allow future inspection of results.
    """

    uuid = models.CharField(max_length=36, db_index=True)
    submitted = models.DateTimeField(auto_now=True)
    importer = models.TextField()
    xlsx_name = models.TextField()
    xlsx_data = models.BinaryField(null=True)
    md5_name = models.TextField()
    md5_data = models.BinaryField(null=True)
    # state as we track through the verification pipeline is stored in here
    state = JSONField()

    def get_importer_cls(self):
        return project_info.cli_options()[self.importer]

    @classmethod
    def create(cls, **kwargs):
        job_uuid = str(uuid.uuid4())
        job = cls(uuid=job_uuid, state={}, **kwargs)
        job.save()
        return job

    def set(self, **kwargs):
        for k, v in kwargs.items():
            self.state[k] = v
            self.save()

    def get(self, k):
        return self.state[k]

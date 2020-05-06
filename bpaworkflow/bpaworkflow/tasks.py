from celery import shared_task
import os
import pickle
import tempfile
import logging
import shutil
import redis
import uuid
import json
from .validate import verify_md5file, verify_spreadsheet
from django.conf import settings
from collections import defaultdict
from bpaingest.metadata import DownloadMetadata

logger = logging.getLogger(__name__)
redis_client = redis.StrictRedis(host=settings.REDIS_HOST, db=settings.REDIS_DB)


class TaskState:
    """
    tracks a validation job as it progresses - state is stored in Redis
    """

    def __init__(self, submission_id):
        self.submission_id = submission_id

    @classmethod
    def create(cls):
        submission_id = str(uuid.uuid4())
        redis_client.hset(submission_id, "id", submission_id)
        return TaskState(submission_id)

    def __setattr__(self, name, value):
        if name == "submission_id":
            object.__setattr__(self, name, value)
            return
        redis_client.hset(self.submission_id, name, pickle.dumps(value))

    def __getattr__(self, name):
        return pickle.loads(
            redis_client.hget(self.submission_id.encode("utf8"), name.encode("utf8"))
        )


@shared_task(bind=True)
def validate_spreadsheet(self, submission_id):
    submission = TaskState(submission_id)
    # retrieved from Redis, so just do it once
    cls = submission.cls
    paths = submission.paths
    fake_metadata_info = submission.fake_metadata_info
    # these are fairly quick
    submission.xlsx = verify_spreadsheet(cls, paths["xlsx"], fake_metadata_info)
    return submission_id


@shared_task(bind=True)
def validate_md5(self, submission_id):
    submission = TaskState(submission_id)
    # retrieved from Redis, so just do it once
    cls = submission.cls
    paths = submission.paths
    # these are fairly quick
    submission.md5 = verify_md5file(cls, paths["md5"])
    return submission_id


@shared_task(bind=True)
def validate_bpaingest_json(self, submission_id):
    submission = TaskState(submission_id)

    # retrieved from Redis, so just do it once
    cls = submission.cls
    fake_metadata_info = submission.fake_metadata_info
    paths = submission.paths

    def unchanged_metadata():
        return DownloadMetadata(cls)

    def new_metadata():
        # this will go ahead and download the existing metadata
        dlmeta = DownloadMetadata(cls)
        # copy in the new metadata
        for fpath in paths.values():
            shutil.copy(fpath, os.path.join(dlmeta.path, os.path.basename(fpath)))
        # splice together the metadata from the archive with the synthetic metadata
        with open(dlmeta.info_json) as fd:
            metadata_info = json.load(fd)
        metadata_info.update(fake_metadata_info)
        with open(dlmeta.info_json, "w") as fd:
            json.dump(metadata_info, fd)
        # recreate the class instance with the updated metadata
        dlmeta.meta = dlmeta.make_meta()
        return dlmeta

    def run(meta_maker):
        state = defaultdict(lambda: defaultdict(list))
        # download metadata for all project types and aggregate metadata keys
        with meta_maker() as dlmeta:
            meta = dlmeta.meta
            data_type = meta.ckan_data_type
            state[data_type]["packages"] += meta.get_packages()
            state[data_type]["resources"] += meta.get_resources()

        for data_type in state:
            state[data_type]["packages"].sort(key=lambda x: x["id"])
            state[data_type]["resources"].sort(key=lambda x: x[2]["id"])

        return state

    prior_state = run(unchanged_metadata)
    post_state = run(new_metadata)

    return submission_id


@shared_task(bind=True)
def validate_complete(self, submission_id):
    submission = TaskState(submission_id)
    submission.complete = True
    return submission_id


def invoke_validation(cls, files):
    def write_file(tempd, file_obj):
        # be a bit paranoid, normalise and strip any path components out
        name = os.path.basename(file_obj.name)
        if not name:
            raise Exception("invalid filename provided in upload")
        fpath = os.path.join(tempd, name)
        with open(fpath, "wb") as fd:
            for chunk in file_obj.chunks():
                fd.write(chunk)
        return fpath

    def write_files(path):
        paths = {}
        for field_name, file_obj in files.items():
            paths[field_name] = write_file(path, file_obj)
        return paths

    def fabricate_metadata_info(path):
        # fabricate metadata information for the files uploaded by the user
        metadata_info = {}
        for filename in os.listdir(path):
            # synthesise harmless additional context data so we can run the ingestor
            metadata_info[os.path.basename(filename)] = obj = {
                "base_url": "https://example.com/does-not-exist/",
            }
            for k in cls.metadata_url_components:
                obj[k] = "BPAOPS-99999"
        return metadata_info

    state = TaskState.create()
    state.cls = cls
    state.complete = False
    state.path = tempfile.mkdtemp(prefix="bpaworkflow-", dir=settings.CELERY_DATADIR)
    state.paths = write_files(state.path)
    state.fake_metadata_info = fabricate_metadata_info(state.path)
    (
        validate_spreadsheet.s()
        | validate_md5.s()
        | validate_bpaingest_json.s()
        | validate_complete.s()
    ).delay(state.submission_id)
    return state.submission_id

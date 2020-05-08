from celery import shared_task
import os
import pickle
import tempfile
import logging
import shutil
import redis
import uuid
import json
from .validate import verify_md5file, verify_spreadsheet, verify_metadata, linkage_qc
from django.conf import settings
from collections import defaultdict
from bpaingest.metadata import DownloadMetadata

redis_client = redis.StrictRedis(host=settings.REDIS_HOST, db=settings.REDIS_DB)


def make_file_logger(name):
    tmpf = tempfile.mktemp("bpaingest-log-", dir=settings.CELERY_DATADIR)
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(tmpf)
    fmt = logging.Formatter("[%(levelname)-7s] %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    return tmpf, logger


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
LOGGER = logging.getLogger("debug")

@shared_task(bind=True)
def validate_spreadsheet(self, submission_id):
    LOGGER.debug("Task 1...")
    submission = TaskState(submission_id)
    # retrieved from Redis, so just do it once
    cls = submission.cls
    paths = submission.paths
    fake_metadata_info = submission.fake_metadata_info
    # these are fairly quick
    logger = logging.getLogger("spreadsheet")
    submission.xlsx = verify_spreadsheet(logger, cls, paths["xlsx"], fake_metadata_info)
    LOGGER.debug("Completed Task 1...")
    return submission_id


@shared_task(bind=True)
def validate_md5(self, submission_id):
    LOGGER.debug("Task 2...")
    submission = TaskState(submission_id)
    # retrieved from Redis, so just do it once
    cls = submission.cls
    paths = submission.paths
    # these are fairly quick
    logger = logging.getLogger("md5")
    submission.md5 = verify_md5file(logger, cls, paths["md5"])
    LOGGER.debug("Completed Task 2...")
    return submission_id


@shared_task(bind=True)
def other_bpaingest_json(self, submission_id):
    submission = TaskState(submission_id)

    # retrieved from Redis, so just do it once
    cls = submission.cls


@shared_task(bind=True)
def validate_bpaingest_json(self, submission_id):
    LOGGER.debug("Task 3...")
    submission = TaskState(submission_id)

    # retrieved from Redis, so just do it once
    cls = submission.cls
    fake_metadata_info = submission.fake_metadata_info
    paths = submission.paths

    def prior_metadata(logger):
        LOGGER.debug("....Unchanged download start...")
        download = DownloadMetadata(logger, cls)
        LOGGER.debug("....Unchanged download finished...")
        return download

    def post_metadata(logger):
        LOGGER.debug("....Changed download start...")
        # this will go ahead and download the existing metadata
        dlmeta = DownloadMetadata(logger, cls)
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
        dlmeta.meta = dlmeta.make_meta(logger)
        LOGGER.debug("....Changed download finished...")
        return dlmeta

    def validate2_bpaingest_json(self, submission_id, state):
        LOGGER.debug("Task 4...")
        # submission = TaskState(submission_id)

        LOGGER.debug('starting linking state...................')
        linkage_qc(state)
        LOGGER.debug('finished linking state...................')
        LOGGER.debug("Completed Task 4...")
        return submission_id

    def run(name, meta_maker):
        LOGGER.debug('starting sub-task downloads run ...................')
        logfile, logger = make_file_logger(name)
        state = defaultdict(lambda: defaultdict(list))
        # download metadata for all project types and aggregate metadata keys
        # data_type_meta = {}
        with meta_maker(logger) as dlmeta:
            meta = dlmeta.meta
            data_type = meta.ckan_data_type
            state[data_type]["meta"] = meta
            (packages, resources) = verify_metadata(meta)
            state[data_type]["packages"] += packages
            state[data_type]["resources"] += resources
            LOGGER.debug("Continuing...")

        LOGGER.debug('starting sorting state...................')
        for data_type in state:
            state[data_type]["packages"].sort(key=lambda x: x["id"])
            state[data_type]["resources"].sort(key=lambda x: x[2]["id"])
        LOGGER.debug('finished sorting state...................')

        LOGGER.debug('starting linking state...................')
        linkage_qc(state)
        LOGGER.debug('finished linking state...................')
        LOGGER.debug('completed sub-task downloads run...................')
        with open(logfile) as fd:
            log = fd.read()

        os.unlink(logfile)

        return log, state

    LOGGER.debug('Task 3a..........starting prior state....')
    prior_state = run("prior.{}".format(submission_id), prior_metadata)
    LOGGER.debug('3b...........starting post state....')
    post_state = run("post.{}".format(submission_id), post_metadata)

    validate2_bpaingest_json(prior_state)
    LOGGER.debug("Completed Task 3...")
    return submission_id


# @shared_task(bind=True)
# def validate2_bpaingest_json(self, submission_id, state):
#     LOGGER.debug("Task 4...")
#     # submission = TaskState(submission_id)
#
#     LOGGER.debug('starting linking state...................')
#     linkage_qc(state)
#     LOGGER.debug('finished linking state...................')
#     LOGGER.debug("Completed Task 4...")
#     return submission_id


@shared_task(bind=True)
def validate_complete(self, submission_id):
    LOGGER.debug("Task 5...")
    submission = TaskState(submission_id)
    submission.complete = True
    paths = submission.paths
    for fpath in paths.values():
        os.unlink(fpath)
    os.rmdir(submission.path)
    LOGGER.debug("Completed Task 5...")
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

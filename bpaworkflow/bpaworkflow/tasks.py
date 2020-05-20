from celery import shared_task
import os
import re
import tempfile
import logging
import shutil
import redis
import json
from django.http import HttpResponseForbidden
from .validate import verify_md5file, verify_spreadsheet, collect_linkage_dump_linkage
from .models import VerificationJob
from django.conf import settings
from collections import defaultdict
from bpaingest.metadata import DownloadMetadata

redis_client = redis.StrictRedis(host=settings.REDIS_HOST, db=settings.REDIS_DB)


def make_file_logger(name):
    tmpf = tempfile.mktemp(prefix="bpaingest-log-", suffix=".log", dir=settings.CELERY_DATADIR)
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(tmpf)
    fmt = logging.Formatter("[%(levelname)-7s] %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    return tmpf, logger


@shared_task(bind=True)
def validation_setup(self, job_uuid):
    """
    sets up the temporary working directory for the uploaded files
    """

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

    def write_file(fname, binary_field):
        target = os.path.join(temp_path, fname)
        with open(target, "wb") as fd:
            fd.write(binary_field)
        return target

    def write_files():
        paths = {}
        paths["xlsx"] = write_file(job.xlsx_name, job.xlsx_data)
        paths["md5"] = write_file(job.md5_name, job.md5_data)
        return paths

    job = VerificationJob.objects.get(uuid=job_uuid)
    cls = job.get_importer_cls()
    temp_path = tempfile.mkdtemp(prefix="bpaworkflow-", dir=settings.CELERY_DATADIR)
    path_info = write_files()
    job.set(
        path_info=path_info,
        temp_path=temp_path,
        temp_metadata_info=fabricate_metadata_info(temp_path),
    )
    return job_uuid


@shared_task(bind=True)
def validate_spreadsheet(self, job_uuid):
    job = VerificationJob.objects.get(uuid=job_uuid)
    cls = job.get_importer_cls()
    logger = logging.getLogger("spreadsheet")
    paths = job.state["path_info"]
    job.set(xlsx=verify_spreadsheet(
        logger, cls, paths["xlsx"], job.state["temp_metadata_info"]
    ))
    return job_uuid


@shared_task(bind=True)
def validate_md5(self, job_uuid):
    job = VerificationJob.objects.get(uuid=job_uuid)
    cls = job.get_importer_cls()
    logger = logging.getLogger("md5")
    paths = job.state["path_info"]
    result = verify_md5file(logger, cls, paths["md5"])
    job.set(md5=result)
    return job_uuid


@shared_task(bind=True)
def validate_bpaingest_json(self, job_uuid):
    logger = logging.getLogger("validate_bpaingest")
    job = VerificationJob.objects.get(uuid=job_uuid)
    # This job runs longer than others. Set a result early for subscriptions to capture as other results come in, before this one completed.
    job.set(diff=["Validating, please wait..."])
    # retrieved from Redis, so just do it once
    cls = job.get_importer_cls()
    temp_metadata_info = job.state["temp_metadata_info"]
    paths = job.state["path_info"]

    def prior_metadata(logger):
        return DownloadMetadata(logger, cls)

    def post_metadata(logger):
        # this will go ahead and download the existing metadata
        dlmeta = DownloadMetadata(logger, cls)
        # copy in the new metadata
        for fpath in paths.values():
            shutil.copy(fpath, os.path.join(dlmeta.path, os.path.basename(fpath)))
        # splice together the metadata from the archive with the synthetic metadata
        with open(dlmeta.info_json) as fd:
            metadata_info = json.load(fd)
        metadata_info.update(temp_metadata_info)
        with open(dlmeta.info_json, "w") as fd:
            json.dump(metadata_info, fd)
        # recreate the class instance with the updated metadata
        dlmeta.meta = dlmeta.make_meta(logger)
        return dlmeta

    def run(name, meta_maker):
        logfile, logger = make_file_logger(name)
        state = defaultdict(lambda: defaultdict(list))
        data_type_meta = {}
        # download metadata for all project types and aggregate metadata keys
        with meta_maker(logger) as dlmeta:
            meta = dlmeta.meta
            data_type = meta.ckan_data_type
            data_type_meta[data_type] = meta
            try:
                state[data_type]["packages"] += meta.get_packages()
                state[data_type]["resources"] += meta.get_resources()
            except AttributeError as ae:
                logger.warning(
                    "There was a problem capturing packages or resources (It could be that there was no meta-tracking object).",
                    ae
                )

        for data_type in state:
            state[data_type]["packages"].sort(key=lambda x: x["id"])
            state[data_type]["resources"].sort(key=lambda x: x[2]["id"])

        with open(logfile) as fd:
            log = fd.read()

        os.unlink(logfile)
        return log, state, data_type, data_type_meta

    def diff_json(json1, json2):
        difference = {k: json2[k] for k in set(json2) - set(json1)}
        return difference

    prior_log, prior_state, prior_data_type, _prior_data_type_meta = run(
        "prior.{}".format(job_uuid), prior_metadata
    )
    post_log, post_state, post_data_type, post_data_type_meta = run(
        "post.{}".format(job_uuid), post_metadata
    )
    diff_state = diff_json(prior_state, post_state)
    linkage_results = collect_linkage_dump_linkage(logger, diff_state, post_data_type_meta)
    if not isinstance(linkage_results, list):
        linkage_results = ["ERROR: An error occurred in linking results."]
    job.set(diff=linkage_results)
    return job_uuid


@shared_task(bind=True)
def validate_complete(self, job_uuid):
    job = VerificationJob.objects.get(uuid=job_uuid)
    paths = job.state["path_info"]
    job.set(complete=True)
    for fpath in paths.values():
        os.unlink(fpath)
    os.rmdir(job.state["temp_path"])
    return job_uuid


# be a little bit paranoid; this matches every file in the existing archive
valid_filename = re.compile(r"^[A-Za-z0-9_\- .()]+\.(md5|xlsx)$")


def invoke_validation(importer, files):
    def get_filename(key):
        name = files[key].name
        if not valid_filename.match(name):
            raise HttpResponseForbidden()
        return name

    def read_file(key):
        size = 0
        buf = []
        file_obj = files[key]
        for chunk in file_obj.chunks():
            size += len(chunk)
            if size > settings.VERIFICATION_MAX_SIZE:
                raise HttpResponseForbidden()
            buf.append(chunk)
        return b"".join(buf)

    job = VerificationJob.create(
        importer=importer,
        md5_name=get_filename("md5"),
        md5_data=read_file("md5"),
        xlsx_name=get_filename("xlsx"),
        xlsx_data=read_file("xlsx"),
    )
    job.set(complete=False)
    (
            validation_setup.s()
            | validate_spreadsheet.s()
            | validate_md5.s()
            | validate_bpaingest_json.s()
            | validate_complete.s()
    ).delay(job.uuid)

    return job.uuid

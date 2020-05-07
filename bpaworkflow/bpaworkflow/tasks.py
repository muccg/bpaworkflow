from celery import shared_task
import os
import pickle
import tempfile
import logging
import shutil
import redis
import uuid
import json

from deepdiff import DeepDiff

from .validate import verify_md5file, verify_spreadsheet, verify_metadata
from django.conf import settings
from collections import defaultdict
from bpaingest.metadata import DownloadMetadata
from pprint import pprint

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
    logger.debug("Task 1...")
    submission = TaskState(submission_id)
    # retrieved from Redis, so just do it once
    cls = submission.cls
    paths = submission.paths
    fake_metadata_info = submission.fake_metadata_info
    # these are fairly quick
    submission.xlsx = verify_spreadsheet(cls, paths["xlsx"], fake_metadata_info)
    logger.debug("Completed Task 1...")
    return submission_id


@shared_task(bind=True)
def validate_md5(self, submission_id):
    logger.debug("Task 2...")
    submission = TaskState(submission_id)
    # retrieved from Redis, so just do it once
    cls = submission.cls
    paths = submission.paths
    # these are fairly quick
    submission.md5 = verify_md5file(cls, paths["md5"])
    logger.debug("Completed Task 2...")
    return submission_id


@shared_task(bind=True)
def other_bpaingest_json(self, submission_id):
    submission = TaskState(submission_id)

    # retrieved from Redis, so just do it once
    cls = submission.cls


@shared_task(bind=True)
def validate_bpaingest_json(self, submission_id):
    logger.debug("Task 3...")
    submission = TaskState(submission_id)

    # retrieved from Redis, so just do it once
    cls = submission.cls
    fake_metadata_info = submission.fake_metadata_info
    paths = submission.paths

    def unchanged_metadata():
        logger.debug("....Unchanged download start...")
        download = DownloadMetadata(cls)
        logger.debug("....Unchanged download finished...")
        return download

    def new_metadata():
        logger.debug("....Changed download start...")
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
        logger.debug("....Changed download finished...")
        return dlmeta

    def linkage_qc(state):
        counts = {}
        # QC resource linkage
        logger.info('inside linkage state')
        for data_type in state:
            resource_linkage_package_id = {}
            packages = state[data_type]["packages"]
            resources = state[data_type]["resources"]
            data_type_meta = state[data_type]["meta"]
            counts[data_type] = len(packages), len(resources)
            logger.info("counts are %r" % counts)
            for package_obj in packages:
                logger.info('next obj in packages')
                linkage_tpl = tuple(
                    package_obj[t] for t in data_type_meta.resource_linkage
                )
                if linkage_tpl in resource_linkage_package_id:
                    logger.error(
                        "{}: more than one package linked for tuple {}".format(
                            data_type, linkage_tpl
                        )
                    )
                resource_linkage_package_id[linkage_tpl] = package_obj["id"]
            linked_tuples = set()
            for resource_linkage, legacy_url, resource_obj in resources:
                logger.info('next obj in resources')
                linked_tuples.add(resource_linkage)
                if resource_linkage not in resource_linkage_package_id:
                    dirname1, resource_name = os.path.split(legacy_url)
                    _dirname2, ticket = os.path.split(dirname1)
                    logger.error(
                        "dangling resource: {} (ticket: {}, linkage: {})".format(
                            resource_name, ticket, resource_linkage
                        )
                    )
            for linkage_tpl, package_id in resource_linkage_package_id.items():
                logger.info('next obj in resource linkages')
                if linkage_tpl not in linked_tuples:
                    logger.error(
                        "{}: package has no linked resources, tuple: {}".format(
                            package_id, linkage_tpl
                        )
                    )
        for data_type, (p, r) in counts.items():
            logger.debug("{}: {} packages, {} resources".format(data_type, p, r))

    def write_file2(tempd, file_obj):
        # be a bit paranoid, normalise and strip any path components out
        name = os.path.basename(file_obj.name)
        if not name:
            raise Exception("invalid filename provided in upload")
        fpath = os.path.join(tempd, name)
        with open(fpath, "wb") as fd:
            for chunk in file_obj.chunks():
                fd.write(chunk)
        return fpath

    def diff_json(json1, json2):
        logger.debug('starting json diff run ...................')
        ddiff = DeepDiff(json1, json2, ignore_order=True)
        logger.info(ddiff)
        logger.debug('completed json diff run ...................')



    def run(meta_maker):
        logger.debug('starting sub-task downloads run ...................')
        state = defaultdict(lambda: defaultdict(list))
        # download metadata for all project types and aggregate metadata keys
        # data_type_meta = {}
        with meta_maker() as dlmeta:
            meta = dlmeta.meta
            data_type = meta.ckan_data_type
            state[data_type]["meta"] = meta
            (packages, resources) = verify_metadata(meta)
            state[data_type]["packages"] += packages
            state[data_type]["resources"] += resources
            # try:
            #     logger.debug('inside next meta maker routine....')
            #     meta = dlmeta.meta
            #     # logger.info('meta properties are : %s' % pprint(vars(meta)))
            #     # logger.info('meta is : %s' % pprint(meta))
            #     data_type = meta.ckan_data_type
            #     # logger.info('data type is {}'.format(data_type))
            #     state[data_type]["meta"] = meta
            #     state[data_type]["packages"] += meta.get_packages()
            #     state[data_type]["resources"] += meta.get_resources()
            #     logger.debug('inside next meta maker routine completed.....')
            # except Exception as e:
            #     logger.error("Error processing metadata: {}".format(e))
            # finally:
            logger.debug("Continuing...")

        logger.debug('starting sorting state...................')
        for data_type in state:
            state[data_type]["packages"].sort(key=lambda x: x["id"])
            state[data_type]["resources"].sort(key=lambda x: x[2]["id"])
        logger.debug('finished sorting state...................')

        logger.debug('starting linking state...................')
        linkage_qc(state)
        logger.debug('finished linking state...................')
        logger.debug('completed sub-task downloads run...................')
        # with open(args.filename, 'w') as fd:
        #     json.dump(state, fd, sort_keys=True, indent=2, separators=(',', ': '))
        json_dump = json.dumps(state, sort_keys=True, indent=2, separators=(',', ': '))
        diff_json(json_dump, submission['json']['posted'])
        return state

    logger.info('Task 3a..........starting prior state....')
    prior_state = run(unchanged_metadata)
    logger.info('3b...........starting post state....')
    post_state = run(new_metadata)
    logger.debug("Completed Task 3...")
    return submission_id


# @shared_task(bind=True)
# def validate2_bpaingest_json(self, submission_id, state):
#     logger.debug("Task 4...")
#     submission = TaskState(submission_id)
#
#     def linkage_qc(state):
#         counts = {}
#
#         # QC resource linkage
#         logger.info('inside linkage state')
#         for data_type in state:
#             resource_linkage_package_id = {}
#
#             packages = state[data_type]["packages"]
#             resources = state[data_type]["resources"]
#             data_type_meta = state[data_type]["meta"]
#             counts[data_type] = len(packages), len(resources)
#             logger.info("counts are %r" % counts)
#
#             for package_obj in packages:
#                 logger.info('next obj in packages')
#                 linkage_tpl = tuple(
#                     package_obj[t] for t in data_type_meta.resource_linkage
#                 )
#                 if linkage_tpl in resource_linkage_package_id:
#                     logger.error(
#                         "{}: more than one package linked for tuple {}".format(
#                             data_type, linkage_tpl
#                         )
#                     )
#                 resource_linkage_package_id[linkage_tpl] = package_obj["id"]
#
#             linked_tuples = set()
#             for resource_linkage, legacy_url, resource_obj in resources:
#                 logger.info('next obj in resources')
#                 linked_tuples.add(resource_linkage)
#                 if resource_linkage not in resource_linkage_package_id:
#                     dirname1, resource_name = os.path.split(legacy_url)
#                     _dirname2, ticket = os.path.split(dirname1)
#                     logger.error(
#                         "dangling resource: {} (ticket: {}, linkage: {})".format(
#                             resource_name, ticket, resource_linkage
#                         )
#                     )
#
#             for linkage_tpl, package_id in resource_linkage_package_id.items():
#                 logger.info('next obj in resource linkages')
#                 if linkage_tpl not in linked_tuples:
#                     logger.error(
#                         "{}: package has no linked resources, tuple: {}".format(
#                             package_id, linkage_tpl
#                         )
#                     )
#
#         for data_type, (p, r) in counts.items():
#             logger.debug("{}: {} packages, {} resources".format(data_type, p, r))
#
#     logger.debug('starting linking state...................')
#     linkage_qc(state)
#     logger.debug('finished linking state...................')
#     logger.debug("Completed Task 4...")
#     return submission_id

@shared_task(bind=True)
def validate_complete(self, submission_id):
    logger.debug("Task 5...")
    submission = TaskState(submission_id)
    submission.complete = True
    logger.debug("Completed Task 5...")
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
        json = {}
        for field_name, file_obj in files.items():
            written_file = write_file(path, file_obj)
            if field_name != 'json':
                paths[field_name] = written_file
            else:
                json['posted'] = written_file
        return paths, json

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
    state.paths, state.json = write_files(state.path)
    state.fake_metadata_info = fabricate_metadata_info(state.path)
    (
            validate_spreadsheet.s()
            | validate_md5.s()
            | validate_bpaingest_json.s()
            | validate_complete.s()
    ).delay(state.submission_id)
    return state.submission_id

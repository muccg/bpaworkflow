import logging
import os
from functools import wraps

from xlrd import XLRDError

from bpaingest.libs.excel_wrapper import ExcelWrapper

logger = logging.getLogger("rainbow")
LOGGER = logging.getLogger("debug")


def exceptions_to_error(verification_func):
    @wraps(verification_func)
    def wrapped_verification(*args, **kwargs):
        try:
            return verification_func(*args, **kwargs)
        except Exception as e:
            return ["Verification failed with an error: %s" % (repr(e))]

    return wrapped_verification


@exceptions_to_error
def verify_spreadsheet(logging, cls, fpath, metadata_info):
    logging.debug("delegating to rainbow logger for Excel...")
    kwargs = cls.spreadsheet["options"]
    try:
        wrapper = ExcelWrapper(
            logger,
            cls.spreadsheet["fields"],
            fpath,
            additional_context=metadata_info[os.path.basename(fpath)],
            suggest_template=False,
            **kwargs
        )
    except XLRDError as e:
        return [
            "The provided spreadsheet could not be read: %s" % str(e),
            "Please ensure the spreadsheet is in Microsoft Excel (XLSX) format.",
        ]
    return wrapper.get_errors()


@exceptions_to_error
def verify_md5file(logger, cls, fpath):
    instance = cls(logger, "/dev/null")
    p = instance.parse_md5file_unwrapped(fpath)
    return ["File does not meet convention: `%s'" % t for t in p.no_match]


@exceptions_to_error
def verify_metadata(logging, meta):
    LOGGER.debug('inside next meta maker routine....')
    # logger.info('meta is : %s' % pprint(meta))
    # logger.info('data type is {}'.format(data_type))
    packages = meta.get_packages()
    resources = meta.get_resources()
    LOGGER.debug('inside next meta maker routine completed.....')
    return packages, resources

@exceptions_to_error
def linkage_qc(logging, state):
    counts = {}
    # QC resource linkage
    LOGGER.debug('inside linkage state')
    for data_type in state:
        resource_linkage_package_id = {}
        packages = state[data_type]["packages"]
        resources = state[data_type]["resources"]
        # remove data_type_meta as we don't want it in state anymore
        data_type_meta = state[data_type].pop("meta")
        counts[data_type] = len(packages), len(resources)
        LOGGER.debug("counts are %r" % counts)
        for package_obj in packages:
            linkage_tpl = tuple(
                package_obj[t] for t in data_type_meta.resource_linkage
            )
            if linkage_tpl in resource_linkage_package_id:
                LOGGER.error(
                    "{}: more than one package linked for tuple {}".format(
                        data_type, linkage_tpl
                    )
                )
            resource_linkage_package_id[linkage_tpl] = package_obj["id"]
        linked_tuples = set()
        for resource_linkage, legacy_url, resource_obj in resources:
            linked_tuples.add(resource_linkage)
            if resource_linkage not in resource_linkage_package_id:
                dirname1, resource_name = os.path.split(legacy_url)
                _dirname2, ticket = os.path.split(dirname1)
                LOGGER.error(
                    "dangling resource: {} (ticket: {}, linkage: {})".format(
                        resource_name, ticket, resource_linkage
                    )
                )
        for linkage_tpl, package_id in resource_linkage_package_id.items():
            if linkage_tpl not in linked_tuples:
                LOGGER.error(
                    "{}: package has no linked resources, tuple: {}".format(
                        package_id, linkage_tpl
                    )
                )
    for data_type, (p, r) in counts.items():
        LOGGER.debug("{}: {} packages, {} resources".format(data_type, p, r))

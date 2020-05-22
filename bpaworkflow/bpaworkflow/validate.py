import logging
import os
from functools import wraps

from xlrd import XLRDError

from bpaingest.libs.excel_wrapper import ExcelWrapper
from bpaingest.dump import linkage_qc

logger = logging.getLogger("rainbow")


def exceptions_to_error(verification_func):
    @wraps(verification_func)
    def wrapped_verification(*args, **kwargs):
        try:
            return verification_func(*args, **kwargs)
        except Exception as e:
            return ["Verification failed with an error: %s" % (repr(e))]

    return wrapped_verification


def linkage_collector(errors_collection):
    def collate(message):
        errors_collection.append(message)

    return collate


@exceptions_to_error
def collect_linkage_dump_linkage(logger, diff_state, post_data_type_meta):
    errors_collection = []
    collector = linkage_collector(errors_collection)
    linkage_qc(logger, diff_state, post_data_type_meta, collector)
    return errors_collection


@exceptions_to_error
def verify_spreadsheet(logging, cls, fpath, metadata_info):
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

import logging
import os
from functools import wraps

from xlrd import XLRDError

from bpaingest.libs.excel_wrapper import ExcelWrapper

logger = logging.getLogger("rainbow")


def exceptions_to_error(verification_func):
    @wraps(verification_func)
    def wrapped_verification(*args, **kwargs):
        try:
            return verification_func(*args, **kwargs)
        except Exception as e:
            return ["Verification failed with an error: %s" % (repr(e))]

    return wrapped_verification


@exceptions_to_error
def verify_spreadsheet(cls, fpath, metadata_info):
    kwargs = cls.spreadsheet["options"]
    try:
        wrapper = ExcelWrapper(
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
def verify_md5file(cls, fpath):
    p = cls.parse_md5file_unwrapped(fpath)
    return ["File does not meet convention: `%s'" % t for t in p.no_match]


@exceptions_to_error
def verify_metadata(meta):
    logger.debug('inside next meta maker routine....')
    # logger.info('meta is : %s' % pprint(meta))
    # logger.info('data type is {}'.format(data_type))
    packages = meta.get_packages()
    resources = meta.get_resources()
    logger.debug('inside next meta maker routine completed.....')
    return packages, resources

import os
import logging
from bpaingest.libs.excel_wrapper import ExcelWrapper

from tempfile import TemporaryDirectory
from functools import wraps
from xlrd import XLRDError


logger = logging.getLogger("rainbow")


def exceptions_to_error(verification_func):
    @wraps(verification_func)
    def wrapped_verification(*args, **kwargs):
        try:
            return verification_func(*args, **kwargs)
        except Exception as e:
            return ["Verification failed with an error: %s" % (repr(e))]

    return wrapped_verification


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


def run_validator(cls, files):
    with TemporaryDirectory(prefix="bpaworkflow") as tempd:
        paths = {}
        for field_name, file_obj in files.items():
            paths[field_name] = write_file(tempd, file_obj)

        # fabricate metadata information for the files uploaded by the user
        metadata_info = {}
        for filename in os.listdir(tempd):
            # synthesise harmless additional context data so we can run the ingestor
            metadata_info[os.path.basename(filename)] = obj = {
                "base_url": "https://example.com/does-not-exist/",
            }
            for k in cls.metadata_url_components:
                obj[k] = "BPAOPS-999"

        response = {}
        response["xlsx"] = verify_spreadsheet(cls, paths["xlsx"], metadata_info)
        response["md5"] = verify_md5file(cls, paths["md5"])
    return response

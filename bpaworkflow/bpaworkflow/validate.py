import os
import logging
from bpaingest.metadata import DownloadMetadata

from tempfile import TemporaryDirectory


logger = logging.getLogger("rainbow")


def write_file(tempd, file_obj):
    # be a bit paranoid, normalise and strip any path components out
    name = os.path.basename(file_obj.name)
    if not name:
        raise Exception("invalid filename provided in upload")
    with open(os.path.join(tempd, name), 'wb') as fd:
        for chunk in file_obj.chunks():
            fd.write(chunk)


def run_validator(cls, files):
    with TemporaryDirectory(prefix='bpaworkflow') as tempd:
        for field_name, file_obj in files.items():
            logger.critical('writing a file: %s' % repr(file_obj))
            write_file(tempd, file_obj)

        # we don't want the project metadata to download, so we
        # clear the project metadata URLs
        cls.metadata_urls = []
        # fabricate metadata information for the files uploaded by the user
        metadata_info = {}
        for filename in os.listdir(tempd):
            metadata_info[os.path.basename(filename)] = obj = {
                "base_url": "https://example.com/does-not-exist/",
            }
            for k in cls.metadata_url_components:
                obj[k] = 'BPAOPS-999'

        with DownloadMetadata(cls, path=tempd, force_fetch=True, metadata_info=metadata_info) as dlmeta:
            logger.critical(dlmeta.meta)
            dlmeta.meta.get_packages()
            dlmeta.meta.get_resources()

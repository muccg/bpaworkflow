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
    fpath = os.path.join(tempd, name)
    with open(fpath, 'wb') as fd:
        for chunk in file_obj.chunks():
            fd.write(chunk)
    return fpath


def verify_spreadsheet(cls, fpath, metadata_info):
    rows = cls.parse_spreadsheet(fpath, metadata_info)
    logger.critical(len(rows))


def run_validator(cls, files):
    with TemporaryDirectory(prefix='bpaworkflow') as tempd:
        paths = {}
        for field_name, file_obj in files.items():
            logger.critical('writing a file: %s' % repr(file_obj))
            paths[field_name] = write_file(tempd, file_obj)

        # fabricate metadata information for the files uploaded by the user
        metadata_info = {}
        for filename in os.listdir(tempd):
            # synthesise harmless additional context data so we can run the ingestor
            metadata_info[os.path.basename(filename)] = obj = {
                "base_url": "https://example.com/does-not-exist/",
            }
            for k in cls.metadata_url_components:
                obj[k] = 'BPAOPS-999'

        verify_spreadsheet(cls, paths['xlsx'], metadata_info)

        # we don't want the project metadata to download, so we
        # clear the project metadata URLs
        cls.metadata_urls = []

        with DownloadMetadata(cls, path=tempd, force_fetch=True, metadata_info=metadata_info) as dlmeta:
            logger.critical(dlmeta.meta)
            dlmeta.meta.get_resources()

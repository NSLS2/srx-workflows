from prefect import flow, task, get_run_logger

import glob
import os
import stat
import pyxrf
import dask
from pyxrf.api import make_hdf

from data_validation import get_run
from xanes_exporter import create_subdir

# from pyxrf.api import make_hdf

CATALOG_NAME = "srx"


@task
def export_xrf_hdf5(scanid, api_key=None, dry_run=False):
    logger = get_run_logger()

    logger.info(f"{pyxrf.__file__ = }")

    logger.info(f"{dask.__file__ = }")

    # Load header for our scan
    h = get_run(scanid, api_key=api_key)

    if h.start["scan"]["type"] not in ["XRF_FLY", "XRF_STEP"]:
        logger.info(
            "Incorrect document type. Not running pyxrf.api.make_hdf on this document."
        )
        return

    # Check if this is an alignment scan
    # scan_input array consists of [startx, stopx, number pts x, start y, stop y, num pts y, dwell]
    idx_NUM_PTS_Y = 5
    if (
        h.start["scan"]["type"] == "XRF_FLY"
        and h.start["scan"]["scan_input"][idx_NUM_PTS_Y] == 1
    ):
        logger.info(
            "This is likely an alignment scan. Not running pyxrf.api.make_hdf on this document."
        )
        return

    working_dir = (
        f"/nsls2/data/srx/proposals/{h.start['cycle']}/{h.start['data_session']}/xrfmaps"  # noqa: E501
    )

    create_subdir(working_dir)

    os.umask(0o007)  # Read/write access for user and group only

    prefix = "autorun_scan2D_"

    logger.info(f"{working_dir =}")
    os.environ["TILED_API_KEY"] = (
        api_key  # pyxrf assumes Tiled API key as an environment variable
    )
    if dry_run:
        logger.info("Dry run: not creating HDF5 file using PyXRF")
    else:
        make_hdf(scanid, wd=working_dir, prefix=prefix, catalog_name=CATALOG_NAME)

        # chmod g+w for created file(s)
        # context: https://nsls2.slack.com/archives/C04UUSG88VB/p1718911163624149
        for file in glob.glob(f"{working_dir}/{prefix}{scanid}*.h5"):
            os.chmod(file, os.stat(file).st_mode | stat.S_IWGRP)


@flow(log_prints=True)
def xrf_hdf5_exporter(scanid, api_key=None, dry_run=False):
    logger = get_run_logger()
    logger.info("Start writing file with xrf_hdf5 exporter...")
    export_xrf_hdf5(scanid, api_key=api_key, dry_run=dry_run)
    logger.info("Finish writing file with xrf_hdf5 exporter.")

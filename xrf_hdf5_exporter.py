from prefect import flow, task, get_run_logger

CATALOG_NAME = "srx"

import glob
import os
import stat

from utils import get_tiled_client
# from pyxrf.api import make_hdf

tiled_client = get_tiled_client()
tiled_client_raw = tiled_client["raw"]

@task
def export_xrf_hdf5(scanid):
    logger = get_run_logger()
    import pyxrf
    logger.info(f"{pyxrf.__file__ = }")
    import dask
    logger.info(f"{dask.__file__ = }")
    from pyxrf.api import make_hdf

    # Load header for our scan
    h = tiled_client_raw[scanid]

    if h.start["scan"]["type"] not in ["XRF_FLY", "XRF_STEP"]:
        logger.info(
            "Incorrect document type. Not running pyxrf.api.make_hdf on this document."
        )
        return

    # Check if this is an alignment scan
    # scan_input array consists of [startx, stopx, number pts x, start y, stop y, num pts y, dwell]
    idx_NUM_PTS_Y = 5
    if h.start["scan"]["type"] == "XRF_FLY" and h.start["scan"]["scan_input"][idx_NUM_PTS_Y] == 1:
        logger.info(
            "This is likely an alignment scan. Not running pyxrf.api.make_hdf on this document."
        )
        return

    if "SRX Beamline Commissioning".lower() in h.start["proposal"]["title"].lower():
        working_dir = f"/nsls2/data/srx/proposals/commissioning/{h.start['data_session']}"
    else:
        working_dir = f"/nsls2/data/srx/proposals/{h.start['cycle']}/{h.start['data_session']}"  # noqa: E501

    os.umask(0o007)  # Read/write access for user and group only

    prefix = "autorun_scan2D_"

    logger.info(f"{working_dir =}")
    make_hdf(scanid, wd=working_dir, prefix=prefix, catalog_name=CATALOG_NAME)

    # chmod g+w for created file(s)
    # context: https://nsls2.slack.com/archives/C04UUSG88VB/p1718911163624149
    for file in glob.glob(f"{working_dir}/{prefix}{scanid}*.h5"):
        os.chmod(file, os.stat(file).st_mode | stat.S_IWGRP)


@flow(log_prints=True)
def xrf_hdf5_exporter(scanid):
    logger = get_run_logger()
    logger.info("Start writing file with xrf_hdf5 exporter...")
    export_xrf_hdf5(scanid)
    logger.info("Finish writing file with xrf_hdf5 exporter.")

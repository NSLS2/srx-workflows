from prefect import flow, task, get_run_logger

CATALOG_NAME = "srx"

### Temporary solution until prefect deployment updates to 2024 environment ###
###############################################################################
import sys, os
conda_env = "2024-2.3-py310-tiled"
python_ver = "python3.10"
# overlay = [
#     f"/nsls2/data/srx/shared/config/bluesky_overlay/{conda_env}/lib/{python_ver}/site-packages",
#     f"/nsls2/conda/envs/{conda_env}/bin",
#     f"/nsls2/conda/envs/{conda_env}/lib/{python_ver}",
#     f"/nsls2/conda/envs/{conda_env}/lib/{python_ver}/lib-dynload",
#     f"/nsls2/conda/envs/{conda_env}/lib/{python_ver}/site-packages",
# ]
# sys.path[:0] = overlay
PYTHONUSERBASE = f"/nsls2/data/{CATALOG_NAME}/shared/config/bluesky_overlay/{conda_env}"
os.environ["PYTHONUSERBASE"] = PYTHONUSERBASE
sys.path[:0] = f"{PYTHONUSERBASE}/bin"
###############################################################################

import glob
import os
import stat

from tiled.client import from_profile
from pyxrf.api import make_hdf

tiled_client = from_profile("nsls2")[CATALOG_NAME]
tiled_client_raw = tiled_client["raw"]

@task
def export_xrf_hdf5(scanid):
    logger = get_run_logger()
    import pyxrf
    logger.info(f"{pyxrf.__file__ = }")

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
    if h.start["scan"]["scan_input"][idx_NUM_PTS_Y] == 1:
        logger.info(
            "This is likely an alignment scan. Not running pyxrf.api.make_hdf on this document."
        )
        return

    if "SRX Beamline Commissioning".lower() in h.start["proposal"]["title"].lower():
        working_dir = f"/nsls2/data/srx/proposals/commissioning/{h.start['data_session']}"
    else:
        working_dir = f"/nsls2/data/srx/proposals/{h.start['cycle']}/{h.start['data_session']}"  # noqa: E501

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

from prefect import task, flow, get_run_logger
from data_validation import data_validation
from xanes_exporter import xanes_exporter
from xrf_hdf5_exporter import xrf_hdf5_exporter
from logscan import logscan
from prefect.blocks.notifications import SlackWebhook

from tiled.client import from_profile
CATALOG_NAME = "srx"


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow
def end_of_run_workflow(stop_doc):
    try:
        uid = stop_doc["run_start"]

        # Get the scan_id
        tiled_client = from_profile("nsls2")[CATALOG_NAME]
        tiled_client_raw = tiled_client["raw"]
        scan_id = tiled_client_raw[uid].start['scan_id']

        # data_validation(uid, return_state=True)
        xanes_exporter(uid)
        xrf_hdf5_exporter(uid)
        logscan(uid)
        log_completion()
        slack_webhook_block = SlackWebhook.load("mon-prefect")
        slack_webhook_block.notify(f"Export successful\n run_start: {uid}\n scan_id: {scan_id}")
    except Exception as e:
        slack_webhook_block = SlackWebhook.load("mon-prefect")
        slack_webhook_block.notify(f"Export failed\n run_start: {uid}\n scan_id: {scan_id}")
        raise

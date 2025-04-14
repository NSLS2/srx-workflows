import traceback

from prefect import task, flow, get_run_logger
from prefect.blocks.notifications import SlackWebhook
from prefect.context import FlowRunContext

from data_validation import data_validation
from xanes_exporter import xanes_exporter
from xrf_hdf5_exporter import xrf_hdf5_exporter
from logscan import logscan

from tiled.client import from_profile

CATALOG_NAME = "srx"


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow
def end_of_run_workflow(stop_doc):
    flow_run_name = FlowRunContext.get().flow_run.dict().get('name')

    try:
        uid = stop_doc["run_start"]

        # Get the scan_id
        tiled_client = from_profile("nsls2")[CATALOG_NAME]
        tiled_client_raw = tiled_client["raw"]
        scan_id = tiled_client_raw[uid].start["scan_id"]

        # data_validation(uid, return_state=True)
        xanes_exporter(uid)
        xrf_hdf5_exporter(uid)
        logscan(uid)
        log_completion()
    except Exception as e:
        tb = traceback.format_exception_only(e)
        slack_webhook_block = SlackWebhook.load("mon-prefect")
        slack_webhook_block.notify(
                f":bangbang: SRX flow-run failed. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}``` ```{tb[-1]}```"
        )
        raise

    slack_webhook_block = SlackWebhook.load("mon-prefect")
    slack_webhook_block.notify(
        f":white_check_mark: SRX flow-run successful. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}```"
    )

from prefect import task, flow, get_run_logger
from data_validation import data_validation
from xanes_exporter import xanes_exporter
from xrf_hdf5_exporter import xrf_hdf5_exporter
from logscan import logscan
from prefect.blocks.notifications import SlackWebhook


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow
def end_of_run_workflow(stop_doc):
    try:
        uid = stop_doc["run_start"]
        # data_validation(uid, return_state=True)
        xanes_exporter(uid)
        xrf_hdf5_exporter(uid)
        logscan(uid)
        log_completion()
        slack_webhook_block = SlackWebhook.load("mon-prefect")
        slack_webhook_block.notify(f"Hello from SRX! run_start: {uid}")
    except Exception as e:
        slack_webhook_block = SlackWebhook.load("mon-prefect")
        slack_webhook_block.notify(f"@srx-support Export failed for run_start: {uid}")
        raise e

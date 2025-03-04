from pathlib import Path
from prefect import flow, task, get_run_logger
from tiled.client import from_profile


tiled_client = from_profile("nsls2")["srx"]
tiled_client_raw = tiled_client["raw"]


def find_scanid(logfile_path, scanid):
    is_scanid = False
    with open(logfile_path) as lf:
        for line in lf:
            params = line.strip().split("\t")
            if int(params[0]) == scanid:
                is_scanid = True
                break
    return is_scanid


@task
def logscan_detailed(scanid):
    logger = get_run_logger()

    h = tiled_client_raw[scanid]

    if (
        "Beamline Commissioning (beamline staff only)".lower()
        in h.start["proposal"]["type"].lower()
    ):
        userdatadir = (
            f"/nsls2/data/srx/proposals/commissioning/{h.start['data_session']}/"
        )
    else:
        userdatadir = (
            f"/nsls2/data/srx/proposals/{h.start['cycle']}/{h.start['data_session']}/"
        )

    if not Path(userdatadir).exists():
        logger.info(
            "Incorrect path. Check cycle and proposal id in document. Not running the logger on this document."
        )
        return

    logfile_path = userdatadir + f"logfile{h.start['data_session']}.txt"
    is_scanid = False
    if Path(logfile_path).exists():
        is_scanid = find_scanid(logfile_path, h.start["scan_id"])

    if not is_scanid:
        # Let's build the string
        #   Each scan should have a scan ID and UID
        out_str = f"{h.start['scan_id']}\t{h.start['uid']}"
        #   I don't think the "scan" dictionary is guaranteed with each scan
        #   I think this is SRX-custom-scan specific
        if "scan" in h.start:
            # type probably exists if scan exists, but better to check
            if "type" in h.start["scan"]:
                out_str += f"\t{h.start['scan']['type']}"
                # This is not in every scan type, e.g. peakup
                if "scan_input" in h.start["scan"]:
                    out_str += f"\t{h.start['scan']['scan_input']}"
        else:
            # We should probably record what the scan was, e.g. count, scan, rel_scan
            if "plan_name" in h.start:
                out_str += f"\t{h.start['plan_name']}"
            else:
                out_str += f"\tunknown scan"
        out_str += "\n"

        # Write to file
        with open(logfile_path, "a") as userlogf:
            userlogf.write(out_str)
            logger.info(f"Added {h.start['scan_id']} to the logs")


@flow(log_prints=True)
def logscan(ref):
    logger = get_run_logger()
    logger.info("Start writing logfile...")
    logscan_detailed(ref)
    logger.info("Finish writing logfile.")

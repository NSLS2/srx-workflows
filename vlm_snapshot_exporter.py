from prefect import flow, task, get_run_logger
import numpy as np
import os
from PIL import Image
from data_validation import get_run


@flow(log_prints=True)
def vlm_image_exporter(ref, api_key=None, dry_run=False):
    logger = get_run_logger()
    logger.info("")

    scan_id = get_run(uid, api_key=api_key).start["scan_id"]
    logger.info(f"Looking for snapshots in scan {scan_id}.")

    export_vlm_image(
        scan_id, overlay=True, raw_image=True, api_key=api_key, dry_run=dry_run
    )
    logger.info(f"Finished exporting any snapshots in scan {scan_id}.")


@task
def export_vlm_image(
    scan_id,
    wd=None,
    overlay=True,
    raw_image=True,
    autoscale=True,
    api_key=None,
    dry_run=False,
):
    logger = get_run_logger()

    # Pseudocode
    # overlays == True
    #       raw_images == True:
    #           Saves both raw images and overlays
    #       raw_images == False:
    #           Saves only overlaid images
    # overlays == False
    #       raw_images == True:
    #           Saves only raw images
    #       raw_images == False:
    #           raises RuntimeError

    # Initial checks
    # Does scan exist
    scan_id = int(scan_id)
    h = get_run(scan_id, api_key=api_key)

    # VLM image data acquired?
    if "camera_snapshot" not in h:
        warn_str = f"No VLM images found for scan {scan_id}."
        logger.info(warn_str)
        return

    if wd is None:
        proposal_id = h.start["proposal"]["proposal_id"]
        cycle = h.start["cycle"]
        wd = f"/nsls2/data/srx/proposals/{cycle}/pass-{proposal_id}/"

    # Create sub-folder
    wd = f"{wd}vlm_snapshots/"
    os.makedirs(wd, exist_ok=True)

    images = h["camera_snapshot"]["data"]["nano_vlm_image"][:, 0].astype(np.float64)

    # logger.info('VLM images found; writing images to folder.')
    for image, title in zip(images, ["before", "after"]):
        # Normalize
        image -= image.mean() - 2 * image.std()
        image /= image.mean() + 2 * image.std()
        image[image < 0] = 0
        image[image > 1] = 1

        # Return raw png
        image = Image.fromArray((image * 65535).astype(np.uint16))
        filename = os.path.join(wd, f"scan{scan_id}_VLM_image_{title}.png")
        if dry_run:
            logger.info(f"Dry run: Not saving image to {filename}")
        else:
            image.save(filename)

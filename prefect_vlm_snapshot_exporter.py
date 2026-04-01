from prefect import flow, task, get_run_logger
import time as ttime
import numpy as np
import os
from skimage import io
from PIL import Image, ImageDraw, ImageFont
import matplotlib.pyplot as plt
from data_validation import get_run


@flow(log_prints=True)
def vlm_image_exporter(ref, api_key=None):
    logger = get_run_logger()
    logger.info('')

    scan_id = get_run(uid, api_key=api_key).start['scan_id']
    logger.info(f'Looking for snapshots in scan {scan_id}.')

    export_vlm_image(scan_id,
                     overlay=True,
                     raw_image=True,
                     image_type='.tif')
    logger.info(f'Finished exporting any snapshots in scan {scan_id}.')


@task
def export_vlm_image(scan_id,
                     wd=None,
                     overlay=True,
                     raw_image=True,
                     image_type='.tif',
                     autoscale=True,
                     api_key=None
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
    if 'camera_snapshot' not in h:
        warn_str = f'No VLM images found for scan {scan_id}.'
        logger.info(warn_str)
        return    
    
    # Logical combinations
    if not overlay and not raw_image:
        err_str = "One or both of 'overlay' and 'raw_image' must be True."
        raise ValueError(err_str)

    image_type = str(image_type)
    if image_type[0] != '.':
        image_type = '.' + image_type
    supported_image_types = {'.tif', '.tiff', '.png', '.bmp', '.jpeg'}
    if image_type not in supported_image_types:
        err_str = (f"Image type of ({image_type}) is not supported. "
                   + f"Only supported image types are {supported_image_types}.")
        raise TypeError(err_str)
    
    if wd is None:
        proposal_id = h.start['proposal']['proposal_id']
        cycle = h.start['cycle']
        wd = f'/nsls2/data/srx/proposals/{cycle}/pass-{proposal_id}/'
    
    # Create sub-folder
    wd = f'{wd}vlm_snapshots/'
    os.makedirs(wd, exist_ok=True)
    
    images = h['camera_snapshot']['data']['nano_vlm_image'][:, 0].astype(np.uint16)

    # logger.info('VLM images found; writing images to folder.')
    for image, title in zip(images, ['before', 'after']):
        # Normalize
        image = image.astype(np.float64)
        if autoscale:
            image -= image.mean() - 2 * image.std()
            image /= image.mean() + 2 * image.std()
            image[image < 0] = 0
            image[image > 1] = 1
        else:
            image -= image.min()
            image /= image.max()

        # Return raw tiff
        if raw_image:
            io.imsave(os.path.join(wd, f'scan{scan_id}_VLM_image_{title}{image_type}'),
                      (image * 65535).astype(np.uint16),
                      check_contrast=False)
        if overlay:
            marker = tuple([h.start['scan']['detectors']['nano_vlm'][f'cross_position_{a}'] for a in ['x', 'y']])

            overlayed_image = _overlay_image(image.copy(), marker)
            io.imsave(os.path.join(wd, f'scan{scan_id}_VLM_image_{title}_overlay{image_type}'),
                      overlayed_image.astype(np.uint8),
                      check_contrast=False)


def _overlay_image(image, marker):

    # Hard-coded scale
    scale = 0.5 # pixels/um
    bar_length_um = 200
    bar_length_px = int(bar_length_um / scale)
    bar_height_px = 15
    unit_name = 'μm'

    position = 'lower_right'
    color = (255, 0, 0)

    # Convert to grayscale and leave contiguous in memory
    image = plt.cm.gray(image / image.max())[:, :, :3].copy()
    image = (image * 255).astype(np.uint8)
    image = Image.fromarray(image)
    draw = ImageDraw.Draw(image)

    # Get image dimensions
    w, h = image.size

    # Define padding
    v_padding = 75
    h_padding = 50

    # Determine position
    bar_x1 = w - h_padding - bar_length_px
    bar_y1 = h - v_padding - bar_height_px
    text_x = bar_x1 + bar_length_px // 2
    text_y = bar_y1 + bar_height_px + 5

    bar_x2 = bar_x1 + bar_length_px
    bar_y2 = bar_y1 + bar_height_px

    # Draw the scale bar rectangle
    # print(f'{bar_x1=}', f'{bar_x2=}')
    # print(f'{bar_y1=}', f'{bar_y2=}')

    # Add scalebar
    draw.rectangle([bar_x1, bar_y1, bar_x2, bar_y2], fill=color)

    # Add text label
    text = f"{bar_length_um} {unit_name}"
    font = ImageFont.truetype("DejaVuSans.ttf", 50)
    text_width, text_height = draw.textbbox((0, 0), text, font=font)[2:]
    # print(f'{text_width=}', f'{text_height=}')
    draw.text((text_x - (text_width // 2),
               text_y), text, font=font, fill=color,
               stroke_width=1, # 2 for bold
              )

    # Add beam position indicator (+)
    marker_size = 50
    marker_width = 10
    draw.line((marker[0] - marker_size // 2,
               marker[1],
               marker[0] + marker_size // 2,
               marker[1]),
               fill=color,
               width=marker_width)
    draw.line((marker[0],
               marker[1] - marker_size // 2,
               marker[0],
               marker[1] + marker_size // 2),
               fill=color,
               width=marker_width)           

    return np.asarray(image, dtype=np.uint8)

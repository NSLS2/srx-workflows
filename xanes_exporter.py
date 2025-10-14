from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from tiled.client import from_profile

import time as ttime
import numpy as np
import xraylib as xrl
import pandas as pd


api_key = Secret.load("tiled-srx-api-key", _sync=True).get()
tiled_client = from_profile("nsls2", api_key=api_key)["srx"]
tiled_client_raw = tiled_client["raw"]


def xanes_textout(
    scanid=-1,
    header=[],
    userheader={},
    column=[],
    usercolumn={},
    usercolumnname=[],
    output=True,
):
    """
    scan: can be scan_id (integer) or uid (string). default=-1 (last scan run)
    header: a list of items that exist in the event data to be put into
            the header
    userheader: a dictionary defined by user to put into the header
    column: a list of items that exist in the event data to be put into
            the column data
    output: print all header fileds. if output = False, only print the ones
            that were able to be written
            default = True

    """

    h = tiled_client_raw[scanid]
    if (
        "Beamline Commissioning (beamline staff only)".lower()
        in h.start["proposal"]["type"].lower()
    ):
        filepath = f"/nsls2/data/srx/proposals/commissioning/{h.start['data_session']}/scan_{h.start['scan_id']}_xanes.txt"
    else:
        filepath = f"/nsls2/data/srx/proposals/{h.start['cycle']}/{h.start['data_session']}/scan_{h.start['scan_id']}_xanes.txt"  # noqa: E501

    with open(filepath, "w") as f:
        dataset_client = h["primary"]["data"]

        staticheader = (
            "# XDI/1.0 MX/2.0\n"
            + "# Beamline.name: "
            + h.start["beamline_id"]
            + "\n"
            + "# Facility.name: NSLS-II\n"
            + "# Facility.ring_current:"
            + str(dataset_client["ring_current"][0])
            + "\n"
            + "# Scan.start.uid: "
            + h.start["uid"]
            + "\n"
            + "# Scan.start.time: "
            + str(h.start["time"])
            + "\n"
            + "# Scan.start.ctime: "
            + ttime.ctime(h.start["time"])
            + "\n"
            + "# Mono.name: Si 111\n"
        )

        f.write(staticheader)

        for item in header:
            if item in dataset_client.keys():
                f.write("# " + item + ": " + str(dataset_client[item]) + "\n")
                if output is True:
                    print(f"{item} is written")
            else:
                print(f"{item} is not in the scan")

        for key in userheader:
            f.write("# " + key + ": " + str(userheader[key]) + "\n")
            if output is True:
                print(f"{key} is written")

        file_data = {}
        for idx, item in enumerate(column):
            if item in dataset_client.keys():
                # retrieve the data from tiled that is going to be used
                # in the file
                file_data[item] = dataset_client[item].read()
                f.write("# Column." + str(idx + 1) + ": " + item + "\n")

        f.write("# ")
        for item in column:
            if item in dataset_client.keys():
                f.write(str(item) + "\t")

        for item in usercolumnname:
            f.write(item + "\t")

        f.write("\n")
        f.flush()

        offset = False
        for idx in range(len(file_data[column[0]])):
            for item in column:
                if item in file_data:
                    f.write("{0:8.6g}  ".format(file_data[item][idx]))
            for item in usercolumnname:
                if item in usercolumn:
                    if offset is False:
                        try:
                            f.write("{0:8.6g}  ".format(usercolumn[item][idx]))
                        except KeyError:
                            offset = True
                            f.write("{0:8.6g}  ".format(usercolumn[item][idx + 1]))
                    else:
                        f.write("{0:8.6g}  ".format(usercolumn[item][idx + 1]))
            f.write("\n")


@task
def xanes_afterscan_plan(scanid):
    logger = get_run_logger()

    # Custom header list
    headeritem = []
    # Load header for our scan
    h = tiled_client_raw[scanid]

    if h.start["scan"].get("type") != "XAS_STEP":
        logger.info("Incorrect document type. Not running exporter on this document.")
        return
    # Construct basic header information
    userheaderitem = {}
    userheaderitem["uid"] = h.start["uid"]
    userheaderitem["sample.name"] = h.start["scan"]["sample_name"]

    # Create columns for data file
    columnitem = ["energy_energy", "energy_bragg", "energy_c2_x"]
    # Include I_M, I_0, and I_t from the SRS
    if "sclr1" in h.start["detectors"]:
        if "sclr_i0" in h["primary"].descriptors[0]["object_keys"]["sclr1"]:
            columnitem.extend(["sclr_im", "sclr_i0", "sclr_it"])
        else:
            columnitem.extend(["sclr1_mca3", "sclr1_mca2", "sclr1_mca4"])

    else:
        raise KeyError("SRS not found in data!")
    # Include fluorescence data if present, allow multiple rois
    if "xs" in h.start["detectors"]:
        if "ROI" in h.start["scan"].keys():
            roinum = list(h.start["scan"]["ROI"])
        else:
            roinum = [1]  # if no ROI key found, assume ROI 1
        logger.info(roinum)
        for i in roinum:
            logger.info(f"Current roinumber: {i}")
            roi_name = "roi{:02}".format(i)
            roi_key = []

            xs_channels = h["primary"].descriptors[0]["object_keys"]["xs"]
            for xs_channel in xs_channels:
                logger.info(f"Current xs_channel: {xs_channel}")
                if (
                    "mca" + roi_name in xs_channel and "total_rbv" in xs_channel
                ):  # noqa: E501
                    roi_key.append(xs_channel)

            columnitem.extend(roi_key)

    # if ('xs2' in h.start['detectors']):
    #     if (type(roinum) is not list):
    #         roinum = [roinum]
    #     for i in roinum:
    #         roi_name = 'roi{:02}'.format(i)
    #         roi_key = []
    #         roi_key.append(getattr(xs2.channel1.rois, roi_name).value.name)

    # [columnitem.append(roi) for roi in roi_key]

    # Construct user convenience columns allowing prescaling of ion chamber,
    # diode and fluorescence detector data
    usercolumnitem = {}
    datatablenames = []

    if "xs" in h.start["detectors"]:
        datatablenames.extend(roi_key)
    if "xs2" in h.start["detectors"]:
        datatablenames.extend(roi_key)
    if "sclr1" in h.start["detectors"]:
        if "sclr_im" in h["primary"].descriptors[0]["object_keys"]["sclr1"]:
            datatablenames.extend(["sclr_im", "sclr_i0", "sclr_it"])
            datatable = h["primary"].read(datatablenames)
        else:
            datatablenames.extend(["sclr1_mca2", "sclr1_mca3", "sclr1_mca4"])
            datatable = h["primary"].read(datatablenames)
    else:
        raise KeyError
    # Calculate sums for xspress3 channels of interest
    if "xs" in h.start["detectors"]:
        for i in roinum:
            roi_name = "roi{:02}".format(i)
            roi_key = []
            for xs_channel in xs_channels:
                if (
                    "mca" + roi_name in xs_channel and "total_rbv" in xs_channel
                ):  # noqa: E501
                    roi_key.append(xs_channel)
            roisum = sum(datatable[roi_key].to_array()).to_series()
            roisum = roisum.rename_axis("seq_num").rename(lambda x: x + 1)
            usercolumnitem["If-{:02}".format(i)] = roisum
            # usercolumnitem['If-{:02}'.format(i)].round(0)

    # if 'xs2' in h.start['detectors']:
    #     for i in roinum:
    #         roi_name = 'roi{:02}'.format(i)
    #         roisum=datatable[getattr(xs2.channel1.rois,roi_name).value.name]
    #         usercolumnitem['If-{:02}'.format(i)] = roisum
    #         usercolumnitem['If-{:02}'.format(i)].round(0)

    xanes_textout(
        scanid=scanid,
        header=headeritem,
        userheader=userheaderitem,
        column=columnitem,
        usercolumn=usercolumnitem,
        usercolumnname=usercolumnitem.keys(),
        output=False,
    )


@task
def xas_fly_exporter(uid):
    logger = get_run_logger()
    # Get a scan header
    hdr = tiled_client_raw[uid]
    start_doc = hdr.start

    # Get proposal directory location
    if (
        "Beamline Commissioning (beamline staff only)".lower()
        in hdr.start["proposal"]["type"].lower()
    ):
        root = f"/nsls2/data/srx/proposals/commissioning/{hdr.start['data_session']}/"
    else:
        root = f"/nsls2/data/srx/proposals/{hdr.start['cycle']}/{hdr.start['data_session']}/"

    # Identify scan streams
    scan_streams = [s for s in hdr if s != "baseline" and "monitor" not in s]

    # ROI information
    roi_num = start_doc["scan"]["roi_num"]
    roi_name = start_doc["scan"]["roi_names"][roi_num - 1]
    roi_symbol, roi_line = roi_name.split("_")
    roi_Z = xrl.SymbolToAtomicNumber(roi_symbol)
    if "ka" in roi_line.lower():
        roi_line_ind = xrl.KA_LINE
    elif "kb" in roi_line.lower():
        roi_line_ind = xrl.KB_LINE
    elif "la" in roi_line.lower():
        roi_line_ind = xrl.LA_LINE
    elif "lb" in roi_line.lower():
        roi_line_ind = xrl.LB_LINE
    else:
        logger.info("Line identification failed")
        return

    # Get bin values
    E = xrl.LineEnergy(roi_Z, roi_line_ind)
    E_bin = np.round(E * 100, decimals=0).astype(int)
    E_width = 10
    E_min = E_bin - E_width
    E_max = E_bin + E_width

    # Get ring current
    ring_current_start = np.round(
        hdr["baseline"]["data"]["ring_current"][0], decimals=0
    ).astype(str)

    # Static header
    staticheader = (
        f"# XDI/1.0 MX/2.0\n"
        + f"# Beamline.name: {hdr.start['beamline_id']}\n"
        + f"# Facility.name: NSLS-II\n"
        + f"# Facility.ring_current: {ring_current_start}\n"
        + f"# IVU.harmonic: {hdr.start['scan']['harmonic']}\n"
        + f"# Mono.name: Si 111\n"
        + f"# Scan.start.uid: {hdr.start['uid']}\n"
        + f"# Scan.start.scanid: {hdr.start['scan_id']}\n"
        + f"# Scan.start.time: {hdr.start['time']}\n"
        + f"# Scan.start.ctime: {ttime.ctime(hdr.start['time'])}\n"
        + f"# Scan.ROI.name: {roi_name}\n"
        + f"# Scan.ROI.number: {roi_num}\n"
        + f"# Scan.ROI.range: {f'[{E_min}:{E_max}]'}\n"
        + f"# \n"
    )

    for stream in sorted(scan_streams):
        # Set a filename
        fname = f"scan_{hdr.start['scan_id']}_{stream}.txt"
        fname = root + fname

        # Get the full table
        tbl = hdr[stream]["data"]
        df = pd.DataFrame()
        keys = [k for k in tbl.keys()[:] if "time" not in k]
        for k in keys:
            if "channel" in k:
                # We will process later
                continue
            df[k] = tbl[k].read()

        df.set_index("energy", drop=True, inplace=True)

        ch_names = [ch for ch in keys if "channel" in ch]
        for ch in ch_names:
            df[ch] = np.sum(tbl[ch].read()[:, E_min:E_max], axis=1)
            df.rename(columns={ch: ch.split("_")[-1]}, inplace=True)
        df["ch_sum"] = df[[ch for ch in df.keys() if "channel" in ch]].sum(axis=1)

        # Prepare for export
        col_names = [df.index.name] + list(df.columns)
        for i, col in enumerate(col_names):
            staticheader += f"# Column {i+1:02}: {col}\n"
        staticheader += "# \n# "

        # Export data to file
        with open(fname, "w") as f:
            f.write(staticheader)
        df.to_csv(fname, float_format="%.3f", sep=" ", mode="a")


@flow(log_prints=True)
def xanes_exporter(ref):
    logger = get_run_logger()
    logger.info("Start writing file with xanes_exporter...")

    # Get scan type
    scan_type = tiled_client_raw[ref].start.get("scan", {}).get("type", "unknown")

    # Redirect to correction function - or pass
    if scan_type == "XAS_STEP":
        logger.info("Starting xanes step-scan exporter.")
        xanes_afterscan_plan(ref)
        logger.info("Finished writing file with xanes step-scan exporter.")
    elif scan_type == "XAS_FLY":
        logger.info("Starting xanes fly-scan exporter.")
        xas_fly_exporter(ref)
        logger.info("Finished writing file with xanes fly-scan exporter.")
    else:
        logger.info(f"xanes exporter for {scan_type=} not available")

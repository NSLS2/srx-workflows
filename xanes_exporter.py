from prefect import flow, task, get_run_logger
from tiled.client import from_profile

import time as ttime


tiled_client = from_profile("nsls2")["srx"]
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
    if "Beamline Commissioning (beamline staff only)".lower() in h.start["proposal"]["type"].lower():
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
                file_data[item] = dataset_client[
                    item
                ].read()
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
                            f.write("{0:8.6g}  ".format(
                                usercolumn[item][idx + 1]))
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
        logger.info(
            "Incorrect document type. Not running exporter on this document."
        )
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
                if "mca" + roi_name in xs_channel and "total_rbv" in xs_channel:  # noqa: E501
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
                if "mca" + roi_name in xs_channel and "total_rbv" in xs_channel:  # noqa: E501
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


@flow(log_prints=True)
def xanes_exporter(ref):
    logger = get_run_logger()
    logger.info("Start writing file with xanes_exporter...")
    xanes_afterscan_plan(ref)
    logger.info("Finish writing file with xanes_exporter.")


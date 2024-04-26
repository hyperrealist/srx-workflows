from prefect import flow, task, get_run_logger
from tiled.client import from_profile
from pyxrf.api import make_hdf

tiled_client = from_profile("nsls2")["srx"]
tiled_client_raw = tiled_client["raw"]

@task
def export(scanid):
    logger = get_run_logger()

    # Load header for our scan
    h = tiled_client_raw[scanid]

    if h.start["scan"]["type"] not in ["XRF_FLY", "XRF_STEP"]:
        logger.info(
            "Incorrect document type. Not running pyxrf.api.make_hdf on this document."
        )
        return

    working_dir = f"/nsls2/data/srx/proposals/{h.start['cycle']}/{h.start['data_session']}"  # noqa: E501
    prefix = "autorun_scan2D"

    logger.info(f"{working_dir =}")
    make_hdf(scanid, wd=working_dir, prefix=prefix)

@flow(log_prints=True)
def xrf_hdf5_exporter(scanid):
    logger = get_run_logger()
    logger.info("Start writing file...")
    export(scanid)
    logger.info("Finish writing file.")

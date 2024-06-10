from pathlib import Path
from prefect import flow, task, get_run_logger
from tiled.client import from_profile

tiled_client = from_profile("nsls2")["srx"]
tiled_client_raw = tiled_client["raw"]


def find_scanid(logfile_path, scanid):
    is_scanid = False
    with open(logfile_path) as lf:
        lines = lf.readlines()
        for line in lines:
            params = line.strip().split("\t")
            if int(params[0]) == scanid:
                is_scanid = True
                break
    return is_scanid


@task
def logscan_detailed(scanid):
    logger = get_run_logger()
    
    h = tiled_client_raw[scanid]
    
    if "SRX Beamline Commissioning".lower() in h.start["proposal"]["proposal_title"].lower():
        userdatadir = f"/nsls2/data/srx/proposals/commissioning/{h.start['data_session']}/"
    else:
        userdatadir = f"/nsls2/data/srx/proposals/{h.start['cycle']}/{h.start['data_session']}/"
        
    if not Path(userdatadir).exist():
        logger.info(
            "Incorrect path. Check cycle and proposal id in document. Not running the logger on this document."
        )
        return
    
    logfile_path = userdatadir + f"logfile{h.start['data_session']}.txt"
    is_scanid = False
    if Path(logfile_path).exists():
        is_scanid = find_scanid(logfile_path, h.start['scan_id'])
    
    if not is_scanid:
        with open(logfile_path, "a") as userlogf:
            userlogf.write(str(h.start['scan_id']) + '\t' + str(h.start['uid']) + '\t' + str(h.start['scan']['type']) + '\t' + str(h.start['scan']['scan_input']) + '\n')
            logger.info(f"Added {h.start['scan_id']} to the logs")


@flow(log_prints=True)
def logscan(ref):
    logger = get_run_logger()
    logger.info("Start writing logfile...")
    logscan_detailed(ref)
    logger.info("Finish writing logfile.")
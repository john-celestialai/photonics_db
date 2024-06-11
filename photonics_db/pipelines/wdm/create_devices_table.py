"""
A simple script for uploading the WDM DOE metadata to a database table
"""

from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from photonics_db.tables.wdm import WDMDevices


def create_devices_table(session: Session):

    # Load and clean the WDM DOE table data
    devices_file = Path(__file__).parent / Path("wdm_devices.csv")
    devices_df = pd.read_csv(devices_file, sep=",", header=0)
    devices = devices_df.replace({np.nan: None}).to_dict("records")

    session.execute(insert(WDMDevices).on_conflict_do_nothing(), devices)
    session.commit()


if __name__ == "__main__":
    from sqlalchemy import create_engine

    from photonics_db import database_address

    engine = create_engine(database_address + "/john_dev")
    with Session(engine) as sess:
        create_devices_table(sess)

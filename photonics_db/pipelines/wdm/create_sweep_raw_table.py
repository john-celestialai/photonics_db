from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from photonics_db.tables.wdm import *

wrap_io = lambda x: int((x - 1) % 3 + 1)


def create_sweep_raw_table(session: Session):

    directory = Path("/Users/jrollinson/projects/wdm_ap_data_upload") / Path("WDM")
    rows = []

    for filename in directory.rglob("*EULER*.csv"):
        # Look up the measurement id based on (wafer_id, die_id, device_id)
        attrs = filename.stem.split("__")
        wafer_id = attrs[1].replace("IM4477232804", "R2P0E433PLG6")
        die_id = "_".join(attrs[3:5])
        device_id = attrs[0]
        result = session.execute(
            sa.select(WDMMeasurements)
            .where(WDMMeasurements.wafer_id == wafer_id)
            .where(WDMMeasurements.die_id == die_id)
            .where(WDMMeasurements.device_id == device_id)
        ).scalar_one()

        # Load the measurement sweeps file
        df = pd.read_csv(
            filename,
            sep=",",
            skiprows=2,
            names=["input", "output", "wavelength_nm", "transmission_db"],
        )

        # Split the measurement file into the individual sweeps
        for i, (name, subtable) in enumerate(df.groupby(["input", "output"])):
            measurement = {
                "measurement_id": result.measurement_id,
                "sweep_id": i,
                "input": wrap_io(name[0]),
                "output": wrap_io(name[1]),
                "voltage_v": None,
                "current_ma": None,
                "wavelength_nm": subtable["wavelength_nm"].tolist(),
                "transmission_db": subtable["transmission_db"].tolist(),
            }

            rows.append(measurement)

    session.execute(insert(WDMSweepRaw).on_conflict_do_nothing(), rows)
    session.commit()

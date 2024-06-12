from pathlib import Path

import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from photonics_db.tables.wdm import WDMSweepDeembed

wrap_io = lambda x: int((x - 1) % 3 + 1)


def create_sweep_deembed_table(session: Session):
    directory = Path("/Users/jrollinson/projects/wdm_ap_data_upload") / Path("WDM")
    rows = []
    for filename in directory.rglob("*GCDE*.csv"):

        # Look up the measurement id based on (wafer_id, die_id, device_id)
        attrs = filename.stem.split("__")
        wafer_id = attrs[1].replace("IM4477232804", "R2P0E433PLG6")
        die_id = "_".join(attrs[3:5])

        deembed_id = "_".join([wafer_id, die_id, attrs[0]])
        doe_column = attrs[0].split("_")[-1]

        # Split the measurement file into the individual sweeps
        df = pd.read_csv(
            filename,
            sep=",",
            skiprows=2,
            names=["input", "output", "wavelength_nm", "transmission_db"],
        )

        measurement = {
            "deembed_id": deembed_id,
            "doe_column": doe_column,
            "input": wrap_io(df.input[0]),
            "output": wrap_io(df.output[0]),
            "wavelength_nm": df.wavelength_nm.tolist(),
            "transmission_db": df.transmission_db.tolist(),
        }

        rows.append(measurement)

    session.execute(insert(WDMSweepDeembed).on_conflict_do_nothing(), rows)
    session.commit()

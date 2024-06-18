import json
from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session

from photonics_db.tables.wdm import WDMSweepDeembed

wrap_io = lambda x: int((x - 1) % 3 + 1)


def create_sweep_deembed_table(session: Session, directory: Path):
    for filename in directory.rglob("*GCDE*.csv"):
        with open(filename, "r") as f:
            line = str(f.readline()).replace(",}", "}")
            header = json.loads(line)

        # Look up the measurement id based on (wafer_id, die_id, device_id)
        attrs = filename.stem.split("__")
        wafer_id = attrs[1].replace("IM4477232804", "R2P0E433PLG6")
        die_id = "_".join(attrs[3:5])
        temperature = attrs[-4]

        deembed_id = "_".join([wafer_id, die_id, attrs[0]])
        doe_column = attrs[0].split("_")[-1]
        if doe_column.startswith("C"):
            doe_column = 0

        # Split the measurement file into the individual sweeps
        df = pd.read_csv(
            filename,
            sep=",",
            skiprows=2,
            names=["input", "output", "wavelength_nm", "transmission_db"],
        )

        measurement = WDMSweepDeembed(
            deembed_id=deembed_id,
            doe_column=doe_column,
            temperature=temperature,
            fiber_height_um=float(header["metaData"]["Fiber_height_um"]),
            input=wrap_io(df.input[0]),
            output=wrap_io(df.output[0]),
            wavelength_nm=df.wavelength_nm.tolist(),
            transmission_db=df.transmission_db.tolist(),
        )
        session.merge(measurement)
    session.commit()


if __name__ == "__main__":
    from sqlalchemy import create_engine

    from photonics_db import database_address

    directory = Path(
        "/Users/jrollinson/projects/PRB/PEGASUS2/IBB38132/R2P0E438PLF7/WDM"
    )
    engine = create_engine(database_address + "/john_dev")

    with Session(engine) as sess:
        create_sweep_deembed_table(sess, directory)

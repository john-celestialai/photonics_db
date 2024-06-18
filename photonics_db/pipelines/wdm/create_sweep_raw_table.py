import sys
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from photonics_db.tables.wdm import WDMMeasurements, WDMSweepRaw

wrap_io = lambda x: int((x - 1) % 3 + 1)


def create_sweep_raw_table(session: Session, directory: Path):
    files = list(directory.rglob("*EULER*.csv"))
    batch_size = 100

    for j in range(0, len(files), batch_size):
        print(f"Batching files {j}-{j+batch_size}")
        for filename in files[j : j + batch_size]:
            # Look up the measurement id based on (wafer_id, die_id, device_id)
            attrs = filename.stem.split("__")
            wafer_id = attrs[1].replace("IM4477232804", "R2P0E433PLG6")
            die_id = "_".join(attrs[3:5])
            device_id = attrs[0]
            temperature = attrs[-5]

            try:
                result = session.scalars(
                    sa.select(WDMMeasurements)
                    .where(WDMMeasurements.wafer_id == wafer_id)
                    .where(WDMMeasurements.die_id == die_id)
                    .where(WDMMeasurements.device_id == device_id)
                    .where(WDMMeasurements.temperature == temperature)
                ).one()
            except Exception as e:
                print(e)
                print(wafer_id, die_id, device_id)
                sys.exit(1)

            # Load the measurement sweeps file
            df = pd.read_csv(
                filename,
                sep=",",
                skiprows=1,
                header=0,
            )

            # Make all column names lowercase
            df.rename(str.lower, axis="columns", inplace=True)

            if "bias_voltage_v" in df.columns:
                grouping = ["bias_voltage_v", "current_ma", "input", "output"]
            else:
                grouping = ["input", "output"]

            # Split the measurement file into the individual sweeps
            for i, (name, subtable) in enumerate(df.groupby(grouping)):
                if "bias_voltage_v" in grouping:
                    voltage_v = subtable["bias_voltage_v"].tolist()[0]
                    current_ma = subtable["current_ma"].tolist()[0]
                else:
                    voltage_v, current_ma = None, None

                measurement = WDMSweepRaw(
                    measurement_id=result.measurement_id,
                    sweep_id=i,
                    input=wrap_io(subtable["input"].tolist()[0]),
                    output=wrap_io(subtable["output"].tolist()[0]),
                    voltage_v=voltage_v,
                    current_ma=current_ma,
                    wavelength_nm=subtable["wavelength_nm"].tolist(),
                    transmission_db=subtable["transmission_db"].tolist(),
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
        create_sweep_raw_table(sess, directory)

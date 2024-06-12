import math
import sys
import time

import matplotlib.pyplot as plt
import numpy as np
import sqlalchemy as sa
from sqlalchemy.orm import Session

from photonics_db.tables.wdm import (
    WDMDevices,
    WDMMeasurements,
    WDMSweepDeembed,
    WDMSweepMain,
    WDMSweepRaw,
)


# Connect to the database
def create_sweep_main_table(session: Session):

    # Determine the number of raw WDM sweeps to de-embed
    row_count = session.scalar(sa.select(sa.func.count()).select_from(WDMSweepRaw))

    # Batch parameters
    batch_size = 100
    offset = 0
    n_batches = math.ceil(row_count / batch_size)
    start_batch = offset // batch_size

    # Batch over the raw measurements and de-embed
    for k in range(start_batch, n_batches):

        # Fetch the current batch
        start = time.time()
        print(
            f"Batching rows {offset}-{offset+batch_size} ({k+1}/{n_batches}) ...",
            end=" ",
            flush=True,
        )
        result = session.scalars(
            sa.select(WDMSweepRaw).offset(offset).limit(batch_size)
        )
        offset += batch_size

        # Iterate over batch rows to de-embed each measurement
        print(f"Fetching de-embed entries ...", end=" ", flush=True)
        for raw_sweep in result:
            # Fetch measurement metadata
            meas = session.scalars(
                sa.select(WDMMeasurements).where(
                    WDMMeasurements.measurement_id == raw_sweep.measurement_id
                )
            ).one()

            # Fetch device metadata
            device = session.scalars(
                sa.select(WDMDevices).where(WDMDevices.device_id == meas.device_id)
            ).one()

            target_deembed_id = f"{meas.wafer_id}_{meas.die_id}_WDM_RR_GCDE_C{raw_sweep.input}_C{raw_sweep.output}_{device.doe_column}"

            # Fetch the corresponding de-embed measurement
            deembed_meas = session.scalars(
                sa.select(WDMSweepDeembed).where(
                    WDMSweepDeembed.deembed_id == target_deembed_id
                )
            ).one()

            # De-embed grating coupler from raw measurement
            # plt.plot(raw_sweep.transmission_db)
            # plt.plot(deembed_meas.transmission_db)
            # plt.show()
            transmission_db = np.array(raw_sweep.transmission_db) - np.array(
                deembed_meas.transmission_db
            )

            # Determine where the measurement is a thru port or a drop port
            # For drop port, we want (orientation=V, output=3) | (orientation=H, output=2)
            # For thru port, we want (orientation=V, output=2) | (orientation=H, output=3)
            if (device.orientation == "V" and raw_sweep.output == 3) or (
                device.orientation == "H" and raw_sweep.output == 2
            ):
                port_type = "drop"
            elif (device.orientation == "V" and raw_sweep.output == 2) or (
                device.orientation == "H" and raw_sweep.output == 3
            ):
                port_type = "thru"
            else:
                raise ValueError(
                    f"Unrecognized port type for measurement_id={raw_sweep.measurement_id}"
                )

            # Create the new main sweep entry from the de-embedded transmission
            new_entry = WDMSweepMain(
                measurement_id=raw_sweep.measurement_id,
                sweep_id=raw_sweep.sweep_id,
                deembed_id=deembed_meas.deembed_id,
                port_type=port_type,
                current_ma=None,
                voltage_v=None,
                wavelength_nm=raw_sweep.wavelength_nm,
                transmission_db=transmission_db,
            )

            session.merge(new_entry)

        print("Committing transactions ...", end=" ", flush=True)
        session.commit()
        print(f"Batch complete ({time.time() - start:0.1f})")
    print("Completed.")


if __name__ == "__main__":
    from sqlalchemy import create_engine

    from photonics_db import database_address

    engine = create_engine(database_address + "/john_dev")
    with Session(engine) as sess:
        create_sweep_main_table(sess)

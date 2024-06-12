from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from photonics_db import database_address
from photonics_db.pipelines.wdm import (
    create_devices_table,
    create_fit_table,
    create_measurements_table,
    create_sweep_deembed_table,
    create_sweep_main_table,
    create_sweep_raw_table,
    create_wafer_table,
)
from photonics_db.tables import Base
from photonics_db.tables.wdm import *

engine = create_engine(database_address + "/john_dev")
Base.metadata.drop_all(engine, checkfirst=True)
Base.metadata.create_all(engine, checkfirst=True)

print("Connecting to engine")
with Session(engine) as session:

    print("Uploading wafer data.")
    create_wafer_table(session)
    print()

    print("Uploading WDM device data.")
    create_devices_table(session)
    print()

    print("Uploading WDM measurement data.")
    create_measurements_table(session)
    print()

    print("Uploading WDM raw sweep data.")
    create_sweep_raw_table(session)
    print()

    print("Uploading WDM de-embed data.")
    create_sweep_deembed_table(session)
    print()

    print("De-embedding gratings for raw WDM sweeps.")
    create_sweep_main_table(session)
    print()

    print("Extracting fit data for WDM peaks.")
    create_fit_table(session)
    print()

    print("Database upload complete.")

from pathlib import Path

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
# Base.metadata.drop_all(engine, checkfirst=True)
# Base.metadata.create_all(engine, checkfirst=True)

directories = [
    Path("/Users/jrollinson/projects/PRB/PEGASUS2/IBB38132/R2P0E380PLC5/WDM"),
    Path("/Users/jrollinson/projects/PRB/PEGASUS2/IBB38132/R2P0E386PLG0/WDM"),
    Path("/Users/jrollinson/projects/PRB/PEGASUS2/IBB38132/R2P0E424PLG4/WDM"),
    Path("/Users/jrollinson/projects/PRB/PEGASUS2/IBB38132/R2P0E433PLG6/WDM"),
    Path("/Users/jrollinson/projects/PRB/PEGASUS2/IBB38132/R2P0E438PLF7/WDM"),
    Path("/Users/jrollinson/projects/PRB/PEGASUS2/IBB38132/R2P0E444PLF0/WDM"),
]

print("Connecting to engine")
with Session(engine) as session:

    print("Uploading wafer data.")
    create_wafer_table(session)

    print("Uploading WDM device data.")
    create_devices_table(session)

    for directory in directories:
        print(directory)
        print("Uploading WDM measurement data.")
        create_measurements_table(session, directory)

        print("Uploading WDM raw sweep data.")
        create_sweep_raw_table(session, directory)

        print("Uploading WDM de-embed data.")
        create_sweep_deembed_table(session, directory)

        print("De-embedding gratings for raw WDM sweeps.")
        create_sweep_main_table(session)

        print("Extracting fit data for WDM peaks.")
        create_fit_table(session)

    print("Database upload complete.")

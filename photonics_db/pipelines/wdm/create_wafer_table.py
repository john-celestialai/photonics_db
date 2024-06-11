from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from photonics_db.tables.wdm import WaferMetadata


def create_wafer_table(session: Session):

    wafer_metadata_file = Path(__file__).parent / Path("wafer_metadata.csv")
    wafer_metadata = pd.read_csv(wafer_metadata_file, sep=",", header=0)
    wafer_metadata.dropna(subset=["wafer_id"], inplace=True)
    wafers = wafer_metadata.replace({np.nan: None}).to_dict("records")

    session.execute(insert(WaferMetadata).on_conflict_do_nothing(), wafers)
    session.commit()


if __name__ == "__main__":
    from sqlalchemy import create_engine

    from photonics_db import database_address

    engine = create_engine(database_address + "/john_dev")
    with Session(engine) as sess:
        create_wafer_table(sess)

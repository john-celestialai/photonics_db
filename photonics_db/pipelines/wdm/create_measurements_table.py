import json
from datetime import datetime
from pathlib import Path

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from photonics_db.tables.wdm import WDMMeasurements


def create_measurements_table(session: Session):
    directory = Path("/Users/jrollinson/projects/wdm_ap_data_upload/WDM")
    rows = []
    for filename in directory.rglob("*EULER*.csv"):
        with open(filename, "r") as f:
            line = str(f.readline()).replace(",}", "}")
            header = json.loads(line)

        attrs = list(filter(None, filename.stem.split("_")))
        attrs_aux = filename.stem.split("__")

        test_name = "_".join(filename.parent.parts[-1].split("_")[:3])
        measurement_datetime = datetime.strptime("_".join(attrs[-2:]), "%y%m%d_%H%M%S")

        device_id = attrs_aux[0]
        if "GCDE" in device_id:
            device_id = None

        measurement = {
            "tapeout": "PEGASUS2",
            "wafer_id": attrs_aux[1].replace("IM4477232804", "R2P0E433PLG6"),
            "run_id": filename.parts[-2].split("__")[-1],
            "pdk_element": attrs[0],
            "test_sequence": "_".join(attrs[:2]),
            "device_id": device_id,
            "die_id": "_".join(attrs[-4:-2]),
            "row": attrs[-4],
            "column": attrs[-3],
            "temperature": attrs[-5],
            "test_input_file_name": test_name,
            "run_name": filename.parent.parts[-1],
            "measurement_date": measurement_datetime.date(),
            "measurement_time": measurement_datetime.time(),
            "fiber_height_um": float(header["metaData"]["Fiber_height_um"]),
        }

        rows.append(measurement)

    session.execute(insert(WDMMeasurements).on_conflict_do_nothing(), rows)
    session.commit()

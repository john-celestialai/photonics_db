import datetime
from typing import List

import numpy as np
from sqlalchemy import ARRAY, Float, ForeignKey, ForeignKeyConstraint, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from photonics_db.tables.base import Base


class WaferMetadata(Base):
    __tablename__ = "wafers"

    wafer_id: Mapped[str] = mapped_column(primary_key=True)
    imec_id: Mapped[str]
    sfab_id: Mapped[str]
    wafer_vid: Mapped[str]
    process_pull: Mapped[str]
    gesi_epi: Mapped[str | None]
    oxide_dep: Mapped[str | None]
    status: Mapped[str | None]
    notes: Mapped[str | None]

    measurements: Mapped[List["WDMMeasurements"]] = relationship(back_populates="wafer")


class WDMDevices(Base):
    __tablename__ = "wdm_devices"

    device_id: Mapped[str] = mapped_column(primary_key=True)
    orientation: Mapped[str]
    bend_type: Mapped[str]
    coupler_length_um: Mapped[float]
    mh_linewidth_nm: Mapped[int | None]
    ucut_config: Mapped[str | None]
    doe_row: Mapped[int]
    doe_column: Mapped[int]
    ci_type: Mapped[str | None]
    ci_separation_um: Mapped[float | None]

    measurements: Mapped[List["WDMMeasurements"]] = relationship(
        back_populates="device"
    )


class WDMMeasurements(Base):
    __tablename__ = "wdm_measurements"

    measurement_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    sweeps: Mapped[List["WDMSweepRaw"]] = relationship(back_populates="measurement")

    wafer_id: Mapped[str] = mapped_column(ForeignKey("PEGASUS2.wafers.wafer_id"))
    wafer: Mapped["WaferMetadata"] = relationship(back_populates="measurements")

    die_id: Mapped[str]

    device_id: Mapped[str | None] = mapped_column(
        ForeignKey("PEGASUS2.wdm_devices.device_id"),
    )
    device: Mapped["WDMDevices"] = relationship(back_populates="measurements")

    pdk_element: Mapped[str]
    test_sequence: Mapped[str]
    row: Mapped[int]
    column: Mapped[int]
    temperature: Mapped[int]
    test_input_file_name: Mapped[str]
    run_name: Mapped[str]
    measurement_date: Mapped[datetime.date]
    measurement_time: Mapped[datetime.time]
    fiber_height_um: Mapped[float | None]


class WDMSweepRaw(Base):
    __tablename__ = "wdm_sweep_raw"

    measurement_id: Mapped[int] = mapped_column(
        ForeignKey("PEGASUS2.wdm_measurements.measurement_id"),
        primary_key=True,
    )
    measurement: Mapped["WDMMeasurements"] = relationship(back_populates="sweeps")

    sweep_id: Mapped[int] = mapped_column(primary_key=True)
    input: Mapped[int]
    output: Mapped[int]
    voltage_V: Mapped[float | None]
    current_mA: Mapped[float | None]
    wavelength_nm: Mapped[np.ndarray] = mapped_column(ARRAY(Float))
    transmission_db: Mapped[np.ndarray] = mapped_column(ARRAY(Float))


class WDMSweepDeembed(Base):
    __tablename__ = "wdm_sweep_deembed"

    # measurement_id: Mapped[int] = mapped_column(
    #     ForeignKey("PEGASUS2.wdm_measurements.measurement_id")
    # )
    deembed_id: Mapped[str] = mapped_column(primary_key=True)
    input: Mapped[int]
    output: Mapped[int]
    wavelength_nm: Mapped[np.ndarray] = mapped_column(ARRAY(Float))
    transmission_db: Mapped[np.ndarray] = mapped_column(ARRAY(Float))


class WDMSweepMain(Base):
    __tablename__ = "wdm_sweep_main"

    measurement_id: Mapped[int] = mapped_column(primary_key=True)
    sweep_id: Mapped[int] = mapped_column(primary_key=True)
    deembed_id: Mapped[str] = mapped_column(
        ForeignKey("PEGASUS2.wdm_sweep_deembed.deembed_id")
    )
    port_type: Mapped[str]
    voltage_v: Mapped[float | None]
    current_ma: Mapped[float | None]
    wavelength_nm: Mapped[np.ndarray] = mapped_column(ARRAY(Float))
    transmission_db: Mapped[np.ndarray] = mapped_column(ARRAY(Float))

    # For composite foreign keys, we need to add the foreign key constraint to
    # the table arguments
    __table_args__ = (
        ForeignKeyConstraint(
            [measurement_id, sweep_id],
            [WDMSweepRaw.measurement_id, WDMSweepRaw.sweep_id],
        ),
        Base.__table_args__,
    )


class WDMFitData(Base):
    __tablename__ = "wdm_fit"

    measurement_id: Mapped[int] = mapped_column(primary_key=True)
    sweep_id: Mapped[int] = mapped_column(primary_key=True)
    resonance_id: Mapped[int] = mapped_column(primary_key=True)
    peak_wavelength_nm: Mapped[float]
    fsr_nm: Mapped[float | None]
    fwhm_nm: Mapped[float]
    bw_1db_nm: Mapped[float]
    crosstalk_db: Mapped[float]
    insertion_loss_db: Mapped[float]
    fit_params: Mapped[np.ndarray] = mapped_column(ARRAY(Float, dimensions=1))
    fit_covars: Mapped[np.ndarray] = mapped_column(ARRAY(Float, dimensions=1))
    fit_rsquared: Mapped[float]

    __table_args__ = (
        ForeignKeyConstraint(
            [measurement_id, sweep_id],
            [WDMSweepMain.measurement_id, WDMSweepMain.sweep_id],
        ),
        Base.__table_args__,
    )

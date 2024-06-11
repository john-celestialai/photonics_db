"""
Run data reduction (aka FOM extraction) for WDM measurements

Data reduction procedure:
1. Optionally smooth raw transmission data
2. De-embed (i.e., flatten) drop- and pass-port data by subtracting the 
   appropriate raw WDM_GCDE spectra from raw RR drop- and pass-port transmission
3. Convert de-embedded drop-port transmission spectrum to linear scale and fit
   each resonance peak to a Lorentzian (note this example data is using a 100pm 
   resolution which is may be rough for this type of structure, and in practice 
   we may end up using 10pm resolution)
4. Use the extracted Lorentzian fit parameters vs. resonance wavelengths to
   obtain functions of the various FOMs vs. wavelength for each device, and then
   evaluate these at the specified wavelength/s of interest.
5. FSR: we don't bother converting to frequency and instead just report the
   red-looking FSR at each resonance wavelength (i.e., the FSR associated w/ the
   nth resonance is given by λres, n+1 — Ares, n).
6. FWHM: corresponds to 2x the Growth Rate (i.e., parameter b) from the
   Lorentzian fit.
7. Drop port IL: Obtained from the Peak Value (i.e., parameter a) from the
   Lorentzian fit through IL = -10*log10(a).
8. 1dB bandwidth and cross-talk: can be calculated from the Lorentzian fit 
   parameters. Cross talk measured at lambda_resonant + 2.5nm
"""

import math
import time

import numpy as np
import sqlalchemy as sa
from scipy import optimize, signal
from sqlalchemy.orm import Session

from photonics_db.tables.wdm import *

target_wavelength_nm = 1580
target_bandwidth_1db_nm = 0.374
target_crosstalk_offset_nm = 2.5
target_fsr_nm = 12.8


def lorentzian(x, x0, alpha, gamma):
    numer = alpha * gamma
    denom = np.square(x - x0) + np.square(gamma)
    return numer / denom


def extract_peaks(transmission_db: np.ndarray) -> np.ndarray:
    """Extract the peak indices of the ring resonator spectrum."""
    transmission_w = 10 ** (np.array(transmission_db) / 10)
    peaks, _ = signal.find_peaks(transmission_w, prominence=0.5)

    return peaks


def extract_fsr(wavelength_nm: np.ndarray, peaks: list[int]) -> np.ndarray:
    """Extract the free spectral range (FSR) between the peaks.

    The FSR for each resonance peak is compute between the peak and the next
    red-looking peak, i.e. FSR = lambda_n+1 - lambda_n
    """
    peak_wlen = np.array(wavelength_nm)[peaks]
    fsr_nm = np.roll(peak_wlen, -1)[:-1] - peak_wlen[:-1]
    fsr_nm = np.append(fsr_nm, np.nan)

    return fsr_nm


def extract_lorentzian_fit(
    wavelength_nm: np.ndarray, transmission_db: np.ndarray, peak: int, peaks: np.ndarray
) -> tuple[np.ndarray, np.ndarray, float]:
    """Fit a (un-normalized) Lorentzian to the drop port of a ring resonator.

    Lorentzian:
        L(lambda) = alpha * gamma^2 / ((lambda - lambda_0)^2 + gamma^2)
    """
    fsr_idx = np.roll(peaks, -1)[:-1] - peaks[:-1]
    transmission_w = 10 ** (np.array(transmission_db) / 10)

    # Slice out the peak of interest for fitting
    start_idx = max(peak - fsr_idx // 2, 0)
    stop_idx = min(peak + fsr_idx // 2, wavelength_nm.size)
    wlen_fit = wavelength_nm[start_idx:stop_idx]
    trans_fit = transmission_w[start_idx:stop_idx]

    popt, pcov = optimize.curve_fit(
        lorentzian,
        wlen_fit,
        trans_fit,
        p0=(wavelength_nm[peak], -1, 0.5, 1),
        sigma=1e-12,
    )

    # Compute r-square for Lorentzian fit
    residuals = trans_fit - lorentzian(wlen_fit, *popt)
    ss_res = np.sum(np.square(residuals))
    ss_tot = np.sum(np.square(trans_fit - np.mean(trans_fit)))
    r_squared = 1 - ss_res / ss_tot

    return popt, pcov, r_squared


def extract_fwhm(fit_params: list[float]) -> float:
    """Extract the full width at half-maximum (FWHM) of the fit.

    FWHM = 2*gamma
    """
    _, _, gamma = fit_params
    return 2 * gamma


def extract_1db_bandwidth(fit_params) -> float:
    """Extract the 1dB bandwidth of the fit.

    BW_1dB -> Lorentz(lambda) = 10^(-1/10)

    BW_1dB = sqrt(alpha*gamma^2/10^(-1/10) - gamma^2)
    """
    _, alpha, gamma = fit_params
    return np.sqrt(alpha * gamma**2 * 10 ** (1 / 10) - gamma**2)


def extract_crosstalk(fit_params) -> float:
    """Extract the crosstalk at lambda_0 + 2.5nm

    XT_2.5nm_dB = 10log(alpha*gamma^2/(2.5nm + gamma^2))
    """
    _, alpha, gamma = fit_params
    return 10 * np.log10(alpha * gamma**2 / (2.5 + gamma**2))


def extract_insertion_loss(fit_params) -> float:
    """Extract the drop port insertion loss.

    IL_dB = -10log(alpha)
    """
    _, alpha, _ = fit_params
    return -10 * np.log10(alpha)


def create_fit_table(session: Session):

    # Determine the number of WDM measurements
    row_count = session.scalar(
        sa.select(sa.func.count())
        .select_from(WDMSweepMain)
        .where(WDMSweepMain.port_type == "drop")
    )

    # Batch parameters
    batch_size = 100
    n_batches = math.ceil(row_count / batch_size)
    offset = 0
    start_batch = offset // batch_size

    for k in range(start_batch, n_batches):

        start = time.time()
        print(
            f"Batching rows {offset}-{offset+batch_size} ({k+1}/{n_batches}) ...",
            end=" ",
            flush=True,
        )

        # Fetch a batch of WDM_RR measurements
        wdm_rr_results = session.scalars(
            sa.select(WDMSweepMain).where().offset(offset).limit(batch_size)
        ).all()
        offset += batch_size

        for result in wdm_rr_results:
            peaks = extract_peaks(result.wavelength_nm)
            peaks_fsr_nm = extract_fsr(result.wavelength_nm, peaks)
            for i, (peak, fsr_nm) in enumerate(zip(peaks, peaks_fsr_nm)):
                popt, pcov, rsquared = extract_lorentzian_fit(
                    result.wavelength_nm, result.transmission_db, peak, peaks
                )

                peak_wavelength_nm = result.wavelength_nm[peak]
                fwhm_nm = extract_fwhm(popt)
                bw_1db_nm = extract_1db_bandwidth(popt)
                crosstalk_db = extract_crosstalk(popt)
                insertion_loss_db = extract_insertion_loss(popt)

                fit_data = WDMFitData(
                    measurement_id=result.measurement_id,
                    sweep_id=result.sweep_id,
                    resonance_id=i,
                    peak_wavelength_nm=peak_wavelength_nm,
                    fsr_nm=fsr_nm,
                    fwhm_nm=fwhm_nm,
                    bw_1db_nm=bw_1db_nm,
                    crosstalk_db=crosstalk_db,
                    insertion_loss_db=insertion_loss_db,
                    fit_params=popt,
                    fit_covars=pcov,
                    fit_rsquared=rsquared,
                )
                session.merge(fit_data)

        print("Committing transactions ...", end=" ", flush=True)
        session.commit()
        print(f"Batch complete ({time.time() - start:0.1f}s).")
    print("Completed.")


if __name__ == "__main__":
    from sqlalchemy import create_engine

    from photonics_db import database_address

    engine = create_engine(database_address + "/john_dev")
    with Session(engine) as sess:
        create_fit_table(sess)

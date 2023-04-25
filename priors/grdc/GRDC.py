# Standard imports
from datetime import datetime
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

class GRDC:
    """ Stores GRDC data in the SoS.

    Requires that WBM SoS version exists in 'constrained' directory.

    Attributes
    ----------
    FLOAT_FILL: float
        Fill value for any missing float values in mapped GRDC data
    INT_FILL: int
        Fill value for any missing integer values in mapped GRDC data
    map_dict: dict
        Dict organized by continent with reach_id and GRDC discharge data
    sos_reaches: numpy.ndarray
        array of integer reach identifiers retrieved from the SoS
    sos_file: Path
        Path to SoS NetCDF file
    grdc_dict: dict
        Dict of GRDC data
    grdc_file: Path
        Path to GRDC NetCDF file

    Methods
    -------
    map_data()
        Maps GRDC data to SoS data organized by continent.
    read_sos()
        Reads in the SoS data and stores it in a dict organized by continent.
    read_grdc()
        Reads in the GRDC data and stores it in a dict organized by data name.
    update_data()
        Updates GRDC data in the SoS.
    """

    FLOAT_FILL = -999999999999
    INT_FILL = -999

    def __init__(self, sos_file, grdc_file):
        """
        Parameters
        ----------
        sos_file: Path
            Path to SoS NetCDF file
        grdc_file: Path
            Path to GRDC NetCDF file
        """

        self.map_dict = {}
        self.sos_reaches = None
        self.sos_file = sos_file
        self.grdc_dict = {}
        self.grdc_file = grdc_file

    def map_data(self):
        """Maps GRDC data to SoS and stores it in map_dict attribute."""

        # Reach identifiers
        grdc_ids = self.grdc_dict["reach_id"]
        same_ids = np.intersect1d(self.sos_reaches, grdc_ids)
        indexes = np.where(np.isin(grdc_ids, same_ids))[0]

        # Map GRDC data that matches SoS reach identifiers
        self.map_dict["days"] = np.array(range(1, self.grdc_dict["dt"] + 1))
        self.map_dict["grdc_reach_id"] = self.grdc_dict["reach_id"][indexes]
        self.map_dict["fdq"] = self.grdc_dict["fdq"][:,indexes]
        self.map_dict["max_q"] = self.grdc_dict["max_q"][indexes]
        self.map_dict["monthly_q"] = self.grdc_dict["monthly_q"][:,indexes]
        self.map_dict["mean_q"] = self.grdc_dict["mean_q"][indexes]
        self.map_dict["min_q"] = self.grdc_dict["min_q"][indexes]
        self.map_dict["tyr"] = self.grdc_dict["tyr"][indexes]
        self.map_dict["grdc_id"] = self.grdc_dict["grdc_id"][indexes]
        self.map_dict["grdc_q"] = self.grdc_dict["grdc_q"][:,indexes]
        self.map_dict["grdc_qt"] = self.grdc_dict["grdc_qt"][:,indexes]

    def read_grdc(self):
        """Reads in data from GRDC and stores in grdc_dict attribute."""

        grdc = Dataset(self.grdc_file)
        self.grdc_dict["reach_id"] = grdc["Reach_ID"][:].filled(np.nan).astype(int)
        self.grdc_dict["fdq"] = grdc["Flow_DurationQ"][:].filled(np.nan)
        self.grdc_dict["max_q"] = grdc["MaxQ"][:].filled(np.nan)
        self.grdc_dict["monthly_q"] = grdc["MonthlyQ"][:].filled(np.nan)
        self.grdc_dict["mean_q"] = grdc["MeanQ"][:].filled(np.nan)
        self.grdc_dict["min_q"] = grdc["MinQ"][:].filled(np.nan)
        self.grdc_dict["tyr"] = grdc["Two_Year_Return"][:].filled(np.nan)
        self.grdc_dict["grdc_id"] = grdc["GRDC_id"][:].filled(np.nan).astype(int)
        self.grdc_dict["grdc_q"] = grdc["GRDC_Q"][:].filled(np.nan)
        self.grdc_dict["grdc_qt"] = grdc["GRDC_Qt"][:].filled(np.nan)
        self.grdc_dict["dt"] = grdc.dimensions["Time(days)"].size
        grdc.close()

    def read_sos(self):
        """Reads in data from the SoS and stores in sos_reaches attribute."""

        sos = Dataset(self.sos_file)
        self.sos_reaches = sos["reaches"]["reach_id"][:].filled(np.nan).astype(int)
        sos.close()

    def update_data(self):
        """Updates GRDC data in the SoS.
        
        Data is stored in a group labelled model as it will accommodate GRDC for 
        constrained runs.

        Requires: SoS created with GRDC group present.
        """
        
        sos = Dataset(self.sos_file, 'a')
        sos.production_date = datetime.now().strftime('%d-%b-%Y %H:%M:%S')
        grdc = sos["historicQ"]["grdc"]

        grdc["num_days"][:] = self.map_dict["days"]
        grdc["num_grdc_reaches"][:] = range(1, self.map_dict["grdc_reach_id"].shape[0] + 1)
        grdc["grdc_reach_id"][:] = self.map_dict["grdc_reach_id"]
        grdc["grdc_flow_duration_q"][:] = np.transpose(np.nan_to_num(self.map_dict["fdq"], copy=True, nan=self.FLOAT_FILL))
        grdc["grdc_max_q"][:] = np.nan_to_num(self.map_dict["max_q"], copy=True, nan=self.FLOAT_FILL)
        grdc["grdc_monthly_q"][:] = np.transpose(np.nan_to_num(self.map_dict["monthly_q"], copy=True, nan=self.FLOAT_FILL))
        grdc["grdc_mean_q"][:] = np.nan_to_num(self.map_dict["mean_q"], copy=True, nan=self.FLOAT_FILL)
        grdc["grdc_min_q"][:] = np.nan_to_num(self.map_dict["min_q"], copy=True, nan=self.FLOAT_FILL)
        grdc["grdc_two_year_return_q"][:] = np.nan_to_num(self.map_dict["tyr"], copy=True, nan=self.FLOAT_FILL)
        grdc["grdc_id"][:] = np.nan_to_num(self.map_dict["grdc_id"], copy=True, nan=self.INT_FILL)
        grdc["grdc_q"][:] = np.transpose(np.nan_to_num(self.map_dict["grdc_q"], copy=True, nan=self.FLOAT_FILL))
        grdc["grdc_qt"][:] = np.transpose(np.nan_to_num(self.map_dict["grdc_qt"], copy=True, nan=self.FLOAT_FILL))

        sos.close()


if __name__ == "__main__":
    from datetime import datetime

    SOS_DIR = Path("")
    GRDC_FILE = Path("")

    start = datetime.now()
    grdc = GRDC(SOS_DIR, GRDC_FILE)
    grdc.read_sos()
    grdc.read_grdc()
    grdc.map_data()
    grdc.append_data()
    end = datetime.now()
    print(f"Execution time: {end - start}")
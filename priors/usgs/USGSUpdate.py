# Standard imports
from datetime import datetime
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset, stringtochar
import numpy as np

class USGSUpdate:
    """Class that updates USGS gage data in the SoS.
    
    Attributes
    ----------
    FLOAT_FILL: float
        Fill value for any missing float values in mapped GRDC data
    INT_FILL: int
        Fill value for any missing integer values in mapped GRDC data
    map_dict: dict
        Dict organized by continent with reach_id and GRADES discharge data
    sos_dict: dict
        Dict organized by continent reach_ids and SoS file name data
    sos_file: Path
        Path to SoS NetCDF file
    temp_sos: TemporaryDirectory
        Temporary directory that holds old SoS version
    usgs_dict: dict
        Dictionary of USGS data

    Methods
    -------
    map_data()
        Maps USGS data to SoS data organized by continent
    read_sos()
        Reads in the SoS data and stores it in a dict organized by continent
    update_data()
        Updates data in the SoS
    """

    FLOAT_FILL = -999999999999
    INT_FILL = -999

    def __init__(self, sos_file, usgs_dict):
        """
        Parameters
        ----------
        sos_file: Path
            Path to SoS NetCDF file
        temp_sos: TemporaryDirectory
            Temporary directory that holds old SoS version
        usgs_dict: dict
            Dictionary of USGS data
        """

        self.sos_file = sos_file
        self.temp_sos = None
        self.usgs_dict = usgs_dict
        self.map_dict = {}
        self.sos_reaches = None            

    def map_data(self):
        """Maps USGS data to SoS, and stores data in map_dict attribute."""

        # Reach identifiers
        usgs_ids = self.usgs_dict["reachId"]
        same_ids = np.intersect1d(self.sos_reaches, usgs_ids)
        indexes = np.where(np.isin(usgs_ids, same_ids))[0]

        if indexes.size == 0:
            self.map_dict = None
        else:
            # Map USGS data that matches SoS reach identifiers
            self.map_dict["days"] = np.array(range(1, len(self.usgs_dict["Qwrite"][0]) + 1))
            self.map_dict["usgs_reach_id"] = self.usgs_dict["reachId"].astype(np.int64)[indexes]
            self.map_dict["fdq"] = self.usgs_dict["FDQS"][indexes,:]
            self.map_dict["max_q"] =self.usgs_dict["Qmax"][indexes]
            self.map_dict["monthly_q"] = self.usgs_dict["MONQ"][indexes,:]
            self.map_dict["mean_q"] = self.usgs_dict["Qmean"][indexes]
            self.map_dict["min_q"] = self.usgs_dict["Qmin"][indexes]
            self.map_dict["tyr"] = self.usgs_dict["TwoYr"][indexes]
            self.map_dict["usgs_id"] = np.array(self.usgs_dict["dataUSGS"])[indexes]
            self.map_dict["usgs_q"] = self.usgs_dict["Qwrite"][indexes,:]
            self.map_dict["usgs_qt"] = self.usgs_dict["Twrite"][indexes,:]
    
    def read_sos(self):
        """Reads in data from the SoS and stores in sos_reaches attribute."""

        sos = Dataset(Path(self.sos_file))
        self.sos_reaches = sos["reaches"]["reach_id"][:].filled(np.nan).astype(int)
        sos.close()

    def update_data(self):
        """Updates USGS data in the SoS.
        
        Data is stored in a group labelled model as it will accommodate USGS for 
        constrained runs.

        Requires: SoS created with USGS group present.
        """

        if self.map_dict:
            sos = Dataset(self.sos_file, 'a')
            sos.production_date = datetime.now().strftime('%d-%b-%Y %H:%M:%S')

            usgs = sos["usgs"]

            # by this point sel.map_dict["days"] should  have all the historical data as well as the new year
            usgs["num_days"][:] = self.map_dict["days"]

            # this variable is not in the SOS
            # usgs["num_usgs_reaches"][:] = range(1, self.map_dict["usgs_reach_id"].shape[0] + 1)

            # usgs_reach_id needs to change?
            usgs["usgs_reach_id"][:] = self.map_dict["usgs_reach_id"]
            usgs["flow_duration_q"][:] = np.nan_to_num(self.map_dict["fdq"], copy=True, nan=self.FLOAT_FILL)
            usgs["max_q"][:] = np.nan_to_num(self.map_dict["max_q"], copy=True, nan=self.FLOAT_FILL)
            usgs["monthly_q"][:] = np.nan_to_num(self.map_dict["monthly_q"], copy=True, nan=self.FLOAT_FILL)
            usgs["mean_q"][:] = np.nan_to_num(self.map_dict["mean_q"], copy=True, nan=self.FLOAT_FILL)
            usgs["min_q"][:] = np.nan_to_num(self.map_dict["min_q"], copy=True, nan=self.FLOAT_FILL)
            usgs["two_year_return_q"][:] = np.nan_to_num(self.map_dict["tyr"], copy=True, nan=self.FLOAT_FILL)
            usgs["usgs_id"][:] = stringtochar(self.map_dict["usgs_id"].astype("S16"))
            usgs["usgs_q"][:] = np.nan_to_num(self.map_dict["usgs_q"], copy=True, nan=self.FLOAT_FILL)
            usgs["usgs_qt"][:] = np.nan_to_num(self.map_dict["usgs_qt"], copy=True, nan=self.FLOAT_FILL)
                
            sos.close()
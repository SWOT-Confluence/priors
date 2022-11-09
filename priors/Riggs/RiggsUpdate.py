# Standard imports
from datetime import datetime
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset, stringtochar
import numpy as np

class RiggsUpdate:
    """Class that updates Riggs gage data in the SoS.
    
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
    Riggs_dict: dict
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

    def __init__(self, sos_file, Riggs_dict):
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
        self.Riggs_dict = Riggs_dict
        self.map_dict = {}
        self.sos_reaches = None            

    def map_data(self):
        """Maps USGS data to SoS, and stores data in map_dict attribute."""

        # Reach identifiers
        Riggs_ids = self.Riggs_dict["reachId"]
        same_ids = np.intersect1d(self.sos_reaches, Riggs_ids)
        indexes = np.where(np.isin(Riggs_ids, same_ids))[0]

        if indexes.size == 0:
            self.map_dict = None
        else:
            # Map USGS data that matches SoS reach identifiers
            self.map_dict["days"] = np.array(range(1, len(self.Riggs_dict["Qwrite"][0]) + 1))
            self.map_dict["Riggs_reach_id"] = self.Riggs_dict["reachId"].astype(np.int64)[indexes]
            self.map_dict["fdq"] = self.Riggs_dict["FDQS"][indexes,:]
            self.map_dict["max_q"] =self.Riggs_dict["Qmax"][indexes]
            self.map_dict["monthly_q"] = self.Riggs_dict["MONQ"][indexes,:]
            self.map_dict["mean_q"] = self.Riggs_dict["Qmean"][indexes]
            self.map_dict["min_q"] = self.Riggs_dict["Qmin"][indexes]
            self.map_dict["tyr"] = self.Riggs_dict["TwoYr"][indexes]
            self.map_dict["Riggs_id"] = np.array(self.Riggs_dict["dataRiggs"])[indexes]
            self.map_dict["Riggs_q"] = self.Riggs_dict["Qwrite"][indexes,:]
            self.map_dict["Riggs_qt"] = self.Riggs_dict["Twrite"][indexes,:]
    
    def read_sos(self):
        """Reads in data from the SoS and stores in sos_reaches attribute."""

        sos = Dataset(Path(self.sos_file))
        self.sos_reaches = sos["reaches"]["reach_id"][:].filled(np.nan).astype(int)
        sos.close()

    def update_data(self):
        """Updates Riggs data in the SoS.
        
        Data is stored in a group labelled model as it will accommodate Riggs for 
        constrained runs.

        Requires: SoS created with Riggs group present.
        """

        if self.map_dict:
            sos = Dataset(self.sos_file, 'a')
            sos.production_date = datetime.now().strftime('%d-%b-%Y %H:%M:%S')

            Riggs = sos["model"]["Riggs"]
            Riggs["num_days"][:] = self.map_dict["days"]
            Riggs["num_Riggs_reaches"][:] = range(1, self.map_dict["Riggs_reach_id"].shape[0] + 1)
            Riggs["Riggs_reach_id"][:] = self.map_dict["Riggs_reach_id"]
            Riggs["flow_duration_q"][:] = np.nan_to_num(self.map_dict["fdq"], copy=True, nan=self.FLOAT_FILL)
            Riggs["max_q"][:] = np.nan_to_num(self.map_dict["max_q"], copy=True, nan=self.FLOAT_FILL)
            Riggs["monthly_q"][:] = np.nan_to_num(self.map_dict["monthly_q"], copy=True, nan=self.FLOAT_FILL)
            Riggs["mean_q"][:] = np.nan_to_num(self.map_dict["mean_q"], copy=True, nan=self.FLOAT_FILL)
            Riggs["min_q"][:] = np.nan_to_num(self.map_dict["min_q"], copy=True, nan=self.FLOAT_FILL)
            Riggs["two_year_return_q"][:] = np.nan_to_num(self.map_dict["tyr"], copy=True, nan=self.FLOAT_FILL)
            Riggs["Riggs_id"][:] = stringtochar(self.map_dict["Riggs_id"].astype("S16"))
            Riggs["Riggs_q"][:] = np.nan_to_num(self.map_dict["Riggs_q"], copy=True, nan=self.FLOAT_FILL)
            Riggs["Riggs_qt"][:] = np.nan_to_num(self.map_dict["Riggs_qt"], copy=True, nan=self.FLOAT_FILL)
                
            sos.close()
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

    def __init__(self, sos_file, usgs_dict, metadata_json):
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
        self.variable_atts = metadata_json["USGS"]

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
            self.map_dict["usgs_reach_id"] = np.array(self.usgs_dict["reachId"]).astype(np.int64)[indexes]
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
        # was borking here
        # self.sos_reaches = sos["reaches"]["reach_id"][:].filled(np.nan).astype(int)
        self.sos_reaches = sos["reaches"]["reach_id"][:].astype(int)
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

            usgs = sos["USGS"]
            
            usgs["num_days"][:] = self.map_dict["days"]     
            self.set_variable_atts(usgs["num_days"], self.variable_atts["num_days"])

            # this variable is not in the SOS
            # usgs["num_usgs_reaches"][:] = range(1, self.map_dict["usgs_reach_id"].shape[0] + 1)

            # print(self.map_dict["usgs_reach_id"])
            # print(usgs["usgs_reach_id"][:])
            # print(self.map_dict["usgs_reach_id"].shape)
            # print(usgs["usgs_reach_id"][:].shape)

            usgs["USGS_reach_id"][:] = self.map_dict["usgs_reach_id"]
            self.set_variable_atts(usgs["USGS_reach_id"], self.variable_atts["USGS_reach_id"])
            
            usgs["USGS_flow_duration_q"][:] = np.nan_to_num(self.map_dict["fdq"], copy=True, nan=self.FLOAT_FILL)
            self.set_variable_atts(usgs["USGS_flow_duration_q"], self.variable_atts["USGS_flow_duration_q"])
            
            usgs["USGS_max_q"][:] = np.nan_to_num(self.map_dict["max_q"], copy=True, nan=self.FLOAT_FILL)
            self.set_variable_atts(usgs["USGS_max_q"], self.variable_atts["USGS_max_q"])
            
            usgs["USGS_monthly_q"][:] = np.nan_to_num(self.map_dict["monthly_q"], copy=True, nan=self.FLOAT_FILL)
            self.set_variable_atts(usgs["USGS_monthly_q"], self.variable_atts["USGS_monthly_q"])
            
            usgs["USGS_mean_q"][:] = np.nan_to_num(self.map_dict["mean_q"], copy=True, nan=self.FLOAT_FILL)
            self.set_variable_atts(usgs["USGS_mean_q"], self.variable_atts["USGS_mean_q"])
            
            usgs["USGS_min_q"][:] = np.nan_to_num(self.map_dict["min_q"], copy=True, nan=self.FLOAT_FILL)
            self.set_variable_atts(usgs["USGS_min_q"], self.variable_atts["USGS_min_q"])
            
            usgs["USGS_two_year_return_q"][:] = np.nan_to_num(self.map_dict["tyr"], copy=True, nan=self.FLOAT_FILL)
            self.set_variable_atts(usgs["USGS_two_year_return_q"], self.variable_atts["USGS_two_year_return_q"])

            # print(self.map_dict["usgs_id"])
            # print(usgs["usgs_id"][:])
            # print(self.map_dict["usgs_id"].shape)
            # print(usgs["usgs_id"][:].shape)
            
            usgs["USGS_id"][:] = stringtochar(self.map_dict["usgs_id"].astype("S100"))
            self.set_variable_atts(usgs["USGS_id"], self.variable_atts["USGS_id"])            
            
            usgs["USGS_q"][:] = np.nan_to_num(self.map_dict["usgs_q"], copy=True, nan=self.FLOAT_FILL)
            self.set_variable_atts(usgs["USGS_q"], self.variable_atts["USGS_q"])
            
            usgs["USGS_qt"][:] = np.nan_to_num(self.map_dict["usgs_qt"], copy=True, nan=self.FLOAT_FILL)
            self.set_variable_atts(usgs["USGS_qt"], self.variable_atts["USGS_qt"])
                
            sos.close()
            
    def set_variable_atts(self, variable, variable_dict):
        """Set the variable attribute metdata."""
        
        for name, value in variable_dict.items():
            setattr(variable, name, value)
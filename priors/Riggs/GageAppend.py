# Standard imports
import glob
from os import scandir
from pathlib import Path
from shutil import move, rmtree
import tempfile

# Third-party imports
from netCDF4 import Dataset, stringtochar
import numpy as np

class GageAppendR:
    """Class that appends Riggs gage data to the SoS.
    
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
    sos_dir: Path
        Path to SoS directory
    usgs_dict: dict
        Dictionary of USGS data
    riggs_dict: dict
        Dictionary of riggs data

    Methods
    -------
    append()
        Appends data to the SoS
    __append_riggs_data(sos, continent)
        Append riggs data to the SoS file
    map_data()
        Maps riggs data to SoS data organized by continent.
    read_sos()
        Reads in the SoS data and stores it in a dict organized by continent.
    """

    FLOAT_FILL = -999999999999
    INT_FILL = -999

    def __init__(self, sos_dir, riggs_dict):
        """
        Parameters
        ----------
        sos_dir: Path
            Path to SoS directory
        temp_sos: TemporaryDirectory
            Temporary directory that holds old SoS version
        riggs_dict: dict
            Dictionary of riggs data
        """

        self.sos_dir = sos_dir
        self.riggs_dict = riggs_dict
        self.map_dict = { "af": {}, "as": {}, "eu": {}, "na": {},
            "oc": {}, "sa": {} }
        self.sos_dict = { "af": None, "as": None, "eu": None, "na": None,
            "oc": None, "sa": None }

    def append_data(self):
        """Appends data to the SoS.
        
        Data is copied from temporary SoS file and extracted Riggs data.
        """

        # Copy data from SoS files to new files and append =Riggs data
        for continent in self.sos_dict.keys():
            if self.map_dict[continent]:
                sos_file = glob.glob(f"{self.sos_dir}/{continent}*")[0]
                sos = Dataset(sos_file, 'a')
                self.__append_riggs_data(sos, continent)           
                sos.close()

    def __append_riggs_data(self, sos, continent):
        """Append riggs data to the SoS file.

        Data is stored in a group labelled Riggs nested under model.

        Parameters
        ----------
        sos: Dataset
            new NetCDF4 Dataset
        continent: str
            string abbreviation of continent data is for
        """

        # riggs data - NEW
        # name strind are going to incorperate agency
        Uagency=np.unique(self.riggs_dict['Agency'])
        
        ##debug
        #sos.createGroup("model")
        riggz = sos["model"].createGroup("riggs")

       
        for Tagency in Uagency:
            agency=Tagency
            riggs=riggz.createGroup(agency)
            
            riggs.createDimension("num_days", None)
            dt = riggs.createVariable("num_days", "i4", ("num_days", ))
            dt.units = "day"
            dt[:] = self.map_dict[continent]["days"]
            riggs.createDimension("probability", None)
            riggs.createDimension("nchars", 100)
            ai= np.where(np.array(self.riggs_dict['Agency'])==agency)
            
            try:
                riggs.createDimension("num_"+agency+"_reaches", len(ai[0]))
                #locals()["nr"+agency] = riggs.createVariable(agency+"_reaches", "i4", ("num_"+agency+"_reaches", ))
                
            
            finally:
                locals()["nr"+agency] = riggs.createVariable(agency+"_reaches", "i4", ("num_"+agency+"_reaches", ))
                locals()["nr"+agency].units = "reach"
                locals()["nr"+agency][:] = range(1, self.map_dict[continent]["riggs_reach_id"][ai].shape[0] + 1)
        
                locals()["riggs_reach_id"+agency] = riggs.createVariable(agency+"_reach_id", "i8", ("num_"+agency+"_reaches", ))
                locals()["riggs_reach_id"+agency].format = "CBBBBBRRRRT"
                locals()["riggs_reach_id"+agency][:] = self.map_dict[continent]["riggs_reach_id"][ai]
            
           
                locals()["fdq"+agency] = riggs.createVariable("flow_duration_q", "f8", ("num_"+agency+"_reaches", "probability"), fill_value=self.FLOAT_FILL)
                locals()["fdq"+agency].long_name = agency+" flow_Duration_curve_discharge"
                locals()["fdq"+agency].comment = agency+" discharge values from the flow duration curve for this reach"
                locals()["fdq"+agency].units = "m^3/s"
                locals()["fdq"+agency][:] = np.nan_to_num(self.map_dict[continent]["fdq"][ai], copy=True, nan=self.FLOAT_FILL)
        
                locals()["max_q"+agency] = riggs.createVariable("max_q", "f8", ("num_"+agency+"_reaches",), fill_value=self.FLOAT_FILL)
                locals()["max_q"+agency].long_name = agency+" maximum_discharge"
                locals()["max_q"+agency].comment = agency+"highest discharge value in this cell"
                locals()["max_q"+agency].units = "m^3/s"
                locals()["max_q"+agency][:] = np.nan_to_num(self.map_dict[continent]["max_q"][ai], copy=True, nan=self.FLOAT_FILL)
            
           
                
                locals()["monthly_q"+agency] = riggs.createVariable("monthly_q", "f8", ("num_"+agency+"_reaches", "num_months"), fill_value=self.FLOAT_FILL)
                locals()["monthly_q"+agency].long_name = agency+"mean_monthly_discharge"
                locals()["monthly_q"+agency].comment = agency+"monthly mean discharge time series in this cell"
                locals()["monthly_q"+agency].units = "m^3/s"
                locals()["monthly_q"+agency][:] = np.nan_to_num(self.map_dict[continent]["monthly_q"][ai], copy=True, nan=self.FLOAT_FILL)
        
                locals()["mean_q"+agency] = riggs.createVariable("mean_q", "f8", ("num_"+agency+"_reaches",), fill_value=self.FLOAT_FILL)
                locals()["mean_q"+agency].long_name = agency+"mean_discharge"
                locals()["mean_q"+agency].comment = agency+"mean discharge value in this cell"
                locals()["mean_q"+agency].units = "m^3/s"
                locals()["mean_q"+agency][:] = np.nan_to_num(self.map_dict[continent]["mean_q"][ai], copy=True, nan=self.FLOAT_FILL)
        
                locals()["min_q"+agency] = riggs.createVariable("min_q", "f8", ("num_"+agency+"_reaches",), fill_value=self.FLOAT_FILL)
                locals()["min_q"+agency] .long_name = agency+" minimum_discharge"
                locals()["min_q"+agency] .comment = agency+" lowest discharge value in this cell"
                locals()["min_q"+agency] .units = "m^3/s"
                locals()["min_q"+agency] [:] = np.nan_to_num(self.map_dict[continent]["min_q"][ai], copy=True, nan=self.FLOAT_FILL)
                
                locals()["tyr"+agency]  = riggs.createVariable(agency+"two_year_return_q", "f8", ("num_"+agency+"_reaches",), fill_value=self.FLOAT_FILL)
                locals()["tyr"+agency].long_name = agency+" two_Year_Return"
                locals()["tyr"+agency].comment = agency+" two year return interval discharge value in this cell"
                locals()["tyr"+agency].units = "m^3/s"
                locals()["tyr"+agency][:] = np.nan_to_num(self.map_dict[continent]["tyr"][ai], copy=True, nan=self.FLOAT_FILL)
    
            
                
                locals()["riggs_id"+agency] = riggs.createVariable(agency+"_id", "S1", ("num_"+agency+"_reaches", "nchars"), fill_value=self.INT_FILL)
                locals()["riggs_id"+agency].long_name = agency+"_ID_number"
                locals()["riggs_id"+agency][:] = stringtochar(self.map_dict[continent]["riggs_id"][ai].astype("S100"))
                #riggs_id[:] = (self.map_dict[continent]["riggs_id"].astype("S16")
                
                locals()["riggs_q"+agency] = riggs.createVariable(agency+"_q", "f8", ("num_"+agency+"_reaches", "num_days"), fill_value=self.FLOAT_FILL)
                locals()["riggs_q"+agency].long_name = agency+"_discharge_time_series_(daily)"
                locals()["riggs_q"+agency].comment = "Direct port from "+agency
                locals()["riggs_q"+agency].units = "m^3/s"
                locals()["riggs_q"+agency][:] = np.nan_to_num(self.map_dict[continent]["riggs_q"][ai], copy=True, nan=self.FLOAT_FILL)
        
                locals()["riggs_qt"+agency]= riggs.createVariable(agency+"_qt", "f8", ("num_"+agency+"_reaches", "num_days"), fill_value=self.FLOAT_FILL)
                locals()["riggs_qt"+agency].long_name = agency+"_discharge_time_series_(daily)"
                locals()["riggs_qt"+agency].comment = "Direct port from "+agency
                locals()["riggs_qt"+agency].units = "days since Jan 1 Year 1"
                locals()["riggs_qt"+agency][:] = np.nan_to_num(self.map_dict[continent]["riggs_qt"][ai], copy=True, nan=self.FLOAT_FILL)

    def map_data(self):
        """Maps riggs data to SoS organized by continent.
        
        Stores mapped data in map_dict attribute.
        """

        for continent, sos_data in self.sos_dict.items():
            if sos_data:
                # Reach identifiers
                sos_ids = sos_data["reach_id"]
                riggs_ids = self.riggs_dict["reachId"]
                same_ids = np.intersect1d(sos_ids, riggs_ids)
                indexes = np.where(np.isin(riggs_ids, same_ids))[0]

                if indexes.size == 0:
                    self.map_dict[continent] = None
                else:
                    # Map Riggs data that matches SoS reach identifiers
                    self.map_dict[continent]["days"] = np.array(range(1, len(self.riggs_dict["Qwrite"][0]) + 1))
                    self.map_dict[continent]["riggs_reach_id"] = self.riggs_dict["reachId"].astype(np.int64)[indexes]
                    self.map_dict[continent]["fdq"] = self.riggs_dict["FDQS"][indexes,:]
                    self.map_dict[continent]["max_q"] =self.riggs_dict["Qmax"][indexes]
                    self.map_dict[continent]["monthly_q"] = self.riggs_dict["MONQ"][indexes,:]
                    self.map_dict[continent]["mean_q"] = self.riggs_dict["Qmean"][indexes]
                    self.map_dict[continent]["min_q"] = self.riggs_dict["Qmin"][indexes]
                    self.map_dict[continent]["tyr"] = self.riggs_dict["TwoYr"][indexes]
                    self.map_dict[continent]["riggs_id"] = np.array(self.riggs_dict["data"])[indexes]
                    self.map_dict[continent]["riggs_q"] = self.riggs_dict["Qwrite"][indexes,:]
                    self.map_dict[continent]["riggs_qt"] = self.riggs_dict["Twrite"][indexes,:]
            else:
                self.map_dict[continent] = None
    
    def read_sos(self):
        """Reads in data from the SoS and stores in sos_dict attribute."""

        with scandir(self.sos_dir) as entries:
            sos_files = [ entry for entry in entries \
                if entry.name != "unconstrained" and entry.name != "constrained" ]
        for sos_file in sos_files:
            continent = sos_file.name.split('_')[0]
            sos = Dataset(Path(sos_file))
            self.sos_dict[continent] = { 
                "reach_id": sos["reaches"]["reach_id"][:].filled(np.nan)
            }
            sos.close()
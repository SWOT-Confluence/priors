# Standard imports
from datetime import datetime
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset, stringtochar
import numpy as np
import collections
import json

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

    def __init__(self, sos_file, Riggs_dict, metadata_json):
        """
        Parameters
        ----------
        sos_file: Path
            Path to SoS NetCDF file
        temp_sos: TemporaryDirectory
            Temporary directory that holds old SoS version
        Riggs_dict: dict
            Dictionary of riggs gauge data
        """

        self.sos_file = sos_file
        self.temp_sos = None
        self.Riggs_dict = Riggs_dict
        self.map_dict = {}
        self.sos_reaches = None
        self.variable_atts = metadata_json  

    def nested_dict(self):
        return collections.defaultdict(self.nested_dict)

    def read_sos(self):
        """Reads in data from the SoS and stores in sos_reaches attribute."""

        sos = Dataset(Path(self.sos_file))
        self.sos_reaches = sos["reaches"]["reach_id"][:].filled(np.nan).astype(int)
        sos.close()


    def map_data(self):
        """Maps USGS data to SoS, and stores data in map_dict attribute."""

        self.map_dict = self.nested_dict()

        # separate riggs dict by agency
        for agency in set(list(self.Riggs_dict["Agency"])):
            print('processing agency', agency, '-----------------------------------------------------------')

            # currently riggs dict is holding data for many agencies
            # we need to pull out only data for the agency we are currently processing
            # we find the indexes all of the data in riggs dict associated with that agency here
            agency_indexes = np.where(np.array(self.Riggs_dict['Agency'][:]) == agency)

            print(f'there were {len(agency_indexes[0])} entries in riggs dict for {agency}')
            print(f'there were {len(self.Riggs_dict["Agency"][:])} entries total in riggs dict')


            # Reach identifiers
            Riggs_ids_all = self.Riggs_dict["reachId"]
            # print(Riggs_ids_all, '1')
            # print(Riggs_ids_all[0], '2')
            Riggs_ids = []

            for i in agency_indexes[0]:
                # print(i)
                single_id = Riggs_ids_all[i]
                # print(single_id, '-------------------------')
                Riggs_ids.append(int(single_id))
            # Riggs_ids = [int(Riggs_ids_all[i]) for i in agency_indexes]
            # print('looking to match')
            # print(Riggs_ids[:10])
            # print('with')
            # print(self.sos_reaches[:10])
            # print('riggs ids')
            print(len(Riggs_ids), 'riggsids')
            same_ids = np.intersect1d(self.sos_reaches, Riggs_ids)
            indexes = np.where(np.isin(Riggs_ids, same_ids))[0]
            # print('indexes here --------------------------')
            print(len(indexes), 'indexes here')
            
            if indexes.size == 0:
                self.map_dict[agency] = None
            else:
                print('found agency data, writing it into the sos now...')
                # Map Riggs data that matches SoS reach identifiers
                # self.map_dict[agency]["days"] = np.array(range(1, len(self.Riggs_dict["Qwrite"][0]) + 1))
                # self.map_dict[agency]["Riggs_reach_id"] = self.Riggs_dict["reachId"].astype(np.int64)[indexes]
                # self.map_dict[agency]["fdq"] = self.Riggs_dict["FDQS"][indexes,:]
                # self.map_dict[agency]["max_q"] =self.Riggs_dict["Qmax"][indexes]
                # self.map_dict[agency]["monthly_q"] = self.Riggs_dict["MONQ"][indexes,:]
                # self.map_dict[agency]["mean_q"] = self.Riggs_dict["Qmean"][indexes]
                # self.map_dict[agency]["min_q"] = self.Riggs_dict["Qmin"][indexes]
                # self.map_dict[agency]["tyr"] = self.Riggs_dict["TwoYr"][indexes]
                # self.map_dict[agency]["Riggs_id"] = np.array(self.Riggs_dict["data"])[indexes]
                # self.map_dict[agency]["Riggs_q"] = self.Riggs_dict["Qwrite"][indexes,:]
                # self.map_dict[agency]["Riggs_qt"] = self.Riggs_dict["Twrite"][indexes,:]

                self.map_dict[agency]["days"] = np.array(range(1, len(self.Riggs_dict["Qwrite"][0]) + 1))
                self.map_dict[agency]["Riggs_reach_id"] = self.Riggs_dict["reachId"].astype(np.int64)[agency_indexes]
                self.map_dict[agency]["fdq"] = self.Riggs_dict["FDQS"][agency_indexes]
                self.map_dict[agency]["max_q"] =self.Riggs_dict["Qmax"][agency_indexes]
                self.map_dict[agency]["monthly_q"] = self.Riggs_dict["MONQ"][agency_indexes]
                self.map_dict[agency]["mean_q"] = self.Riggs_dict["Qmean"][agency_indexes]
                self.map_dict[agency]["min_q"] = self.Riggs_dict["Qmin"][agency_indexes]
                self.map_dict[agency]["tyr"] = self.Riggs_dict["TwoYr"][agency_indexes]
                self.map_dict[agency]["Riggs_id"] = np.array(self.Riggs_dict["data"])[agency_indexes]
                self.map_dict[agency]["Riggs_q"] = self.Riggs_dict["Qwrite"][agency_indexes]
                self.map_dict[agency]["Riggs_qt"] = self.Riggs_dict["Twrite"][agency_indexes]

        # Serializing json
        # for i in self.map_dict[agency]["Riggs_q"][:10]:
        #     print(i[:10])

        # for i in self.Riggs_dict["Qwrite"][:10]:
        #     print(i[:10])
        # raise
    def update_data(self):
        """Updates Riggs data in the SoS.
        
        Data is stored in a group labelled model as it will accommodate Riggs for 
        constrained runs.

        Requires: SoS created with Riggs group present.
        """

        if self.map_dict:
            # print(self.map_dict)
            sos = Dataset(self.sos_file, 'a')
            sos.production_date = datetime.now().strftime('%d-%b-%Y %H:%M:%S')
            agencies = set(list(self.Riggs_dict["Agency"]))
            for agency in agencies:
                
                print("AGENCY: ", agency)

                
                Riggs = sos[agency]
                
                print('no metadata for ',agency)
                Riggs["num_days"][:] = self.map_dict[agency]["days"]
                print('how many days',len(Riggs["num_days"][:]))
                print('how many gauges found', len(self.map_dict[agency]["Riggs_reach_id"]))
                print('how many are there', len(Riggs[f"{agency}_reach_id"][:]))

                
                # used f string for agency so it generalizes the sos creation for different agencies
                

                
                Riggs[f"{agency}_reach_id"][:] = self.map_dict[agency]["Riggs_reach_id"]

                
                Riggs[f"{agency}_flow_duration_q"][:] = np.nan_to_num(self.map_dict[agency]["fdq"], copy=True, nan=self.FLOAT_FILL)

                
                Riggs[f"{agency}_max_q"][:] = np.nan_to_num(self.map_dict[agency]["max_q"], copy=True, nan=self.FLOAT_FILL)

                
                Riggs[f"{agency}_monthly_q"][:] = np.nan_to_num(self.map_dict[agency]["monthly_q"], copy=True, nan=self.FLOAT_FILL)

                
                Riggs[f"{agency}_mean_q"][:] = np.nan_to_num(self.map_dict[agency]["mean_q"], copy=True, nan=self.FLOAT_FILL)

                
                Riggs[f"{agency}_min_q"][:] = np.nan_to_num(self.map_dict[agency]["min_q"], copy=True, nan=self.FLOAT_FILL)

                
                Riggs[f"{agency}_two_year_return_q"][:] = np.nan_to_num(self.map_dict[agency]["tyr"], copy=True, nan=self.FLOAT_FILL)

                
                Riggs[f"{agency}_id"][:] = stringtochar(self.map_dict[agency]["Riggs_id"].astype("S100"))

                

                Riggs[f"{agency}_q"][:] = np.nan_to_num(self.map_dict[agency]["Riggs_q"], copy=True, nan=self.FLOAT_FILL)

                
                Riggs[f"{agency}_qt"][:] = np.nan_to_num(self.map_dict[agency]["Riggs_qt"], copy=True, nan=self.FLOAT_FILL)

                print('how many days',len(Riggs["num_days"][:]))
                print('how many gauges found', len(self.map_dict[agency]["Riggs_reach_id"]))
                print('how many are there', len(Riggs[f"{agency}_reach_id"][:]))
                
                # used f string for agency so it generalizes the sos creation for different agencies
                

                
                Riggs[f"{agency}_reach_id"][:] = self.map_dict[agency]["Riggs_reach_id"]
                
                Riggs[f"{agency}_flow_duration_q"][:] = np.nan_to_num(self.map_dict[agency]["fdq"], copy=True, nan=self.FLOAT_FILL)

                
                Riggs[f"{agency}_max_q"][:] = np.nan_to_num(self.map_dict[agency]["max_q"], copy=True, nan=self.FLOAT_FILL)

                
                Riggs[f"{agency}_monthly_q"][:] = np.nan_to_num(self.map_dict[agency]["monthly_q"], copy=True, nan=self.FLOAT_FILL)

                
                Riggs[f"{agency}_mean_q"][:] = np.nan_to_num(self.map_dict[agency]["mean_q"], copy=True, nan=self.FLOAT_FILL)

                
                Riggs[f"{agency}_min_q"][:] = np.nan_to_num(self.map_dict[agency]["min_q"], copy=True, nan=self.FLOAT_FILL)

                
                Riggs[f"{agency}_two_year_return_q"][:] = np.nan_to_num(self.map_dict[agency]["tyr"], copy=True, nan=self.FLOAT_FILL)

                
                Riggs[f"{agency}_id"][:] = stringtochar(self.map_dict[agency]["Riggs_id"].astype("S100"))
                
                Riggs[f"{agency}_q"][:] = np.nan_to_num(self.map_dict[agency]["Riggs_q"], copy=True, nan=self.FLOAT_FILL)
                
                Riggs[f"{agency}_qt"][:] = np.nan_to_num(self.map_dict[agency]["Riggs_qt"], copy=True, nan=self.FLOAT_FILL)
                try:
                    variable_atts = self.variable_atts[agency]
                    self.set_variable_atts(Riggs[f"{agency}_qt"], variable_atts[f"{agency}_qt"])
                    self.set_variable_atts(Riggs[f"{agency}_q"], variable_atts[f"{agency}_q"])
                    self.set_variable_atts(Riggs[f"{agency}_id"], variable_atts[f"{agency}_id"])
                    self.set_variable_atts(Riggs["num_days"], variable_atts["num_days"])
                    self.set_variable_atts(Riggs[f"{agency}_reaches"], variable_atts[f"{agency}_reaches"])
                    self.set_variable_atts(Riggs["CAL"], variable_atts["CAL"])
                    self.set_variable_atts(Riggs[f"{agency}_flow_duration_q"], variable_atts[f"{agency}_flow_duration_q"])
                    self.set_variable_atts(Riggs[f"{agency}_max_q"], variable_atts[f"{agency}_max_q"])
                    self.set_variable_atts(Riggs[f"{agency}_monthly_q"], variable_atts[f"{agency}_monthly_q"])
                    self.set_variable_atts(Riggs[f"{agency}_mean_q"], variable_atts[f"{agency}_mean_q"])
                    self.set_variable_atts(Riggs[f"{agency}_min_q"], variable_atts[f"{agency}_min_q"])
                    self.set_variable_atts(Riggs[f"{agency}_two_year_return_q"], variable_atts[f"{agency}_two_year_return_q"])
                    self.set_variable_atts(Riggs[f"{agency}_id"], variable_atts[f"{agency}_id"])
                    self.set_variable_atts(Riggs[f"{agency}_q"], variable_atts[f"{agency}_q"])
                    self.set_variable_atts(Riggs[f"{agency}_qt"], variable_atts[f"{agency}_qt"])

                    self.set_variable_atts(Riggs[f"{agency}_reaches"], variable_atts[f"{agency}_reaches"])
                    self.set_variable_atts(Riggs["CAL"], variable_atts["CAL"])
                    self.set_variable_atts(Riggs[f"{agency}_flow_duration_q"], variable_atts[f"{agency}_flow_duration_q"])
                    self.set_variable_atts(Riggs[f"{agency}_max_q"], variable_atts[f"{agency}_max_q"])
                    self.set_variable_atts(Riggs[f"{agency}_monthly_q"], variable_atts[f"{agency}_monthly_q"])
                    self.set_variable_atts(Riggs[f"{agency}_mean_q"], variable_atts[f"{agency}_mean_q"])
                    self.set_variable_atts(Riggs[f"{agency}_min_q"], variable_atts[f"{agency}_min_q"])
                    self.set_variable_atts(Riggs[f"{agency}_two_year_return_q"], variable_atts[f"{agency}_two_year_return_q"])
                    self.set_variable_atts(Riggs["num_days"], variable_atts["num_days"])

                except:
                    print('no metadata for ',agency)

                
                if agency in self.variable_atts.keys():
                    variable_atts = self.variable_atts[agency]
                    self.set_variable_atts(Riggs["num_days"], variable_atts["num_days"])
                
                # used f string for agency so it generalizes the sos creation for different agencies
                
                    self.set_variable_atts(Riggs[f"{agency}_reaches"], variable_atts[f"{agency}_reaches"])
                    self.set_variable_atts(Riggs[f"{agency}_max_q"], variable_atts[f"{agency}_max_q"])
                    self.set_variable_atts(Riggs["CAL"], variable_atts["CAL"])
                    self.set_variable_atts(Riggs[f"{agency}_reach_id"], variable_atts[f"{agency}_reach_id"])
                    self.set_variable_atts(Riggs[f"{agency}_flow_duration_q"], variable_atts[f"{agency}_flow_duration_q"])
                    self.set_variable_atts(Riggs[f"{agency}_monthly_q"], variable_atts[f"{agency}_monthly_q"])
                    self.set_variable_atts(Riggs[f"{agency}_mean_q"], variable_atts[f"{agency}_mean_q"])
                    self.set_variable_atts(Riggs[f"{agency}_min_q"], variable_atts[f"{agency}_min_q"])
                    self.set_variable_atts(Riggs[f"{agency}_two_year_return_q"], variable_atts[f"{agency}_two_year_return_q"])
                    self.set_variable_atts(Riggs[f"{agency}_id"], variable_atts[f"{agency}_id"])
                    self.set_variable_atts(Riggs[f"{agency}_q"], variable_atts[f"{agency}_q"])
                    self.set_variable_atts(Riggs[f"{agency}_qt"], variable_atts[f"{agency}_qt"])
                else:
                    print('metadata not found for this agency:', agency)
            sos.close()
            
    def set_variable_atts(self, variable, variable_dict):
        """Set the variable attribute metdata."""
        
        for name, value in variable_dict.items():
            setattr(variable, name, value)
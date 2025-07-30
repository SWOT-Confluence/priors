# Standard imports
from datetime import datetime, timezone
from dateutil import relativedelta
import json
from pathlib import Path
import sys
import traceback
import uuid

# Third-party imports
import boto3
boto3.set_stream_logger("boto3.resources")
import botocore
from netCDF4 import Dataset, stringtochar
import numpy as np

def closest(lst, K):
    # https://www.geeksforgeeks.org/python-find-closest-number-to-k-in-given-list/

    return min(range(len(lst)), key = lambda i: abs(lst[i]-K))

class Sos:
    """Class that represents the SoS and required ops to create a new version.
    
    Attributes
    ----------
    bad_priors: np.array
        list of either USGS or GRDC q priors that are less than 0.
    bad_priors_source: np.array
        list that indicates source of bad prior
    continent: str
        continent abbreviation to id SoS file
    overwritten_indexes: np.array
        list of integer index values where grades data was overwritten
    overwritten_source: np.array
        list of either 'usgs' or 'grdc' to indicate source of overwritten data
    run_type: str
        'constrained' or 'unconstrained' data product type
    sos_dir: Path
        path to SoS directory on local storage
    sos_file: Path
        path to SoS file
    confluence_creds: dict
            Dictionary of s3 credentials 
    suffix: str
        ending name of the SoS
    VERS_LENGTH: int
        number of integers in SoS identifier
    version: str
        current version of the SoS (for current execution)
    last_run_time: str
        %Y-%m-%d of the sos production date, to be used for pulling gauge data
    
    Methods
    -------
    copy_sos()
        Copy the latest version of the SoS to local storage
    create_new_version(priors_list)
        Creates a new version of the SoS with updated priors
    upload_new_version()
        Uploads new version to Confluence S3 bucket
    """



    VERS_LENGTH = 4
    MOD_TIME = 0    # seconds

    def __init__(self, continent, run_type, sos_dir, metadata_json, priors_list,
                 podaac_update, podaac_bucket, sos_bucket, swordversion):



        """
        Parameters
        ----------
        continent: str
            continent abbreviation to id SoS file
        run_type: str
            'constrained' or 'unconstrained' data product type
        sos_dir: Path
            path to SoS directory on local storage
        """
        self.bad_prior = np.array([])
        self.bad_prior_source = np.array([])
        self.continent = continent
        self.last_run_time = ""
        self.metadata_json = metadata_json
        self.overwritten_indexes = np.array([])
        self.overwritten_source = np.array([])
        self.run_type = run_type
        self.sos_dir = sos_dir
        self.sos_file = None
        self.version = ""
        self.priors_list = priors_list
        self.run_date = datetime.now()
        self.podaac_update = podaac_update
        self.podaac_bucket = podaac_bucket
        self.sos_bucket = sos_bucket
        SWORD_VERSION = f"v{swordversion}"
        suffix = f"_sword_v{swordversion}_SOS_priors.nc"
        self.swordversion = SWORD_VERSION
        self.suffix = suffix

    def download_previous_sos(self, sos_version):
        """Download the previous version of the SoS file to local storage."""

        # Download pervious version of the SoS
        previous_version = str(int(sos_version) - 1)
        padding = ['0'] * (self.VERS_LENGTH - len(previous_version))
        previous_version = f"{''.join(padding)}{previous_version}"
        print(f"Locating: {self.run_type}/{previous_version}/{self.continent}{self.suffix}")
        try:
            s3 = boto3.client("s3")
            response = s3.download_file(Bucket=self.sos_bucket, Key=f"{self.run_type}/{previous_version}/{self.continent}{self.suffix}", Filename=f"{self.sos_dir}/{self.continent}{self.suffix}")
        except botocore.exceptions.ClientError as error:
            print(f"ERROR: Could not download current version of the SoS.")
            print(error)
            raise error
        print(f"Downloaded: {self.run_type}/{previous_version}/{self.continent}{self.suffix}")

    def create_new_version(self):
        """Create new version of the SoS file from the previous version.
        
        Returns
        -------
        sos_file: pathlib.PosixPath
            Path to new SoS file on local storage
        """

        # Retrieve global attribute metadata
        global_atts = self.metadata_json["global_attributes"]
        
        # Create new SoS
        self.sos_file = Path(f"{str(self.sos_dir)}/{self.continent}{self.suffix}")
        sos = Dataset(self.sos_file, 'a')
        self.last_run_time = datetime.strptime(sos.production_date.split(' ')[0], '%d-%b-%Y').strftime('%Y-%m-%d')
        
        # Store global atts
        for name, value in global_atts.items():
            setattr(sos, name, value)
            
        # Fix inconsistencies in global attributes
        fix_global_attributes(sos)
            
        # Update attributes for current execution
        global_atts_extra = self.metadata_json["global_attributes_extra"]
        
        # # Version and UUID
        self.version = str(int(sos.product_version) + 1)
        print('updating to new version', self.version,'-----------------------------------------') 
        padding = ['0'] * (self.VERS_LENGTH - len(self.version))
        sos.product_version = f"{''.join(padding)}{self.version}"
        sos.date_modified = self.run_date.strftime('%Y-%m-%dT%H:%M:%S')
        sos.uuid = str(uuid.uuid4())
        
        # # History and source
        sos.history = f"{self.run_date.strftime('%Y-%m-%dT%H:%M:%S')}: SoS version {''.join(padding)}{self.version} created by Confluence version {global_atts_extra['confluence_version']}"
        source = f"Gage data sources: {', '.join(global_atts_extra['continent_agency'][self.continent])}"
        if self.run_type == "constrained":
            source += f"; Model data source: GRADES"
        else:
            source += f"; Model data source: WBM-Sed"
        if "gbpriors" in self.priors_list:
            source += f"; Other data source: GeoBAM"
        sos.source = source  
        
        # # Comment and references
        comment = f"Prior data was taken from gage agencies: {', '.join(global_atts_extra['continent_agency'][self.continent])}"
        if self.run_type == "constrained":
            comment += f"; Model data source for constrained run: Global Reach-scale A priori Discharge Estimates (GRADES)"
        else:
            comment += f"; Model data source for unconstrained run: Water Balance Model (WBM-Sed)"
        if "gbpriors" in self.priors_list:
            comment += f"; GeoBAM priors created from SWOT shapefiles"
        sos.comment = comment
        reference = ""
        for agency in global_atts_extra["continent_agency"][self.continent]:
            reference += f"{global_atts_extra['references'][agency]}; "
        if self.run_type == "constrained":
            reference += f"{global_atts_extra['references']['constrained']}; "
        else:
            reference += f"{global_atts_extra['references']['unconstrained']}; "
        if "gbpriors" in self.priors_list:
            reference += f"{global_atts_extra['references']['gbpriors']}; "
        sos.references = reference
        
        # Update reach and node variables
        set_variable_atts(sos["reaches"]["reach_id"], self.metadata_json["reaches"]["reach_id"])
        set_variable_atts(sos["nodes"]["node_id"], self.metadata_json["nodes"]["node_id"])
        set_variable_atts(sos["nodes"]["reach_id"], self.metadata_json["nodes"]["reach_id"])
        
        # Update model variables
        update_model(sos["model"], self.metadata_json[f"model_{self.run_type}"])
        
        # Update historicQ
        if "historicQ" in sos.groups.keys(): update_historic_gauges(sos["historicQ"], self.metadata_json, self.continent)
        
        sos.close()
        print(f"Created version {''.join(padding)}{self.version} of: {self.sos_file.name}")
        
    def store_geospatial_data(self, sword_file):
        """Store geospatial data - lat, lon, river names and coverage."""
        
        sword = Dataset(sword_file)
        sos = Dataset(self.sos_file, 'a')
        
        # Global attributes
        sos.geospatial_lat_min = sword.y_min
        sos.geospatial_lat_max = sword.y_max
        sos.geospatial_lon_min = sword.x_min
        sos.geospatial_lon_max = sword.y_min
        
        # Reach-level data
        reaches = sos["reaches"]
        # # Latitude
        if "x" not in reaches.variables:
            x = reaches.createVariable("x", "f8", ("num_reaches"), compression="zlib")
            x[:] = sword["reaches"]["x"][:]
        else:
            x = reaches["x"]
        set_variable_atts(x, self.metadata_json["reaches"]["x"])
        # # Longitude
        if "y" not in reaches.variables:
            y = reaches.createVariable("y", "f8", ("num_reaches"), compression="zlib")
            y[:] = sword["reaches"]["y"][:]
        else:
            y = reaches["y"]
        set_variable_atts(y, self.metadata_json["reaches"]["y"])
        ## River names
        if "river_name" not in reaches.variables:
            river_name = reaches.createVariable("river_name", str, ("num_reaches"))
            river_name._Encoding = "ascii"
            river_name[:] = sword["reaches"]["river_name"][:]
        else:
            river_name = reaches["river_name"]
        set_variable_atts(river_name, self.metadata_json["reaches"]["river_name"])
        
        # Node-level data
        nodes = sos["nodes"]
        # # Latitude
        if "x" not in nodes.variables:
            x = nodes.createVariable("x", "f8", ("num_nodes"), compression="zlib")
            x[:] = sword["nodes"]["x"][:]
        else:
            x = nodes["x"]
        set_variable_atts(x, self.metadata_json["nodes"]["x"])
        # # Longitude
        if "y" not in nodes.variables:
            y = nodes.createVariable("y", "f8", ("num_nodes"), compression="zlib")
            y[:] = sword["nodes"]["y"][:]
        else:
            y = nodes["y"]
        set_variable_atts(y, self.metadata_json["nodes"]["y"])
        ## River names
        if "river_name" not in nodes.variables:
            river_name = nodes.createVariable("river_name", str, ("num_nodes"))
            river_name._Encoding = "ascii"
            river_name[:] = sword["nodes"]["river_name"][:]
        else:
            river_name = nodes["river_name"]
        set_variable_atts(river_name, self.metadata_json["nodes"]["river_name"])
                
        sword.close()
        sos.close()

    def overwrite_grades(self):
        """Overwrite GRADES data with gaged (USGS or GRDC) data in the SoS."""

        sos = Dataset(self.sos_file, 'a')
        
        self.bad_prior = np.zeros(sos.dimensions["num_reaches"].size, dtype=np.int32)
        self.bad_prior_source = np.full(sos.dimensions["num_reaches"].size, "xxxx", dtype="S4")
        self.overwritten_indexes = np.zeros(sos.dimensions["num_reaches"].size, dtype=np.int32)
        self.overwritten_source = np.full(sos.dimensions["num_reaches"].size, "xxxx", dtype="S4")

        grdc_reach_ids = sos["historicQ"]["grdc"]["grdc_reach_id"][:]
        for rid in grdc_reach_ids:
            self._overwrite_prior(rid, sos, sos["historicQ"]["grdc"], "grdc")
    

        # could make this iterative based on global agency variable, also check cal/val split
        if self.continent == "na":

             # historic USGS
            historic_usgs_reach_ids = sos["historicQ"]["USGS"]["USGS_reach_id"][:]
            for index, rid in enumerate(historic_usgs_reach_ids):
                self._overwrite_prior(rid, sos, sos["historicQ"]["USGS"], "USGS")   

            # USGS
            usgs_reach_ids = sos["USGS"]["USGS_reach_id"][:]
            usgs_cal = sos["USGS"]["CAL"][:]
            for index, rid in enumerate(usgs_reach_ids) :
                # check for cal/val
                if usgs_cal[index] == 1:
                    self._overwrite_prior(rid, sos, sos["USGS"], "USGS")

            # Historic WSC
            historic_wsc_reach_ids = sos["historicQ"]["WSC"]["WSC_reach_id"][:]
            for index, rid in enumerate(historic_wsc_reach_ids):
                self._overwrite_prior(rid, sos, sos["historicQ"]["WSC"], "WSC")  

            # WSC
            wsc_reach_ids = sos["WSC"]["WSC_reach_id"][:]
            wsc_cal = sos["WSC"]["CAL"][:]
            for index, rid in enumerate(wsc_reach_ids):
                # check for cal/val
                if wsc_cal[index] == 1:
                    self._overwrite_prior(rid, sos, sos["WSC"], "WSC")

        if self.continent == 'eu':
            # defra
            defra_reach_ids = sos["DEFRA"]["DEFRA_reach_id"][:]
            defra_cal = sos["DEFRA"]["CAL"][:]
            for index, rid in enumerate(defra_reach_ids) :
                # check for cal/val
                if defra_cal[index] == 1: 
                    self._overwrite_prior(rid, sos, sos["DEFRA"], "DEFRA")


            # Historic EAU
            historic_wsc_reach_ids = sos["historicQ"]["EAU"]["EAU_reach_id"][:]
            for index, rid in enumerate(historic_wsc_reach_ids):
                self._overwrite_prior(rid, sos, sos["historicQ"]["EAU"], "EAU")  

            # EAU
            wsc_reach_ids = sos["EAU"]["EAU_reach_id"][:]
            wsc_cal = sos["EAU"]["CAL"][:]
            for index, rid in enumerate(wsc_reach_ids):
                # check for cal/val
                if wsc_cal[index] == 1:
                    self._overwrite_prior(rid, sos, sos["EAU"], "EAU")

        if self.continent == 'oc':
            #ABOM
            defra_reach_ids = sos["ABOM"]["ABOM_reach_id"][:]
            defra_cal = sos["ABOM"]["CAL"][:]
            for index, rid in enumerate(defra_reach_ids) :
                # check for cal/val
                if defra_cal[index] == 1: 
                    self._overwrite_prior(rid, sos, sos["ABOM"], "ABOM") 
        
        if self.continent == 'as':

            # Historic MLIT
            historic_wsc_reach_ids = sos["historicQ"]["MLIT"]["MLIT_reach_id"][:]
            for index, rid in enumerate(historic_wsc_reach_ids):
                self._overwrite_prior(rid, sos, sos["historicQ"]["MLIT"], "MLIT")  


            # defra_reach_ids = sos["MLIT"]["MLIT_reach_id"][:]
            # defra_cal = sos["MLIT"]["CAL"][:]
            # for index, rid in enumerate(defra_reach_ids) :
            #     # check for cal/val
            #     if defra_cal[index] == 1: 
            #         self._overwrite_prior(rid, sos, sos["MLIT"], "MLIT")
        
        if self.continent == 'sa':

            # Historic Hidroweb
            historic_wsc_reach_ids = sos["historicQ"]["Hidroweb"]["Hidroweb_reach_id"][:]
            for index, rid in enumerate(historic_wsc_reach_ids):
                self._overwrite_prior(rid, sos, sos["historicQ"]["Hidroweb"], "Hidroweb")  


            defra_reach_ids = sos["Hidroweb"]["Hidroweb_reach_id"][:]
            defra_cal = sos["Hidroweb"]["CAL"][:]
            for index, rid in enumerate(defra_reach_ids) :
                # check for cal/val
                if defra_cal[index] == 1: 
                    self._overwrite_prior(rid, sos, sos["Hidroweb"], "Hidroweb")


            # DGA, only historic
            defra_reach_ids = sos["historicQ"]["DGA"]["DGA_reach_id"][:]
            for index, rid in enumerate(defra_reach_ids) :
                self._overwrite_prior(rid, sos, sos["historicQ"]["DGA"], "DGA")

        # Hydroshaq, no matter the continent
        usgs_reach_ids = sos["hydroshare"]["hydroshare_reach_id"][:]
        usgs_cal = sos["hydroshare"]["CAL"][:]
        for index, rid in enumerate(usgs_reach_ids) :
            # check for cal/val
            if usgs_cal[index] == 1:
                self._overwrite_prior(rid, sos, sos["hydroshare"], "hydroshare")


        self._create_dims_vars(sos)

        sos["model"]["overwritten_indexes"][:] = self.overwritten_indexes
        set_variable_atts(sos["model"]["overwritten_indexes"], self.metadata_json["model_constrained"]["overwritten_indexes"])
        
        sos["model"]["overwritten_source"][:] = stringtochar(np.array(self.overwritten_source, dtype="S4"))
        set_variable_atts(sos["model"]["overwritten_source"], self.metadata_json["model_constrained"]["overwritten_source"])
        
        sos["model"]["bad_priors"][:] = self.bad_prior
        set_variable_atts(sos["model"]["bad_priors"], self.metadata_json["model_constrained"]["bad_priors"])
        
        sos["model"]["bad_prior_source"][:] = stringtochar(np.array(self.bad_prior_source, dtype="S4"))
        set_variable_atts(sos["model"]["bad_prior_source"], self.metadata_json["model_constrained"]["bad_prior_source"])

        sos.close()

    def _overwrite_prior(self, reach_id, sos, gage, source):
        """Overwrite prior in grades with prior found in gage.

        Parameters
        ----------
        reach_id: int
            unique reach identifier
        sos: netCDF4._netCDF4.Dataset
            sos NetCDF Dataset
        gage: netCDF4._netCDF4.Group
            gage NetCDF group
        source: str
            name of gage data product source
        """

        sos_index = np.where(reach_id == sos["reaches"]["reach_id"][:])
        gage_index = np.where(reach_id == gage[f"{source}_reach_id"][:])
        try:
            all_agencies = sos.gauge_agency.split(';')

            no_nrt_validation_gauges_in_reach = True

            for current_agency_name in all_agencies:
                is_there_a_nrt_val_gauge_in_the_reach = bool(len(np.where(sos[current_agency_name]['CAL'][np.where(sos[current_agency_name][f'{current_agency_name}_reach_id'][:] == reach_id)] == 0)[0]))
                if is_there_a_nrt_val_gauge_in_the_reach == True:
                    no_nrt_validation_gauges_in_reach = False
        except Exception as e:
            print(e)
            traceback.print_exception(*sys.exc_info())
            no_nrt_validation_gauges_in_reach = True

        if no_nrt_validation_gauges_in_reach:
            # check to see if more than one gauge was found
            if len(gage_index[0]) > 1:
                double_gauge = True 

                # find the mean q for each gauge
                gage_mean_q_list = [gage[f"{source}_mean_q"][i] for i in gage_index[0]]

                # in order to decide what one will replace the grades data, we find what guage had the closest to the prediction
                # this method of sorting could change
                winner_index = closest( gage_mean_q_list, sos["model"]["mean_q"][sos_index][0])

            else:
                double_gauge = False

            grades = sos["model"]
            if self._isvalid_q(gage, gage_index, source):
                if not double_gauge:
                    grades["flow_duration_q"][sos_index] = gage[f"{source}_flow_duration_q"][gage_index]
                    grades["max_q"][sos_index] = gage[f"{source}_max_q"][gage_index]
                    grades["monthly_q"][sos_index] = gage[f"{source}_monthly_q"][gage_index]
                    grades["mean_q"][sos_index] = gage[f"{source}_mean_q"][gage_index]
                    grades["min_q"][sos_index] = gage[f"{source}_min_q"][gage_index]
                    grades["two_year_return_q"][sos_index] = gage[f"{source}_two_year_return_q"][gage_index]
                    self.overwritten_indexes[sos_index] = 1
                    self.overwritten_source[sos_index] = source
                else:
                    grades["flow_duration_q"][sos_index] = gage[f"{source}_flow_duration_q"][gage_index][winner_index]
                    grades["max_q"][sos_index] = gage[f"{source}_max_q"][gage_index][winner_index]
                    grades["monthly_q"][sos_index] = gage[f"{source}_monthly_q"][gage_index][winner_index]
                    grades["mean_q"][sos_index] = gage[f"{source}_mean_q"][gage_index][winner_index]
                    grades["min_q"][sos_index] = gage[f"{source}_min_q"][gage_index][winner_index]
                    grades["two_year_return_q"][sos_index] = gage[f"{source}_two_year_return_q"][gage_index][winner_index]
                    self.overwritten_indexes[sos_index] = 1
                    self.overwritten_source[sos_index] = source

            else:
                self.bad_prior[sos_index] = 1
                self.bad_prior_source[sos_index] = source
        
    def _historic_overwrite_prior(self, reach_id, sos, gage, source):
        """Overwrite prior in grades with historic prior found in gage.

        Parameters
        ----------
        reach_id: int
            unique reach identifier
        sos: netCDF4._netCDF4.Dataset
            sos NetCDF Dataset
        gage: netCDF4._netCDF4.Group
            gage NetCDF group
        source: str
            name of gage data product source
        """

        sos_index = np.where(reach_id == sos["reaches"]["reach_id"][:])
        gage_index = np.where(reach_id == gage[f"{source}_reach_id"][:])
        
        # check to see if more than one gauge was found
        if len(gage_index[0]) > 1:
            double_gauge = True 

            # find the mean q for each gauge
            gage_mean_q_list = [gage[f"{source}_mean_q"][i] for i in gage_index[0]]

            # in order to decide what one will replace the grades data, we find what guage had the closest to the prediction
            # this method of sorting could change
            winner_index = closest( gage_mean_q_list, sos["model"]["mean_q"][sos_index][0])

        else:
            double_gauge = False

        grades = sos["model"]
        if self._isvalid_q(gage, gage_index, source):
            if not double_gauge:
                grades["flow_duration_q"][sos_index] = gage[f"{source}_flow_duration_q"][gage_index]
                grades["max_q"][sos_index] = gage[f"{source}_max_q"][gage_index]
                grades["monthly_q"][sos_index] = gage[f"{source}_monthly_q"][gage_index]
                grades["mean_q"][sos_index] = gage[f"{source}_mean_q"][gage_index]
                grades["min_q"][sos_index] = gage[f"{source}_min_q"][gage_index]
                grades["two_year_return_q"][sos_index] = gage[f"{source}_two_year_return_q"][gage_index]
                self.overwritten_indexes[sos_index] = 1
                self.overwritten_source[sos_index] = source
            else:
                grades["flow_duration_q"][sos_index] = gage[f"{source}_flow_duration_q"][gage_index][winner_index]
                grades["max_q"][sos_index] = gage[f"{source}_max_q"][gage_index][winner_index]
                grades["monthly_q"][sos_index] = gage[f"{source}_monthly_q"][gage_index][winner_index]
                grades["mean_q"][sos_index] = gage[f"{source}_mean_q"][gage_index][winner_index]
                grades["min_q"][sos_index] = gage[f"{source}_min_q"][gage_index][winner_index]
                grades["two_year_return_q"][sos_index] = gage[f"{source}_two_year_return_q"][gage_index][winner_index]
                self.overwritten_indexes[sos_index] = 1
                self.overwritten_source[sos_index] = source

        else:
            self.bad_prior[sos_index] = 1
            self.bad_prior_source[sos_index] = source


    def _isvalid_q(self, gage, gage_index, source):
        """Test if any q priors in gage data are less than or equal to 0.
        
        Parameters
        ----------
        gage: netCDF4.Group
            Gage group that  contains q priors
        gage_index: int
            integer index of gage data (corresponds to reach id)
            
        Returns
        -------
        Boolean indicator of valid (True) or invalid data (False)
        
        """
        
        keys = [f"{source}_flow_duration_q", f"{source}_max_q", f"{source}_monthly_q", f"{source}_mean_q", f"{source}_min_q", f"{source}_two_year_return_q"]
        valid = True
        for key in keys:
            var = gage[key][gage_index]
            if np.any(var <= 0) == True:
                valid = False
                break
        return valid

    def _create_dims_vars(self, sos):
        """Create dimensions and variables to track overwritten data.
        
        Parameters
        ----------
        sos: netCDF4._netCDF4.Dataset
            sos NetCDF Dataset
        """
        
        if "overwritten_indexes" not in sos["model"].variables:
            oi = sos["model"].createVariable("overwritten_indexes", "i4", ("num_reaches",), compression="zlib")
            oi.comment = "Indexes of GRADES priors that were overwritten."
            oi.long_name = "overwritten priors indexes"
            oi.valid_min = 0
            oi.valid_max = 1
            oi.flag_values = "0 1"
            oi.flag_meanings = "not_overwritten overwritten"
            oi.coverage_content_type = "qualityInformation"

        if "overwritten_source" not in sos["model"].variables:
            if "nchar" not in sos["model"].dimensions:
                sos["model"].createDimension("nchar", 4)
            os = sos["model"].createVariable("overwritten_source", "S1", ("num_reaches", "nchar"), compression="zlib")
            os.comment = "Source of gage data that overwrote GRADES priors."
            os.long_name = "overwritten priors sources"
            os.coverage_content_type = "referenceInformation"
        
        if "bad_priors" not in sos["model"].variables:
            bp = sos["model"].createVariable("bad_priors", "i4", ("num_reaches",), compression="zlib")
            bp.comment = "Indexes of invalid gage priors that were not overwritten."
            bp.valid_min = 0
            bp.valid_max = 1
            bp.flag_values = "0 1"
            bp.flag_meanings = "not_overwritten overwritten"
            bp.coverage_content_type = "qualityInformation"

        if "bad_prior_source" not in sos["model"].variables:
            bps = sos["model"].createVariable("bad_prior_source", "S1", ("num_reaches", "nchar"), compression="zlib")
            bps.comment = "Source of invalid gage priors."
            bps.long_name = "invalid gage prior sources"
            bps.coverage_content_type = "referenceInformation"

    def update_time_coverage(self, min_qt, max_qt):
        """Update time coverage global attributes for sos_file."""
        
        sos = Dataset(self.sos_file, 'a')
        if min_qt == "NO TIME DATA" or max_qt == "NO TIME DATA":
            if min_qt == "NO TIME DATA":
                sos.time_coverage_start = "NO TIME DATA"
            else:
                sos.time_coverage_start = min_qt.strftime("%Y-%m-%dT%H:%M:%S")
            if max_qt == "NO TIME DATA":
                sos.time_coverage_end = "NO TIME DATA"
            else:
                sos.time_coverage_end = max_qt.strftime("%Y-%m-%dT%H:%M:%S")
            duration = "NO TIME DATA"
        else:
            sos.time_coverage_start = min_qt.strftime("%Y-%m-%dT%H:%M:%S")
            sos.time_coverage_end = max_qt.strftime("%Y-%m-%dT%H:%M:%S")
            duration = relativedelta.relativedelta(max_qt, min_qt)
            sos.time_coverage_duration = f"P{duration.years}Y{duration.months}M{duration.days}DT{duration.hours}H{duration.minutes}M{duration.seconds}S"
        sos.close()

    def upload_file(self):
        """Upload SoS file to S3 bucket(s)."""

        # Upload to Confluence bucket
        sos_ds = Dataset(self.sos_file, 'r')
        vers = sos_ds.product_version
        print('uploading ', vers)
        sos_ds.close()

        s3 = boto3.client("s3")
        if self.sos_bucket == "confluence-sos":
            response = s3.upload_file(str(self.sos_file), 
                                    "confluence-sos", 
                                    f"{self.run_type}/{vers}/{self.sos_file.name}")
        else:
            response = s3.upload_file(str(self.sos_file), 
                                self.sos_bucket, 
                                f"{self.run_type}/{vers}/{self.sos_file.name}",
                                ExtraArgs={"ServerSideEncryption": "aws:kms"})
        print(f"Uploaded: {self.run_type}/{vers}/{self.sos_file.name}")
        
        # Upload to PO.DAAC bucket
        if self.podaac_update:
            sos_filename = f"{self.continent}_sword_{self.swordversion}_SOS_priors_{self.run_type}_{vers}_{self.run_date.strftime('%Y%m%dT%H%M%S')}.nc"
            response = s3.upload_file(str(self.sos_file), 
                                      self.podaac_bucket, 
                                      sos_filename,
                                      ExtraArgs={"ServerSideEncryption": "AES256"})
            print(f"Uploaded: {self.podaac_bucket}/{sos_filename}")

def set_variable_atts(variable, variable_dict):
        """Set the variable attribute metdata."""
        
        for name, value in variable_dict.items():
            setattr(variable, name, value)
                
def update_model(model_grp, metadata_json):
    """Update model metadata."""
    
    for name, variable in model_grp.variables.items():
        set_variable_atts(variable, metadata_json[name])
        
def update_historic_gauges(historicq_grp, metadata_json, continent):
    """Update historic gauge data metadata."""
    
    # Locate agencies for continent
    gauge_agencies = metadata_json["global_attributes_extra"]["continent_agency"][continent]
    
    # Update metadata for each gauge agency
    for gauge_agency in gauge_agencies:
        if gauge_agency == "GRDC": 
            gauge_grp = historicq_grp["grdc"]
        else:
            gauge_grp = historicq_grp[gauge_agency]
        for name, variable in gauge_grp.variables.items():
            set_variable_atts(variable, metadata_json[gauge_agency][name])

def fix_global_attributes(sos):
    """Fix global attributes to remove inconsistencies."""
    
    # Gage_Agency
    if "Gage_Agency" in sos.__dict__:
        value = sos.Gage_Agency
        del sos.Gage_Agency
        sos.gauge_agency = value
        
    # Name
    if "Name" in sos.__dict__:
        value = sos.Name
        del sos.Name
        sos.continent = value
        
    # Production date
    if "production_date" in sos.__dict__:
        value = sos.production_date
        del sos.production_date
        sos.date_created = datetime.strptime(value, '%d-%b-%Y %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
        
    if "version" in sos.__dict__:
        value = sos.version
        del sos.version
        sos.product_version = value

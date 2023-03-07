# Standard imports
from datetime import datetime
from pathlib import Path

# Third-party imports
import boto3
import logging
boto3.set_stream_logger("boto3.resources")
from boto3.session import Session
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
    SUFFIX: str
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

    SUFFIX = "_sword_v11_SOS_priors.nc"
    VERS_LENGTH = 4

    def __init__(self, continent, run_type, sos_dir):
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
        self.overwritten_indexes = np.array([])
        self.overwritten_source = np.array([])
        self.run_type = run_type
        self.sos_dir = sos_dir
        self.sos_file = None
        self.version = ""

    def copy_sos(self, fake_current):
        """Copy the latest version of the SoS file to local storage."""
        
        s3 = boto3.client("s3")
        object_list = s3.list_objects(Bucket="confluence-sos", Prefix=self.run_type)
        objects = [obj["Key"].split('/')[1] for obj in object_list["Contents"]]       

        dirs = list(set(objects))
        dirs.sort()
        print(f"Directories located for SoS: [{', '.join(dirs)}]")
        current = dirs[-1]

        if fake_current != 'foo':
            current = fake_current
        
        print(f"Locating: {self.run_type}/{current}/{self.continent}{self.SUFFIX}")
        response = s3.download_file(Bucket="confluence-sos", Key=f"{self.run_type}/{current}/{self.continent}{self.SUFFIX}", Filename=f"{self.sos_dir}/{self.continent}{self.SUFFIX}")
        print(f"Downloaded: {self.run_type}/{current}/{self.continent}{self.SUFFIX}")

    def create_new_version(self):
        """Create new version of the SoS file.
        
        Returns
        -------
        sos_file: pathlib.PosixPath
            Path to new SoS file on local storage
        """

        self.sos_file = Path(f"{str(self.sos_dir)}/{self.continent}{self.SUFFIX}")
        sos = Dataset(self.sos_file, 'a')
        self.last_run_time = datetime.strptime(sos.production_date.split(' ')[0], '%d-%b-%Y').strftime('%Y-%m-%d')
        # print(self.last_run_time)
        self.version = str(int(sos.version) + 1)
        padding = ['0'] * (self.VERS_LENGTH - len(self.version))
        sos.version = f"{''.join(padding)}{self.version}"
        sos.production_date = datetime.now().strftime('%d-%b-%Y %H:%M:%S')
        sos.close()
        print(f"Created version {''.join(padding)}{self.version} of: {self.sos_file.name}")

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
            historic_usgs_reach_ids = sos["historicQ"]["usgs"]["usgs_reach_id"][:]
            for index, rid in enumerate(historic_usgs_reach_ids):
                self._overwrite_prior(rid, sos, sos["historicQ"]["usgs"], "usgs")   

            # USGS
            usgs_reach_ids = sos["usgs"]["usgs_reach_id"][:]
            usgs_cal = sos["usgs"]["CAL"][:]
            for index, rid in enumerate(usgs_reach_ids) :
                # check for cal/val
                if usgs_cal[index] == 1:
                    self._overwrite_prior(rid, sos, sos["usgs"], "usgs")

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


        self._create_dims_vars(sos)

        sos["model"]["overwritten_indexes"][:] = self.overwritten_indexes
        sos["model"]["overwritten_source"][:] = stringtochar(np.array(self.overwritten_source, dtype="S4"))
        
        sos["model"]["bad_priors"][:] = self.bad_prior
        sos["model"]["bad_prior_source"][:] = stringtochar(np.array(self.bad_prior_source, dtype="S4"))

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
        
        # check to see if more than one gauge was found
        if len(gage_index[0]) > 1:
            double_gauge = True 

            # find the mean q for each gauge
            gage_mean_q_list = [gage["mean_q"][i] for i in gage_index[0]]

            # in order to decide what one will replace the grades data, we find what guage had the closest to the prediction
            # this method of sorting could change
            winner_index = closest( gage_mean_q_list, sos["model"]["mean_q"][sos_index][0])

        else:
            double_gauge = False

        grades = sos["model"]
        if self._isvalid_q(gage, gage_index):
            if not double_gauge:
                grades["flow_duration_q"][sos_index] = gage["flow_duration_q"][gage_index]
                grades["max_q"][sos_index] = gage["max_q"][gage_index]
                grades["monthly_q"][sos_index] = gage["monthly_q"][gage_index]
                grades["mean_q"][sos_index] = gage["mean_q"][gage_index]
                grades["min_q"][sos_index] = gage["min_q"][gage_index]
                grades["two_year_return_q"][sos_index] = gage["two_year_return_q"][gage_index]
                self.overwritten_indexes[sos_index] = 1
                self.overwritten_source[sos_index] = source
            else:
                grades["flow_duration_q"][sos_index] = gage["flow_duration_q"][gage_index][winner_index]
                grades["max_q"][sos_index] = gage["max_q"][gage_index][winner_index]
                grades["monthly_q"][sos_index] = gage["monthly_q"][gage_index][winner_index]
                grades["mean_q"][sos_index] = gage["mean_q"][gage_index][winner_index]
                grades["min_q"][sos_index] = gage["min_q"][gage_index][winner_index]
                grades["two_year_return_q"][sos_index] = gage["two_year_return_q"][gage_index][winner_index]
                self.overwritten_indexes[sos_index] = 1
                self.overwritten_source[sos_index] = source

        else:
            self.bad_prior[sos_index] = 1
            self.bad_prior_source[sos_index] = source
        
    def _isvalid_q(self, gage, gage_index):
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
        
        keys = ["flow_duration_q", "max_q", "monthly_q", "mean_q", "min_q", "two_year_return_q"]
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
            oi = sos["model"].createVariable("overwritten_indexes", "i4", ("num_reaches",))
            oi.comment = "Indexes of GRADES priors that were overwritten."

        if "overwritten_source" not in sos["model"].variables:
            print(sos["model"].dimensions)
            if "nchar" not in sos["model"].dimensions:
                sos["model"].createDimension("nchar", 4)
            os = sos["model"].createVariable("overwritten_source", "S1", ("num_reaches", "nchar"))
            os.comment = "Source of gage data that overwrote GRADES priors."
        
        if "bad_priors" not in sos["model"].variables:
            bp = sos["model"].createVariable("bad_priors", "i4", ("num_reaches",))
            bp.comment = "Indexes of invalid gage priors that were not overwritten."

        if "bad_prior_source" not in sos["model"].variables:
            bps = sos["model"].createVariable("bad_prior_source", "S1", ("num_reaches", "nchar"))
            bps.comment = "Source of invalid gage priors."


    def upload_file(self):
        """Upload SoS file to S3 bucket."""

        sos_ds = Dataset(self.sos_file, 'r')
        vers = sos_ds.version
        sos_ds.close()

        s3 = boto3.client("s3")
        response = s3.upload_file(str(self.sos_file), "confluence-sos", f"{self.run_type}/{vers}/{self.sos_file.name}")
        print(f"Uploaded: {self.run_type}/{vers}/{self.sos_file.name}")
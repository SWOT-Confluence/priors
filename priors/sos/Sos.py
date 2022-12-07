# Standard imports
from datetime import datetime, timezone
import glob
from os import scandir
from pathlib import Path
from shutil import copy
import warnings


# Third-party imports
import boto3
from boto3.session import Session
from netCDF4 import Dataset, stringtochar
import numpy as np
import s3fs


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
    confluence_fs: S3FileSystem
        references Confluence S3 buckets
    continent: str
        continent abbreviation to id SoS file
    MOD_TIME: int
        nunber of seconds since last modification 
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

    LOCAL_SOS = Path("")    # Path to temporary local SoS
    SUFFIX = "_sword_v11_SOS"
    MOD_TIME = 7200
    VERS_LENGTH = 4

    def __init__(self, continent, run_type, sos_dir, confluence_creds):
        """
        Parameters
        ----------
        continent: str
            continent abbreviation to id SoS file
        run_type: str
            'constrained' or 'unconstrained' data product type
        sos_dir: Path
            path to SoS directory on local storage
        confluence_creds: dict
            Dictionary of s3 credentials 
        """
        self.confluence_creds = confluence_creds
        self.bad_prior = np.array([])
        self.bad_prior_source = np.array([])
        self.confluence_fs = s3fs.S3FileSystem(key=confluence_creds["key"], 
            secret=confluence_creds["secret"], 
            client_kwargs={"region_name": confluence_creds["region"]})
        # self.confluence_fs = None   # Temporary local function
        self.continent = continent
        self.overwritten_indexes = np.array([])
        self.overwritten_source = np.array([])
        self.run_type = run_type
        self.sos_dir = sos_dir
        self.sos_file = None
        self.version = ""

    def copy_sos(self):
        """Copy the latest version of the SoS file to local storage."""

        session = Session(aws_access_key_id=self.confluence_creds['key'],
                        aws_secret_access_key=self.confluence_creds['secret'])
        s3 = session.resource('s3')
        bucket = s3.Bucket(name="confluence-sos")
        print([i for i in bucket.objects.filter(Prefix=f"{self.run_type}/")])
        dirs = list(set([str(obj.key).split('/')[1] for obj in bucket.objects.filter(Prefix=f"{self.run_type}/") if obj.key != "constrained/"]))
        dirs.sort()
        current = dirs[-1]
        obj = s3.Object(bucket_name="confluence-sos", key=f"{self.run_type}/{current}/{self.continent}{self.SUFFIX}_priors.nc")
        
        if current != "0000":
            try:
                if (datetime.now(timezone.utc) - obj.last_modified).seconds < self.MOD_TIME:
                    obj = self._locate_previous_version(dirs, current, s3)
            except s3.meta.client.exceptions.ClientError as error:
                obj = self._locate_previous_version(dirs, current, s3)
        
        print(f"Downloading: {obj.key}")
        obj.download_file(f"{self.sos_dir}/{self.continent}{self.SUFFIX}.nc")
        
    def _locate_previous_version(self, dirs, current, s3):
        """Locate the previous version of a file in the dirs list.
        
        Parameter
        ---------
        dirs: list
            list of string names of directories in S3  
        current: str
            name of current directory
        s3: boto3.resources.factory.s3.ServiceResource
            s3 resource that represents SoS bucket
        
        Returns
        -------
        s3 Object representing previous version of file
        """
        
        dirs.remove(current)
        return s3.Object(bucket_name="confluence-sos", key=f"{self.run_type}/{dirs[-1]}/{self.continent}{self.SUFFIX}_priors.nc")

    def copy_local(self):
        """Temporary copy local method."""

        sos_dir = self.LOCAL_SOS / self.run_type
        with scandir(sos_dir) as entries:
            dirs = [ entry.name for entry in entries ]
            curr_dir = max(dirs)
        file = glob.glob(f"{str(sos_dir / curr_dir)}/{self.continent}*.nc")[0]
        copy(file, self.sos_dir / f"{self.continent}{self.SUFFIX}.nc")

    def create_new_version(self):
        """Create new version of the SoS file.
        
        Returns
        -------
        sos_file: pathlib.PosixPath
            Path to new SoS file on local storage
        """

        self.sos_file = Path(f"{str(self.sos_dir)}/{self.continent}{self.SUFFIX}.nc")
        print(f"Creating new version of: {self.sos_file}")
        sos = Dataset(self.sos_file, 'a')
        self.last_run_time = datetime.strptime(sos.production_date.split(' ')[0], '%d-%b-%Y').strftime('%Y-%m-%d')
        print(self.last_run_time)

        self.version = str(int(sos.version) + 1)
        padding = ['0'] * (self.VERS_LENGTH - len(self.version))
        sos.version = f"{''.join(padding)}{self.version}"
        sos.production_date = datetime.now().strftime('%d-%b-%Y %H:%M:%S')

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


            defra_reach_ids = sos["MLIT"]["MLIT_reach_id"][:]
            defra_cal = sos["MLIT"]["CAL"][:]
            for index, rid in enumerate(defra_reach_ids) :
                # check for cal/val
                if defra_cal[index] == 1: 
                    self._overwrite_prior(rid, sos, sos["MLIT"], "MLIT")
        
        # if self.continent == 'sa':

        #     # Historic MLIT
        #     historic_wsc_reach_ids = sos["historicQ"]["MLIT"]["MLIT_reach_id"][:]
        #     for index, rid in enumerate(historic_wsc_reach_ids):
        #         self._overwrite_prior(rid, sos, sos["historicQ"]["MLIT"], "MLIT")  


        #     defra_reach_ids = sos["MLIT"]["MLIT_reach_id"][:]
        #     defra_cal = sos["MLIT"]["CAL"][:]
        #     for index, rid in enumerate(defra_reach_ids) :
        #         # check for cal/val
        #         if defra_cal[index] == 1: 
        #             self._overwrite_prior(rid, sos, sos["MLIT"], "MLIT")


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
        self.confluence_fs.put(str(self.sos_file), f"confluence-sos/{self.run_type}/{vers}/{self.sos_file.stem}_priors.nc")

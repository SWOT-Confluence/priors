# Standard imports
from datetime import datetime
import glob
from os import scandir
from pathlib import Path
from shutil import copy

# Third-party imports
from netCDF4 import Dataset, stringtochar
import numpy as np
import s3fs

# Local imports
from priors.sos.conf import confluence_creds

class Sos:
    """Class that represents the SoS and required ops to create a new version.
    
    Attributes
    ----------
    confluence_fs: S3FileSystem
        references Confluence S3 buckets
    continent: str
        continent abbreviation to id SoS file
    overwritten_indexes: list
        list of integer index values where grades data was overwritten
    overwritten_source: list
        list of either 'usgs' or 'grdc' to indicate source of overwritten data
    run_type: str
        'constrained' or 'unconstrained' data product type
    sos_dir: Path
        path to SoS directory on local storage
    sos_file: Path
        path to SoS file
    VERS_LENGTH: int
        number of integers in SoS identifier
    version: str
        current version of the SoS (for current execution)
    
    Methods
    -------
    copy_sos()
        Copy the latest version of the SoS to local storage
    create_new_version(priors_list)
        Creates a new version of the SoS with updated priors
    upload_new_version()
        Uploads new version to Confluence S3 bucket
    """

    LOCAL_SOS = Path("/home/nikki/Documents/confluence/data/sos/sos")    # Path to temporary local SoS
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

        self.confluence_fs = s3fs.S3FileSystem(key=confluence_creds["key"], 
            secret=confluence_creds["secret"], 
            client_kwargs={"region_name": confluence_creds["region"]})
        # self.confluence_fs = None   # Temporary local function
        self.continent = continent
        self.overwritten_indexes = []
        self.overwritten_source = []
        self.run_type = run_type
        self.sos_dir = sos_dir
        self.sos_file = None
        self.version = ""

    def copy_sos(self):
        """Copy the latest version of the SoS file to local storage."""
        
        dirs = self.confluence_fs.ls(f"confluence-sos/{self.run_type}")
        curr_dir = max(dirs)
        file = self.confluence_fs.glob(f"{curr_dir}/{self.continent}*.nc")[0]
        self.confluence_fs.download(file, f"{str(self.sos_dir)}/{file.split('/')[-1]}")

    def copy_local(self):
        """Temporary copy local method."""

        sos_dir = self.LOCAL_SOS / self.run_type
        with scandir(sos_dir) as entries:
            dirs = [ entry.name for entry in entries ]
            curr_dir = max(dirs)
        file = glob.glob(f"{str(sos_dir / curr_dir)}/{self.continent}*.nc")[0]
        copy(file, self.sos_dir)

    def create_new_version(self):
        """Create new version of the SoS file.
        
        Returns
        -------
        sos_file: pathlib.PosixPath
            Path to new SoS file on local storage
        """

        self.sos_file = Path(glob.glob(f"{str(self.sos_dir)}/{self.continent}*.nc")[0])
        sos = Dataset(self.sos_file, 'a')

        self.version = str(int(sos.version) + 1)
        padding = ['0'] * (self.VERS_LENGTH - len(self.version))
        sos.version = f"{''.join(padding)}{self.version}"
        sos.production_date = datetime.now().strftime('%d-%b-%Y %H:%M:%S')

        sos.close()

    def overwrite_grades(self):
        """Overwrite GRADES data with gaged (USGS or GRDC) data in the SoS."""

        sos = Dataset(self.sos_file, 'a')

        grdc_reach_ids = sos["model"]["grdc"]["grdc_reach_id"][:]
        for rid in grdc_reach_ids:
            self._overwrite_prior(rid, sos, "grdc")

        if self.continent == "na":
            usgs_reach_ids = sos["model"]["usgs"]["usgs_reach_id"][:]
            for rid in usgs_reach_ids:
                self._overwrite_prior(rid, sos, "usgs")
        
        if sos.version == "0001":
            self._create_dims_vars(sos)

        sos["model"]["overwritten_indexes"][:] = self.overwritten_indexes
        sos["model"]["overwritten_source"][:] = stringtochar(np.array(self.overwritten_source, dtype="S4"))

        sos.close()

    def _overwrite_prior(self, reach_id, sos, source):
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
        gage_index = np.where(reach_id == sos["model"][source][f"{source}_reach_id"][:])
        
        grades = sos["model"]
        gage = sos["model"][source]
        grades["flow_duration_q"][sos_index] = gage["flow_duration_q"][gage_index]
        grades["max_q"][sos_index] = gage["max_q"][gage_index]
        grades["monthly_q"][sos_index] = gage["monthly_q"][gage_index]
        grades["mean_q"][sos_index] = gage["mean_q"][gage_index]
        grades["min_q"][sos_index] = gage["min_q"][gage_index]
        grades["two_year_return_q"][sos_index] = gage["two_year_return_q"][gage_index]

        self.overwritten_indexes.append(sos_index[0][0])
        self.overwritten_source.append(source)

    def _create_dims_vars(self, sos):
        """Create dimensions and variables to track overwritten data.
        
        Parameters
        ----------
        sos: netCDF4._netCDF4.Dataset
            sos NetCDF Dataset
        """

        sos["model"].createDimension("num_overwritten", None)
        sos["model"].createDimension("nchar", 4)

        oi = sos["model"].createVariable("overwritten_indexes", "i4", ("num_overwritten",))
        oi.comment = "Indexes of GRADES priors that were overwritten."
        os = sos["model"].createVariable("overwritten_source", "S1", ("num_overwritten", "nchar"))
        os.comment = "Source of gage data that overwrote GRADES priors."

    def upload_file(self):
        """Upload SoS file to S3 bucket."""

        sos_ds = Dataset(self.sos_file, 'r')
        vers = sos_ds.version
        sos_ds.close()
        self.confluence_fs.put(str(self.sos_file), f"confluence-sos/{self.run_type}/{vers}/{self.sos_file.name}")

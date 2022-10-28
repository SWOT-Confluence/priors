"""Module to generate geoBAM priors and gaged priors for constrained run type. 

Module overwrites GRADES data with gaged data for constrained run type after 
uploading all new prior data to the Confluence S3 bucket. The module uses 
command line arguments to determine which priors to generate.

Command line arguments
----------------------
1 run_type (required): 'unconstrained' or 'constrained' 
2 grdc (optional): 'grdc'
3 usgs (optional): 'usgs'
4 gbpriors (optional): 'gbpriors'

Classes
-------
Priors

Functions
---------
main()
    main method to generate, retrieve, and overwrite priors
"""

# Standard imports
from datetime import datetime
import json
import os
from pathlib import Path
import sys

# Local imports
from priors.gbpriors.GBPriorsGenerate import GBPriorsGenerate
from priors.gbpriors.GBPriorsUpdate import GBPriorsUpdate
from priors.grdc.GRDC import GRDC
from priors.sos.Sos import Sos
from priors.usgs.USGSUpdate import USGSUpdate
from priors.usgs.USGSPull import USGSPull
from priors.Riggs.RiggsUpdate import RiggsUpdate
from priors.Riggs.RiggsPull import RiggsPull

# Constants
INPUT_DIR = Path("/mnt/data")

class Priors:
    """Class that coordinates the priors to be generated and stores them in 
    the SoS, uploads a new version to AWS, and overwrites gaged data.
    
    Attributes
    ----------
        cont: str
            Continent abbreviation
        run_type: str
            'constrained' or 'unconstrained' data product type
        priors_list: list
            list of strings which determine which priors to retrieve
        input_dir: Path
            path to input data directory
        sos_dir: Path
            path to SoS directory on local storage

    Methods
    -------
    update(run_type, priors_list)
        Generate and update priors based on arguments.

    """

    def __init__(self, cont, run_type, priors_list, input_dir, sos_dir, confluence_creds):
        """
        Parameters
        ----------
        cont: str
            Continent abbreviation
        run_type: str
            'constrained' or 'unconstrained' data product type
        priors_list: list
            list of strings which determine which priors to retrieve
        input_dir: Path
            path to input data directory
        sos_dir: Path
            path to SoS directory on local storage
        confluence_creds: dict
            Dictionary of s3 credentials            
        """

        self.cont = cont
        self.run_type = run_type
        self.priors_list = priors_list
        self.input_dir = input_dir
        self.sos_dir = sos_dir
        self.confluence_creds = confluence_creds

    def execute_gbpriors(self, sos_file):
        """Create and execute GBPriors operations.
        
        Parameters
        ----------
        sos_file: Path
            path to SOS file to update
        """

        gen = GBPriorsGenerate(sos_file, self.input_dir / "swot")
        gen.run_gb()
        app = GBPriorsUpdate(gen.gb_dict, sos_file)
        app.update_data()
    
    def execute_grdc(self, sos_file):
        """Create and execute GRDC operations.
        
        Parameters
        ----------
        sos_file: Path
            path to SOS file to update
        """

        grdc_file = self.input_dir / "gage" / "GRDC2SWORDout.nc"
        grdc = GRDC(sos_file, grdc_file)
        grdc.read_sos()
        grdc.read_grdc()
        grdc.map_data()
        grdc.update_data()

    def execute_usgs(self, sos_file):
        """Create and execute USGS operations.

        Parameters
        ----------
        sos_file: Path
            path to SOS file to update
        """

        usgs_file = self.input_dir / "gage" / "USGStargetsV5P.nc"
        today = datetime.today().strftime("%Y-%m-%d")

        #preparing for historical data update
        # today_last_year = today.replace(today[:4], str(int(today[:4])-1))
        today_last_year = '1980-1-1'
        usgs_pull = USGSPull(usgs_file, today_last_year, today)
        usgs_pull.pull()
        usgs_update = USGSUpdate(sos_file, usgs_pull.usgs_dict)
        usgs_update.read_sos()
        usgs_update.map_data()
        usgs_update.update_data()
        
    def execute_Riggs(self, sos_file):
        """Create and execute Riggs operations.

        Parameters
        ----------
        sos_file: Path
            path to SOS file to update
        """

        Riggs_file = self.input_dir / "gage" / "Rtarget"
        today = datetime.today().strftime("%Y-%m-%d")

        #preparing for historical data update
        # today_last_year = today.replace(today[:4], str(int(today[:4])-1))
        today_last_year = '1980-1-1'
        Riggs_pull = RiggsPull(Riggs_file, today_last_year, today)
        Riggs_pull.pull()
        Riggs_update = RiggsUpdate(sos_file, Riggs_pull.riggs_dict)
        Riggs_update.read_sos()
        Riggs_update.map_data()
        Riggs_update.update_data()

    def update(self):
        """Generate and update priors based on arguments."""

        # Create SoS object to manage SoS operations
        print("Copy and create new version of the SoS.")
        sos = Sos(self.cont, self.run_type, self.sos_dir, self.confluence_creds)
        sos.copy_sos()
        sos.create_new_version()
        sos_file = sos.sos_file

        # Determine run type and add requested gage priors
        if self.run_type == "constrained":
            if "grdc" in self.priors_list:
                print("Updating GRDC priors.")
                self.execute_grdc(sos_file)

            if "usgs" in self.priors_list and self.cont == "na":
                print("Updating USGS priors.")
                self.execute_usgs(sos_file)


            self.execute_Riggs(sos_file)
        
        # Add geoBAM priors if requested (for either data product)
        if "gbpriors" in self.priors_list:
            print("Updating geoBAM priors.")
            self.execute_gbpriors(sos_file)

        if self.run_type == "constrained":
            # Overwrite GRADES with gage priors
            print("Overwriting GRADES data with gaged priors.")
            sos.overwrite_grades()


        # # Upload priors results to S3 bucket
        # print("Uploading new SoS priors version.")
        # sos.upload_file()


def main():
    """Main method to generate, retrieve, and overwrite priors."""

    # Store command line arguments
    try:
        s3_creds_filename = sys.argv[1]
        run_type = sys.argv[2]
        prior_ops = sys.argv[3:]
        print(f"Running on {run_type} data product and pulling the following: {', '.join(prior_ops)}")
    except IndexError:
        print("Please enter appropriate command line arguments which MUST include run_type.")
        print("Program exit.")
        sys.exit(1)

    # Get continent to run on
    index = int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))
    with open(INPUT_DIR / "continent.json") as jsonfile:
        cont = list(json.load(jsonfile)[index].keys())[0]

    # Get s3 creds for SoS upload
    with open(INPUT_DIR / s3_creds_filename) as jsonfile:
        confluence_creds = json.load(jsonfile)

    # Retrieve and update priors
    priors = Priors(cont, run_type, prior_ops, INPUT_DIR, INPUT_DIR / "sos", confluence_creds)
    priors.update()

if __name__ == "__main__":
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"Execution time: {end - start}")
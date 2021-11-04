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

    def __init__(self, cont, run_type, priors_list, input_dir, sos_dir):
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
        """

        self.cont = cont
        self.run_type = run_type
        self.priors_list = priors_list
        self.input_dir = input_dir
        self.sos_dir = sos_dir

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

        usgs_file = self.input_dir / "gage" / "USGStargetsV3.nc"
        usgs_pull = USGSPull(usgs_file, '1980-1-1', datetime.today().strftime("%Y-%m-%d"))
        usgs_pull.pull()
        usgs_update = USGSUpdate(sos_file, usgs_pull.usgs_dict)
        usgs_update.read_sos()
        usgs_update.map_data()
        usgs_update.update_data()

    def update(self):
        """Generate and update priors based on arguments."""

        # Create SoS object to manage SoS operations
        sos = Sos(self.cont, self.run_type, self.sos_dir)
        sos.copy_sos()
        sos.create_new_version()
        sos_file = sos.sos_file

        # Determine run type and add requested gage priors
        if self.run_type == "constrained":
            if "grdc" in self.priors_list:
                self.execute_grdc(sos_file)

            if "usgs" in self.priors_list and self.cont == "na":
                self.execute_usgs(sos_file)

        # Add geoBAM priors if requested (for either data product)
        if "gbpriors" in self.priors_list:
            self.execute_gbpriors(sos_file)

        # Upload priors results to S3 bucket
        sos.upload_file()

        # Overwrite GRADES with gage priors
        sos.overwrite_grades()

def main():
    """Main method to generate, retrieve, and overwrite priors."""

    # Store command line arguments
    try:
        run_type = sys.argv[1]
        prior_ops = sys.argv[2:]
    except IndexError:
        print("Please enter appropriate command line arguments which MUST include run_type.")
        print("Program exit.")
        sys.exit(1)

    # Get continent to run on
    index = int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))
    with open(INPUT_DIR / "continent.json") as jsonfile:
        cont = list(json.load(jsonfile)[index].keys())[0]

    # Retrieve and update priors
    priors = Priors(cont, run_type, prior_ops, INPUT_DIR, INPUT_DIR / "sos")
    priors.update()

if __name__ == "__main__":
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"Execution time: {end - start}")
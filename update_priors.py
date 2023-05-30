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
import argparse
from datetime import datetime
import json
import os
from pathlib import Path

# Local imports
from priors.gbpriors.GBPriorsGenerate import GBPriorsGenerate
from priors.gbpriors.GBPriorsUpdate import GBPriorsUpdate
from priors.grdc.GRDC import GRDC
from priors.sos.Sos import Sos
from priors.usgs.USGSUpdate import USGSUpdate
from priors.usgs.USGSPull import USGSPull
from priors.Riggs.RiggsUpdate import RiggsUpdate
from priors.Riggs.RiggsPull import RiggsPull

# Third-party imports
import botocore

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

    def __init__(self, cont, run_type, priors_list, input_dir, sos_dir, fake_current):
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
        self.fake_current = fake_current

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

    def execute_usgs(self, sos_file, start_date):
        """Create and execute USGS operations.

        Parameters
        ----------
        sos_file: Path
            path to SOS file to update
        """

        usgs_file = self.input_dir / "gage" / "USGStargetsV7_.nc"
        today = datetime.today().strftime('%Y-%m-%d')
        usgs_pull = USGSPull(usgs_targets = usgs_file, start_date = start_date, end_date = today, sos_file = sos_file)
        usgs_pull.pull()
        usgs_update = USGSUpdate(sos_file, usgs_pull.usgs_dict)
        usgs_update.read_sos()
        usgs_update.map_data()
        usgs_update.update_data()
        
    def execute_Riggs(self, sos_file, start_date):
        """Create and execute Riggs operations.

        Parameters
        ----------
        sos_file: Path
            path to SOS file to update
        """
        Riggs_file = self.input_dir / "gage" / "Rtarget"
        today = datetime.today().strftime("%Y-%m-%d")
        Riggs_pull = RiggsPull(riggs_targets=Riggs_file, start_date=start_date, end_date=today, cont = self.cont,  sos_file = sos_file)
        Riggs_pull.pull()
        Riggs_update = RiggsUpdate(sos_file, Riggs_pull.riggs_dict)
        Riggs_update.read_sos()
        Riggs_update.map_data()
        Riggs_update.update_data()

    def update(self):
        """Generate and update priors based on arguments."""

        # Create SoS object to manage SoS operations
        print("Copy and create new version of the SoS.")
        sos = Sos(self.cont, self.run_type, self.sos_dir)
        try:
            sos.copy_sos(self.fake_current)
        except botocore.exceptions.ClientError:
            print("Exiting program.")
            exit(1)
        sos.create_new_version()
        sos_file = sos.sos_file
        sos_last_run_time = sos.last_run_time

        # Determine run type and add requested gage priors
        # removed constrained run logic check as both unconstrained and constrained now pull gauge data
        # in the future we should write the gauge data to a separate nc file for both

        if "grdc" in self.priors_list:
            print("Updating GRDC priors.")
            self.execute_grdc(sos_file)

        if "usgs" in self.priors_list and self.cont == "na":
            print("Updating USGS priors.")
            self.execute_usgs(sos_file, start_date = '1980-1-1')

        # adding na to this list for now to avoid canada integration
        if 'riggs' in self.priors_list and self.cont not in ['af', 'as', 'na']:
            # riggs modules are having problems with downloading just the delta
            # change start date to sos_last_run_time to continue development
            self.execute_Riggs(sos_file, start_date = '1980-1-1')
        
        # Add geoBAM priors if requested (for either data product)
        if "gbpriors" in self.priors_list:
            print("Updating geoBAM priors.")
            self.execute_gbpriors(sos_file)

        # only overwrite if doing a constrained run
        if self.run_type == "constrained":
            # Overwrite GRADES with gage priors
            print("Overwriting GRADES data with gaged priors.")
            sos.overwrite_grades()


        # Upload priors results to S3 bucket
        print("Uploading new SoS priors version.")
        sos.upload_file()

def create_args():
    """Create and return argparser with arguments."""

    arg_parser = argparse.ArgumentParser(description="Update Confluence SoS priors.")
    arg_parser.add_argument("-i",
                            "--index",
                            type=int,
                            help="Index value to select continent to run on")
    arg_parser.add_argument("-r",
                            "--runtype",
                            type=str,
                            choices=["constrained", "unconstrained"],
                            help="Indicates what type of run to generate priors for.",
                            default="constrained")
    arg_parser.add_argument("-p",
                            "--priors",
                            type=str,
                            nargs="+",
                            default=[],
                            help="List: usgs, grdc, riggs, gbpriors")

    arg_parser.add_argument("-l",
                            "--level",
                            type=str,
                            default='foo',
                            help="Forces priors to pull a certain level sos ex: 0000")
    return arg_parser

def main():
    """Main method to generate, retrieve, and overwrite priors."""

    # Store command line arguments
    arg_parser = create_args()
    args = arg_parser.parse_args()
    print(f"Index: {args.index}")
    print(f"Run type: {args.runtype}")
    if len(args.priors) > 0: print(f"Priors: {', '.join(args.priors)}")

    # Get continent to run on
    i = int(args.index) if args.index != -235 else int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))
    with open(INPUT_DIR / "continent.json") as jsonfile:
        cont = list(json.load(jsonfile)[i].keys())[0]

    # Retrieve and update priors
    priors = Priors(cont, args.runtype, args.priors, INPUT_DIR, INPUT_DIR / "sos", args.level)
    priors.update()

if __name__ == "__main__":
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"Execution time: {end - start}")
"""Module to generate geoBAM priors and gaged priors for constrained run type. 

Module overwrites GRADES data with gaged data for constrained run type after 
uploading all new prior data to the Confluence S3 bucket. The module uses 
command line arguments to determine which priors to generate.

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
import datetime
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
import numpy as np

# Constants
INPUT_DIR = Path("/mnt/data")
SWORD_VERSION = "v15"

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

    def __init__(self, cont, run_type, priors_list, input_dir, sos_dir, 
                 fake_current, metadata_json, historic_qt, add_geospatial, 
                 podaac_update, podaac_bucket, sos_bucket="confluence-sos"):
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
        self.metadata_json = metadata_json
        self.time_dict = {
            "historic_qt": historic_qt[cont]
        }
        self.add_geospatial = add_geospatial
        self.podaac_update = podaac_update
        self.podaac_bucket = podaac_bucket
        self.sos_bucket = sos_bucket

    def execute_gbpriors(self, sos_file):
        """Create and execute GBPriors operations.
        
        Parameters
        ----------
        sos_file: Path
            path to SOS file to update
        """

        gen = GBPriorsGenerate(sos_file, self.input_dir / "swot")
        gen.run_gb()
        app = GBPriorsUpdate(gen.gb_dict, sos_file, metadata_json = self.metadata_json)
        app.update_data()
        return gen.swot_time
    
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
        return grdc.map_dict["grdc_qt"]

    def execute_usgs(self, sos_file, start_date):
        """Create and execute USGS operations.

        Parameters
        ----------
        sos_file: Path
            path to SOS file to update
        """

        usgs_file = self.input_dir / "gage" / "USGStargetsV7_.nc"
        today = datetime.datetime.today().strftime('%Y-%m-%d')
        usgs_pull = USGSPull(usgs_targets = usgs_file, start_date = start_date, end_date = today, sos_file = sos_file)
        usgs_pull.pull()
        usgs_update = USGSUpdate(sos_file, usgs_pull.usgs_dict, metadata_json = self.metadata_json)
        usgs_update.read_sos()
        usgs_update.map_data()
        usgs_update.update_data()
        return usgs_update.map_dict["usgs_qt"]
        
    def execute_Riggs(self, sos_file, start_date):
        """Create and execute Riggs operations.

        Parameters
        ----------
        sos_file: Path
            path to SOS file to update
        """
        Riggs_file = self.input_dir / "gage" / "Rtarget"
        today = datetime.datetime.today().strftime("%Y-%m-%d")
        Riggs_pull = RiggsPull(riggs_targets=Riggs_file, start_date=start_date, end_date=today, cont = self.cont,  sos_file = sos_file)
        Riggs_pull.pull()
        Riggs_update = RiggsUpdate(sos_file, Riggs_pull.riggs_dict, metadata_json = self.metadata_json)
        Riggs_update.read_sos()
        Riggs_update.map_data()
        Riggs_update.update_data()
        
        # Retrieve time data
        time_dict = {}
        for agency in set(list(Riggs_update.Riggs_dict["Agency"])):
            time_dict[agency] = Riggs_update.map_dict[agency]["Riggs_qt"]
        return time_dict
        
    def locate_min_max(self):
        """Locate min and max time values."""
        
        min_qt = datetime.datetime(1965,1,1,0,0,0)
        max_qt = datetime.datetime(1965,1,1,0,0,0)
        
        # Extract min and max from each prior
        swot_ts = datetime.datetime(2000,1,1,0,0,0)
        for agency, time in self.time_dict.items():
            
            # Historic gage time
            if agency == "historic_qt":
                for data in time.values():
                    data_min = datetime.datetime.fromordinal(data["min"])
                    data_max = datetime.datetime.fromordinal(data["max"])
                    if data_min < min_qt or min_qt == datetime.datetime(1965,1,1,0,0,0):
                        min_qt = data_min
                    if data_max > max_qt or max_qt == datetime.datetime(1965,1,1,0,0,0):
                        max_qt = data_max
                continue
            
            # GeoBAM priors SWOT time
            if agency == "gbpriors" and len(time) > 0:
                gb_min = swot_ts + datetime.timedelta(seconds=np.nanmin(time))
                gb_max = swot_ts + datetime.timedelta(seconds=np.nanmax(time))
                if gb_min < min_qt or min_qt == datetime.datetime(1965,1,1,0,0,0):
                    min_qt = gb_min
                if gb_max > max_qt or max_qt == datetime.datetime(1965,1,1,0,0,0):
                    max_qt = gb_max
                continue
            
            # All other gage agencies
            if not np.isnan(time).all():
                time_min = datetime.datetime.fromordinal(int(np.nanmin(time)))
                time_max = datetime.datetime.fromordinal(int(np.nanmax(time)))
                if time_min < min_qt or min_qt == datetime.datetime(1965,1,1,0,0,0):
                    min_qt = time_min
                if time_max > max_qt or max_qt == datetime.datetime(1965,1,1,0,0,0):
                    max_qt = time_max
        
        # Convert time to string data
        if min_qt == datetime.datetime(1965,1,1,0,0,0):
            min_qt = "NO TIME DATA"
        else:
            min_qt = min_qt
            
        if max_qt == datetime.datetime(1965,1,1,0,0,0):
            max_qt = "NO TIME DATA"
        else:
            max_qt = max_qt
        
        return min_qt, max_qt
    
    def update(self):
        """Generate and update priors based on arguments."""

        # Create SoS object to manage SoS operations
        print("Copy and create new version of the SoS.")
        sos = Sos(self.cont, self.run_type, self.sos_dir, self.metadata_json, 
                  self.priors_list, self.podaac_update, self.podaac_bucket,
                  self.sos_bucket)
        try:
            sos.copy_sos(self.fake_current)
        except Exception as e:
            print(e)
            exit(1)
        # except botocore.exceptions.ClientError:
        #     print(botocore.exceptions.ClientError)
        #     print("Exiting program.")
        #     exit(1)
        sos.create_new_version()
        sos_file = sos.sos_file
        sos_last_run_time = sos.last_run_time
        
        # Retrieve geospatial coverage - pull if true flag
        if self.add_geospatial:
            sos.store_geospatial_data(INPUT_DIR / "sword" / f"{self.cont}_sword_{SWORD_VERSION}.nc")
            print('Set geospatial coverage for reaches and nodes including maximum and minimum coverage.')

        # Determine run type and add requested gage priors
        # removed constrained run logic check as both unconstrained and constrained now pull gauge data
        # in the future we should write the gauge data to a separate nc file for both

        if "grdc" in self.priors_list:
            print("Updating GRDC priors.")
            self.execute_grdc(sos_file)

        if "usgs" in self.priors_list and self.cont == "na":
            print("Updating USGS priors.")
            self.time_dict["usgs"] = self.execute_usgs(sos_file, start_date = '1980-1-1')

        # adding na to this list for now to avoid canada integration
        if 'riggs' in self.priors_list and self.cont not in ['af', 'as']:
            # riggs modules are having problems with downloading just the delta
            # change start date to sos_last_run_time to continue development
            self.time_dict.update(self.execute_Riggs(sos_file, start_date = '1980-1-1'))
        
        # Add geoBAM priors if requested (for either data product)
        if "gbpriors" in self.priors_list:
            print("Updating geoBAM priors.")
            self.time_dict["gbpriors"] = self.execute_gbpriors(sos_file)
        
        # only overwrite if doing a constrained run
        if self.run_type == "constrained":
            # Overwrite GRADES with gage priors
            print("Overwriting GRADES data with gaged priors.")
            sos.overwrite_grades()
        
        # Update time coverage in sos file global attributes
        min_qt, max_qt = self.locate_min_max()
        sos.update_time_coverage(min_qt, max_qt)
        print(f'Updated time coverage of the SoS: {min_qt.strftime("%Y-%m-%dT%H:%M:%S")} to {max_qt.strftime("%Y-%m-%dT%H:%M:%S")}')

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
    arg_parser.add_argument("-m",
                            "--metadatajson",
                            type=Path,
                            default=Path(__file__).parent / "metadata" / "metadata.json",
                            help="Path to JSON file that contains global attribute values")
    arg_parser.add_argument("-qt",
                            "--historicqt",
                            type=Path,
                            default=Path(__file__).parent / "metadata" / "historicQt.json",
                            help="Path to JSON file that contains historic timestamps for discharge from gage agencies")
    arg_parser.add_argument("-g",
                            "--addgeospatial",
                            action="store_true",
                            help="Indicate requirement to add geospatial data")
    arg_parser.add_argument("-u",
                            "--podaacupload",
                            action="store_true",
                            help="Indicate requirement to upload to PO.DAAC S3 Bucket")
    arg_parser.add_argument("-b",
                            "--podaacbucket",
                            type=str,
                            help="Name of PO.DAAC S3 bucket to upload to")
    arg_parser.add_argument("-s",
                            "--sosbucket",
                            type=str,
                            default="confluence-sos",
                            help="Name of SoS S3 bucket to upload to")
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
        
    # Load metadata JSON
    with open(args.metadatajson) as jf:
        variable_atts = json.load(jf)
        
    # Load historic q timestamps
    with open(args.historicqt) as jf:
        historicqt = json.load(jf)

    # Retrieve and update priors
    priors = Priors(cont, args.runtype, args.priors, 
                    INPUT_DIR, INPUT_DIR / "sos", args.level, variable_atts, 
                    historicqt, args.addgeospatial, args.podaacupload,
                    args.podaacbucket, args.sosbucket)
    priors.update()

if __name__ == "__main__":
    start = datetime.datetime.now()
    main()
    end = datetime.datetime.now()
    print(f"Execution time: {end - start}")
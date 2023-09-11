"""Module that extracts SoS prior and SWOT observation data to generate
geoBAM priors.

Class
-----
GBPriorsGenerate: Class that generates and stores geoBAM priors

Functions
---------
 check_obs_node(width, d_x_area, slope2, qhat)
        Check observations to determine if data is valid
check_obs_reach(width, d_x_area, slope2, qhat)
    Check observations to determine if data is valid
extract_sos()
        Reads in data from the SoS and returns dictionary of data
extract_swot()
    Extracts and returns SWOT data stored in a dictionary
is_valid_node(obs)
    Determines if observations are valid
is_valid_reach(obs)
    Determines if observations are valid
"""

# Standard imports
import glob
from pathlib import Path

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from priors.gbpriors.GB import GB

class GBPriorsGenerate:
    """Class that generates and stores geoBAM priors.
    
    Attributes
    ----------
    CONT_DICT: dict
        dictionary of continental abbreviations and associated numbers
    FLOAT_FILL: float
        Fill value for any missing float values in mapped WBM data
    gb_dict: dict
        dictionary of GeoBAM priors organized by continent
    INT_FILL: int
        Fill value for any missing integer values in mapped WBM data
    sos_dict: dict
        dictionary of SoS data organized by continent
    swot_dir: Path
        path to directory that contains SWOT NetCDF files

    Methods
    -------
    __create_node_temp_dict(num_reaches, num_nodes)
        Create a dict to initialize numpy arrays for node-level priors
    __create_temp_reach_dict(num_reaches)
        Create a dict to initialize numpy arrays for reach-level priors
    __extract_node_priors(priors, prior_dict, rch_i, nod_i )
        Extract priors and store them in p_dict by index
    __extract_reach_priors(priors, prior_dict, index)
        Extract priors and store them in reach key of the prior_dict by index
    __insert_invalid_float(prior, invalid_indexes)
        Insert NaN value at invalid indezes in river_priors array
    __insert_invalid_int(prior, invalid_indexes)
        Insert NaN value at invalid indezes in river_priors array
    run_gb()
        Executes geoBAM on reach data and stores priors in sos_dict attribute
    """

    FlOAT_FILL = -999999999999
    INT_FILL = -999
    CONT_DICT = { "af" : [1], "as" : [4, 3], "eu" : [2], "na" : [7, 8, 9],
        "oc" : [5], "sa" : [6] }
    

    def __init__(self, sos_file, swot_dir):
        """
        Parameters
        ----------
        sos_file: Path
            path to SoS file
        swot_dir: Path
            path to directory that contains SWOT NetCDF files
        """
        
        self.gb_dict = {}
        self.sos_file = sos_file
        self.sos_dict = extract_sos(sos_file)
        self.swot_dir = swot_dir

    def __create_node_temp_dict(self, num_nodes):
        """Create a dict to initialize numpy arrays for node-level priors."""
        
        sos = Dataset(self.sos_file, 'r')
        gbpriors = sos["gbpriors"]["node"]
        node_dict = {
            "river_type" : gbpriors["river_type"][:].filled(self.INT_FILL),
            "lowerbound_A0" : gbpriors["lowerbound_A0"][:].filled(np.nan),
            "upperbound_A0" : gbpriors["upperbound_A0"][:].filled(np.nan),
            "lowerbound_logn" : gbpriors["lowerbound_logn"][:].filled(np.nan),
            "upperbound_logn" : gbpriors["upperbound_logn"][:].filled(np.nan),
            "lowerbound_b" : gbpriors["lowerbound_b"][:].filled(np.nan),
            "upperbound_b" : gbpriors["upperbound_b"][:].filled(np.nan),
            "lowerbound_logWb" : gbpriors["lowerbound_logWb"][:].filled(np.nan),
            "upperbound_logWb" : gbpriors["upperbound_logWb"][:].filled(np.nan),
            "lowerbound_logDb" : gbpriors["lowerbound_logDb"][:].filled(np.nan),
            "upperbound_logDb" : gbpriors["upperbound_logDb"][:].filled(np.nan),
            "lowerbound_logr" : gbpriors["lowerbound_logr"][:].filled(np.nan),
            "upperbound_logr" : gbpriors["upperbound_logr"][:].filled(np.nan),
            "logA0_hat" : gbpriors["logA0_hat"][:].filled(np.nan),
            "logn_hat" : gbpriors["logn_hat"][:].filled(np.nan),
            "b_hat" : gbpriors["b_hat"][:].filled(np.nan),
            "logWb_hat" : gbpriors["logWb_hat"][:].filled(np.nan),
            "logDb_hat" : gbpriors["logDb_hat"][:].filled(np.nan),
            "logr_hat" : gbpriors["logr_hat"][:].filled(np.nan),
            "logA0_sd" : gbpriors["logA0_sd"][:].filled(np.nan),
            "logn_sd" : gbpriors["logn_sd"][:].filled(np.nan),
            "b_sd" : gbpriors["b_sd"][:].filled(np.nan),
            "logWb_sd" : gbpriors["logWb_sd"][:].filled(np.nan),
            "logDb_sd" : gbpriors["logDb_sd"][:].filled(np.nan),
            "logr_sd" : gbpriors["logr_sd"][:].filled(np.nan),
            "lowerbound_logWc" : gbpriors["lowerbound_logWc"][:].filled(np.nan),
            "upperbound_logWc" : gbpriors["upperbound_logWc"][:].filled(np.nan),
            "lowerbound_logQc" : gbpriors["lowerbound_logQc"][:].filled(np.nan),
            "upperbound_logQc" : gbpriors["upperbound_logQc"][:].filled(np.nan),
            "logWc_hat" : gbpriors["logWc_hat"][:].filled(np.nan),
            "logQc_hat" : gbpriors["logQc_hat"][:].filled(np.nan),
            "logQ_sd" : gbpriors["logQ_sd"][:].filled(np.nan),
            "logWc_sd" : gbpriors["logWc_sd"][:].filled(np.nan),
            "logQc_sd" : gbpriors["logQc_sd"][:].filled(np.nan),
            "Werr_sd" : gbpriors["Werr_sd"][:].filled(np.nan),
            "Serr_sd" : gbpriors["Serr_sd"][:].filled(np.nan),
            "dAerr_sd" : gbpriors["dAerr_sd"][:].filled(np.nan),
            "sigma_man" : gbpriors["sigma_man"][:].filled(np.nan),
            "sigma_amhg" : gbpriors["sigma_amhg"][:].filled(np.nan),
            "overwritten_indexes": np.zeros((num_nodes), dtype=np.int32)
        }
        sos.close()
        return node_dict

    def __create_temp_reach_dict(self, num_reaches):
        """Create a dict to initialize numpy arrays for reach-level priors."""
        sos = Dataset(self.sos_file, 'r')
        gbpriors = sos["gbpriors"]["reach"]
        reach_dict = { 
            "river_type" : gbpriors["river_type"][:].filled(self.INT_FILL),
            "lowerbound_A0" : gbpriors["lowerbound_A0"][:].filled(np.nan),
            "upperbound_A0" : gbpriors["upperbound_A0"][:].filled(np.nan),
            "lowerbound_logn" : gbpriors["lowerbound_logn"][:].filled(np.nan),
            "upperbound_logn" : gbpriors["upperbound_logn"][:].filled(np.nan),
            "lowerbound_b" : gbpriors["lowerbound_b"][:].filled(np.nan),
            "lowerbound_b" : gbpriors["lowerbound_b"][:].filled(np.nan),
            "upperbound_b" : gbpriors["upperbound_b"][:].filled(np.nan),
            "lowerbound_logWb" : gbpriors["lowerbound_logWb"][:].filled(np.nan),
            "upperbound_logWb" : gbpriors["upperbound_logWb"][:].filled(np.nan),
            "lowerbound_logDb" : gbpriors["lowerbound_logDb"][:].filled(np.nan),
            "upperbound_logDb" : gbpriors["upperbound_logDb"][:].filled(np.nan),
            "lowerbound_logr" : gbpriors["lowerbound_logr"][:].filled(np.nan),
            "upperbound_logr" : gbpriors["upperbound_logr"][:].filled(np.nan),
            "logA0_hat" : gbpriors["logA0_hat"][:].filled(np.nan),
            "logn_hat" : gbpriors["logn_hat"][:].filled(np.nan),
            "b_hat" : gbpriors["b_hat"][:].filled(np.nan),
            "logWb_hat" : gbpriors["logWb_hat"][:].filled(np.nan),
            "logDb_hat" : gbpriors["logDb_hat"][:].filled(np.nan),
            "logr_hat" : gbpriors["logr_hat"][:].filled(np.nan),
            "logA0_sd" : gbpriors["logA0_sd"][:].filled(np.nan),
            "logn_sd" : gbpriors["logn_sd"][:].filled(np.nan),
            "b_sd" : gbpriors["b_sd"][:].filled(np.nan),
            "logWb_sd" : gbpriors["logWb_sd"][:].filled(np.nan),
            "logDb_sd" : gbpriors["logDb_sd"][:].filled(np.nan),
            "logr_sd" : gbpriors["logr_sd"][:].filled(np.nan),
            "lowerbound_logWc" : gbpriors["lowerbound_logWc"][:].filled(np.nan),
            "upperbound_logWc" : gbpriors["upperbound_logWc"][:].filled(np.nan),
            "lowerbound_logQc" : gbpriors["lowerbound_logQc"][:].filled(np.nan),
            "upperbound_logQc" : gbpriors["upperbound_logQc"][:].filled(np.nan),
            "logWc_hat" : gbpriors["logWc_hat"][:].filled(np.nan),
            "logQc_hat" : gbpriors["logQc_hat"][:].filled(np.nan),
            "logQ_sd" : gbpriors["logQ_sd"][:].filled(np.nan),
            "logWc_sd" : gbpriors["logWc_sd"][:].filled(np.nan),
            "logQc_sd" : gbpriors["logQc_sd"][:].filled(np.nan),
            "Werr_sd" : gbpriors["Werr_sd"][:].filled(np.nan),
            "Serr_sd" : gbpriors["Serr_sd"][:].filled(np.nan),
            "dAerr_sd" : gbpriors["dAerr_sd"][:].filled(np.nan),
            "sigma_man" : gbpriors["sigma_man"][:].filled(np.nan),
            "sigma_amhg" : gbpriors["sigma_amhg"][:].filled(np.nan),
            "overwritten_indexes": np.zeros((num_reaches), dtype=np.int32)
        }
        sos.close()
        return reach_dict

    def __extract_node_priors(self, priors, prior_dict, rch_i, nod_i, invalid):
        """Extract priors and store them in p_dict by index.
        
        Parameters
        ----------
        priors: rpy2.robjects.vectors.ListVector
            geoBAM priors stored in R named list
        prior_dict: dict
            dictionary of priors for a continent
        rch_i: int
            integer index to reach-level priors
        nod_i: int
            integer index to node-level priors
        invalid: list
            list of invalid index locations
        """

        prior_dict["river_type"][nod_i] = self.__insert_invalid_int(np.array(priors.rx2("River_Type")), invalid)
        
        river_priors = priors.rx2("river_type_priors")
        prior_dict["lowerbound_A0"][rch_i] = np.array(river_priors.rx2("lowerbound_A0"))
        prior_dict["upperbound_A0"][rch_i] = np.array(river_priors.rx2("upperbound_A0"))
        prior_dict["lowerbound_logn"][rch_i] = np.array(river_priors.rx2("lowerbound_logn"))
        prior_dict["upperbound_logn"][rch_i] = np.array(river_priors.rx2("upperbound_logn"))
        prior_dict["lowerbound_b"][rch_i] = np.array(river_priors.rx2("lowerbound_b"))
        prior_dict["upperbound_b"][rch_i] = np.array(river_priors.rx2("upperbound_b"))
        prior_dict["lowerbound_logWb"][rch_i] = np.array(river_priors.rx2("lowerbound_logWb"))
        prior_dict["upperbound_logWb"][rch_i] = np.array(river_priors.rx2("upperbound_logWb"))
        prior_dict["lowerbound_logDb"][rch_i] = np.array(river_priors.rx2("lowerbound_logDb"))
        prior_dict["upperbound_logDb"][rch_i] = np.array(river_priors.rx2("upperbound_logDb"))
        prior_dict["lowerbound_logr"][rch_i] = np.array(river_priors.rx2("lowerbound_logr"))
        prior_dict["upperbound_logr"][rch_i] = np.array(river_priors.rx2("upperbound_logr"))
        
        prior_dict["logA0_hat"][nod_i] = self.__insert_invalid_float(np.array(river_priors.rx2("logA0_hat")), invalid)
        prior_dict["logn_hat"][nod_i] = self.__insert_invalid_float(np.array(river_priors.rx2("logn_hat")), invalid)
        prior_dict["b_hat"][nod_i] = self.__insert_invalid_float(np.array(river_priors.rx2("b_hat")), invalid)
        prior_dict["logWb_hat"][nod_i] = self.__insert_invalid_float(np.array(river_priors.rx2("logWb_hat")), invalid)
        prior_dict["logDb_hat"][nod_i] = self.__insert_invalid_float(np.array(river_priors.rx2("logDb_hat")), invalid)
        prior_dict["logr_hat"][nod_i] = self.__insert_invalid_float(np.array(river_priors.rx2("logr_hat")), invalid)
        prior_dict["logA0_sd"][nod_i] = self.__insert_invalid_float(np.array(river_priors.rx2("logA0_sd")), invalid)
        prior_dict["logn_sd"][nod_i] = self.__insert_invalid_float(np.array(river_priors.rx2("logn_sd")), invalid)
        prior_dict["b_sd"][nod_i] = self.__insert_invalid_float(np.array(river_priors.rx2("b_sd")), invalid)
        prior_dict["logWb_sd"][nod_i] = self.__insert_invalid_float(np.array(river_priors.rx2("logWb_sd")), invalid)
        prior_dict["logDb_sd"][nod_i] = self.__insert_invalid_float(np.array(river_priors.rx2("logDb_sd")), invalid)
        prior_dict["logr_sd"][nod_i] = self.__insert_invalid_float(np.array(river_priors.rx2("logr_sd")), invalid)

        other_priors = priors.rx2("other_priors")
        prior_dict["lowerbound_logWc"][rch_i] = np.array(other_priors.rx2("lowerbound_logWc"))
        prior_dict["upperbound_logWc"][rch_i] = np.array(other_priors.rx2("upperbound_logWc"))
        prior_dict["lowerbound_logQc"][rch_i] = np.array(other_priors.rx2("lowerbound_logQc"))
        prior_dict["upperbound_logQc"][rch_i] = np.array(other_priors.rx2("upperbound_logQc"))
        prior_dict["logWc_hat"][rch_i] = np.array(other_priors.rx2("logWc_hat"))
        prior_dict["logQc_hat"][rch_i] = np.array(other_priors.rx2("logQc_hat"))
        prior_dict["logQ_sd"][rch_i] = np.mean(np.array(other_priors.rx2("logQ_sd")))
        prior_dict["logWc_sd"][rch_i] = np.array(other_priors.rx2("logWc_sd"))
        prior_dict["logQc_sd"][rch_i] = np.array(other_priors.rx2("logQc_sd"))
        prior_dict["Werr_sd"][rch_i] = np.array(other_priors.rx2("Werr_sd"))
        prior_dict["Serr_sd"][rch_i] = np.array(other_priors.rx2("Serr_sd"))
        prior_dict["dAerr_sd"][rch_i] = np.array(other_priors.rx2("dAerr_sd"))
        prior_dict["sigma_man"][nod_i] = self.__insert_invalid_float(np.mean(np.array(other_priors.rx2("sigma_man")), axis=1), invalid)
        prior_dict["sigma_amhg"][nod_i] = self.__insert_invalid_float(np.mean(np.array(other_priors.rx2("sigma_amhg")), axis=1), invalid)
        prior_dict["overwritten_indexes"][nod_i] = np.full(nod_i[0].shape, fill_value=1, dtype=np.int32) 
        print('------------------finished writing node dict')
        return prior_dict

    def __extract_reach_priors(self, priors, prior_dict, index):
        """Extract priors and store them in reach key of the prior_dict by index.
        
        Parameters
        ----------
        priors: rpy2.robjects.vectors.ListVector
            geoBAM priors stored in R named list
        prior_dict: dict
            dictionary of priors for a continent
        index: int
            integer index to store priors at
        """
        
        prior_dict["river_type"][index] = np.array(priors.rx2("River_Type"))
        river_priors = priors.rx2("river_type_priors")
        print('---------------extract reach priors river type ---------------')
        print(river_priors)
        prior_dict["lowerbound_A0"][index] = np.array(river_priors.rx2("lowerbound_A0"))
        prior_dict["upperbound_A0"][index] = np.array(river_priors.rx2("upperbound_A0"))
        prior_dict["lowerbound_logn"][index] = np.array(river_priors.rx2("lowerbound_logn"))
        prior_dict["upperbound_logn"][index] = np.array(river_priors.rx2("upperbound_logn"))
        prior_dict["lowerbound_b"][index] = np.array(river_priors.rx2("lowerbound_b"))
        prior_dict["upperbound_b"][index] = np.array(river_priors.rx2("upperbound_b"))
        prior_dict["lowerbound_logWb"][index] = np.array(river_priors.rx2("lowerbound_logWb"))
        prior_dict["upperbound_logWb"][index] = np.array(river_priors.rx2("upperbound_logWb"))
        prior_dict["lowerbound_logDb"][index] = np.array(river_priors.rx2("lowerbound_logDb"))
        prior_dict["upperbound_logDb"][index] = np.array(river_priors.rx2("upperbound_logDb"))
        prior_dict["lowerbound_logr"][index] = np.array(river_priors.rx2("lowerbound_logr"))
        prior_dict["upperbound_logr"][index] = np.array(river_priors.rx2("upperbound_logr"))
        prior_dict["logA0_hat"][index] = np.array(river_priors.rx2("logA0_hat"))
        prior_dict["logn_hat"][index] = np.array(river_priors.rx2("logn_hat"))
        prior_dict["b_hat"][index] = np.array(river_priors.rx2("b_hat"))
        prior_dict["logWb_hat"][index] = np.array(river_priors.rx2("logWb_hat"))
        prior_dict["logDb_hat"][index] = np.array(river_priors.rx2("logDb_hat"))
        prior_dict["logr_hat"][index] = np.array(river_priors.rx2("logr_hat"))
        prior_dict["logA0_sd"][index] = np.array(river_priors.rx2("logA0_sd"))
        prior_dict["logn_sd"][index] = np.array(river_priors.rx2("logn_sd"))
        prior_dict["b_sd"][index] = np.array(river_priors.rx2("b_sd"))
        prior_dict["logWb_sd"][index] = np.array(river_priors.rx2("logWb_sd"))
        prior_dict["logDb_sd"][index] = np.array(river_priors.rx2("logDb_sd"))
        prior_dict["logr_sd"][index] = np.array(river_priors.rx2("logr_sd"))


        print('---------------extract reach priors other type ---------------')

        other_priors = priors.rx2("other_priors")
        print(other_priors)
        prior_dict["lowerbound_logWc"][index] = np.array(other_priors.rx2("lowerbound_logWc"))
        prior_dict["upperbound_logWc"][index] = np.array(other_priors.rx2("upperbound_logWc"))
        prior_dict["lowerbound_logQc"][index] = np.array(other_priors.rx2("lowerbound_logQc"))
        prior_dict["upperbound_logQc"][index] = np.array(other_priors.rx2("upperbound_logQc"))
        prior_dict["logWc_hat"][index] = np.array(other_priors.rx2("logWc_hat"))
        prior_dict["logQc_hat"][index] = np.array(other_priors.rx2("logQc_hat"))
        prior_dict["logQ_sd"][index] = np.mean(np.array(other_priors.rx2("logQ_sd")))
        prior_dict["logWc_sd"][index] = np.array(other_priors.rx2("logWc_sd"))
        prior_dict["logQc_sd"][index] = np.array(other_priors.rx2("logQc_sd"))
        print('wderrr----------------------------------------------before')
        print(prior_dict["Werr_sd"][index])
        print('wderrr----------------------------------------------replacing with')
        print(np.array(other_priors.rx2("Werr_sd")))
        prior_dict["Werr_sd"][index] = np.array(other_priors.rx2("Werr_sd"))
        print('wderrr----------------------------------------------after')
        print(prior_dict["Werr_sd"][index])
        prior_dict["Serr_sd"][index] = np.array(other_priors.rx2("Serr_sd"))
        prior_dict["dAerr_sd"][index] = np.array(other_priors.rx2("dAerr_sd"))
        prior_dict["sigma_man"][index] = np.mean(np.array(other_priors.rx2("sigma_man")))
        prior_dict["sigma_amhg"][index] = np.mean(np.array(other_priors.rx2("sigma_amhg")))
        prior_dict["overwritten_indexes"][index] = 1
        print('----------finished writing reach dict--------------------')
        return prior_dict

    def __insert_invalid_int(self, prior, invalid_indexes):
        """Insert NaN value at invalid indezes in river_priors array."""

        # Insert NaN for NA_Integer and NA_logical
        prior[prior == -2147483648] = self.INT_FILL
        prior[np.isnan(prior)] = self.INT_FILL

        # Insert NaN for invalid nodes
        for index in invalid_indexes:
            prior = np.insert(prior, index, self.INT_FILL)

        return prior

    def __insert_invalid_float(self, prior, invalid_indexes):
        """Insert NaN value at invalid indezes in river_priors array."""

        # Insert NaN for NA_Integer and NA_logical
        prior[prior == -2147483648] = self.FlOAT_FILL
        prior[np.isnan(prior)] = self.FlOAT_FILL

        # Insert NaN for invalid nodes
        for index in invalid_indexes:
            prior = np.insert(prior, index, self.FlOAT_FILL)

        return prior

    def run_gb(self):
        """Executes geoBAM on reach and node data and stores priors."""

        num_reaches = self.sos_dict["reach_id"].shape[0]
        num_nodes = self.sos_dict["node_id"].shape[0]
        reach_temp_dict = self.__create_temp_reach_dict(num_reaches)
        node_temp_dict = self.__create_node_temp_dict(num_nodes)

        cont = self.sos_dict["cont"]
        swot_files = [ Path(swot_file) for swot_file in glob.glob(f"{self.swot_dir}/{self.CONT_DICT[cont]}*_SWOT.nc") ]

        cnt = 0
        for swot_file in swot_files:
            cnt +=1
            # if str(cnt).endswith('00'):
            #     print('--------------------------------------', cnt, f'of {len(swot_files)} files processed ...')
            try:
                # print(f"Running on file: {swot_file.name}")     
                reach_id = int(swot_file.name.split('_')[0])
                sos_ri = np.where(self.sos_dict["reach_id"] == reach_id)
                
                qhat = self.sos_dict["qhat"][sos_ri[0]]
                swot_data = extract_swot(swot_file, qhat)
                print('-----------------swot data--------------')
                print(swot_data)
                if swot_data["reach"]:
                    gb = GB(swot_data["reach"])
                    data = gb.bam_data_reach()
                    print('------bam reach data------')
                    print(data)
                    priors = gb.bam_priors(data)
                    print('----gbreachpriors-----')
                    print(priors)
                    reach_temp_dict = self.__extract_reach_priors(priors, reach_temp_dict, sos_ri)
                
                if swot_data["node"]:
                    gb = GB(swot_data["node"])
                    data = gb.bam_data_node()
                    priors = gb.bam_priors(data)
                    sos_ni = np.where(self.sos_dict["reach_node_id"] == reach_id)
                    node_temp_dict = self.__extract_node_priors(priors, node_temp_dict, sos_ri, sos_ni, swot_data["node"]["invalid_indexes"])
            except Exception as e:
                print(swot_file.name, 'failed')
                print('---------------important error--------------')
                print(e)
                pass
        
        self.gb_dict["reach"] = reach_temp_dict
        self.gb_dict["node"] = node_temp_dict

def extract_sos(sos_file):
        """Reads in data from the SoS and stores in sos_dict attribute.
        
        May want to overwrite grades with gage priors before executing is 
        running on constrained data product.

        Returns
        -------
        sos_dict: dict
            dictionary of SoS data
        """

        sos = Dataset(sos_file)
        sos_dict = { 
            "cont": sos_file.name.split('_')[0],
            "qhat": sos["model"]["mean_q"][:].filled(np.nan),
            "reach_id": sos["reaches"]["reach_id"][:],
            "node_id": sos["nodes"]["node_id"][:],
            "reach_node_id": sos["nodes"]["reach_id"][:]
        }
        sos.close()
        return sos_dict

def extract_swot(swot_file, qhat):
    """Extract and stores SWOT data in swot_dict attribute.
    
    Requires sos_dict be populated with qhat values.

    Returns
    -------
    swot_dict: dict
        dictionary of reach and node swot observations
    """
    

    swot = Dataset(swot_file)
    node_width = swot["node/width"][:].filled(np.nan)
    node_d_x_area = swot["node/d_x_area"][:].filled(np.nan)
    node_slope = swot["node/slope2"][:].filled(np.nan)
    reach_width = swot["reach/width"][:].filled(np.nan)
    reach_d_x_area = swot["reach/d_x_area"][:].filled(np.nan)
    reach_slope = swot["reach/slope2"][:].filled(np.nan)
    swot.close()

    swot_dict = {
        "node" : check_observations_node(node_width, node_d_x_area, node_slope, qhat),
        "reach" : check_observations_reach(reach_width, reach_d_x_area, reach_slope, qhat)
    }
    return swot_dict

def check_observations_node(width, d_x_area, slope2, qhat):
    """Checks for valid observation data (parameter values).

    Valid data includes:
        - Non-negative values for Qhat, width, and slope
        - Each time step and node has at least 5 valid floating point values

    Returns empty dictionary if invalid data detected.
    """

    # Test validity of data
    qhat[qhat < 0] = np.NAN 
    slope2[slope2 < 0] = np.NaN
    width[width < 0] = np.NaN

    # Qhat
    if np.isnan(qhat[0]):
        return {}

    # slope2
    slope_dict = is_valid_node(slope2)
    if not slope_dict["valid"]:
        return {}

    # width
    width_dict = is_valid_node(width)
    if not width_dict["valid"]:
        return {}

    # d_x_area
    dA_dict = is_valid_node(d_x_area)
    if not dA_dict["valid"]:
        return {}

    # Remove invalid node (row) observations
    invalid_node_indexes = np.unique(np.concatenate((slope_dict["invalid_nodes"][0], 
        width_dict["invalid_nodes"][0], dA_dict["invalid_nodes"][0])))
    slope2 = np.delete(slope2, invalid_node_indexes, axis = 0)
    width = np.delete(width, invalid_node_indexes, axis = 0)
    d_x_area = np.delete(d_x_area, invalid_node_indexes, axis = 0)
    
    # Remove invalid time (column) indexes
    invalid_time_indexes = np.unique(np.concatenate((slope_dict["invalid_times"][0], 
        width_dict["invalid_times"][0], dA_dict["invalid_times"][0])))
    slope2 = np.delete(slope2, invalid_time_indexes, axis = 1)
    width = np.delete(width, invalid_time_indexes, axis = 1)
    d_x_area = np.delete(d_x_area, invalid_time_indexes, axis = 1)
      
    # Valid data is returned
    return {
        "slope2" : slope2,
        "width" : width,
        "d_x_area" : d_x_area,
        "Qhat" : qhat,
        "invalid_indexes" : invalid_node_indexes
    }
    
def is_valid_node(obs):
    """Checks if there are atleast 5 valid nx values for each nt.

    Returns dictionary of whether the observations are valid, and if they are 
    valid includes invalid node and time step indexes.
    """

    # Gather a count of valid values per nx (across nt) returns nt vector
    time = np.apply_along_axis(lambda obs: np.count_nonzero(~np.isnan(obs)),
        axis = 0, arr = obs)

    # Are there enough valid nx per nt
    valid_time = time[time >= 5]

    # Gather a count of valid values per nt (across nx) returns nx vector
    nodes = np.apply_along_axis(lambda obs: np.count_nonzero(~np.isnan(obs)),
        axis = 1, arr = obs)
    
    # Are there enough valid nt per nx
    valid_nodes = nodes[nodes >= 5]

    if valid_time.size >= 5 and valid_nodes.size >= 5:
        return {
            "valid" : True,
            "invalid_nodes" : np.nonzero(nodes < 5),
            "invalid_times" : np.nonzero(time < 5)
            }
    else:
        return {
            "valid" : False,
            "invalid_nodes" : None,
            "invalid_times" : None
            }

def check_observations_reach(width, d_x_area, slope2, qhat):
    """Checks for valid observation data (parameter values).

    Valid data includes:
        - Non-negative values for Qhat, width, and slope
        - Each time step has at least 5 valid floating point values

    Returns empty dictionary if invalid data detected.
    """

    # Test validity of data
    qhat[qhat < 0] = np.NAN 
    slope2[slope2 < 0] = np.NaN
    width[width < 0] = np.NaN

    # Qhat
    if np.isnan(qhat[0]):
        return {}

    # slope2
    slope_dict = is_valid_reach(slope2)
    if not slope_dict["valid"]:
        return {}

    # width
    width_dict = is_valid_reach(width)
    if not width_dict["valid"]:
        return {}

    # d_x_area
    dA_dict = is_valid_reach(d_x_area)
    if not dA_dict["valid"]:
        return {}
    
    # Remove invalid time (column) indexes
    invalid_time_indexes = np.unique(np.concatenate((slope_dict["invalid_times"], 
        width_dict["invalid_times"], dA_dict["invalid_times"])))
    slope2 = np.delete(slope2, invalid_time_indexes, axis = 0)
    width = np.delete(width, invalid_time_indexes, axis = 0)
    d_x_area = np.delete(d_x_area, invalid_time_indexes, axis = 0)
      
    # Valid data is returned
    return {
        "slope2" : slope2,
        "width" : width,
        "d_x_area" : d_x_area,
        "Qhat" : qhat
    }
    
def is_valid_reach(obs):
    """Checks if there are atleast 5 valid nx values for each nt.

    Returns dictionary of whether the observations are valid, and if they are 
    valid includes invalid node and time step indexes.
    """

    # Gather a count of valid values per reach (across nt) returns nt vector
    time = np.apply_along_axis(lambda obs: np.count_nonzero(~np.isnan(obs)),
        axis = 0, arr = obs)

    # Are there enough valid nx per nt
    valid_time = time >= 5

    if valid_time:
        return {
            "valid" : True,
            "invalid_times" : np.argwhere(np.isnan(obs))
        }
    else:
        return {
            "valid" : False,
            "invalid_times" : None
        }
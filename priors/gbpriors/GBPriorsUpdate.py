# Standard imports
from datetime import datetime

# Third-party imports
from netCDF4 import Dataset
import numpy as np

class GBPriorsUpdate:
    """Class that updates geoBAM priors to the SoS.
    
    Attributes
    ----------
    FLOAT_FILL: float
        Fill value for any missing float values in mapped WBM data
    gb_dict: dict
        dictionary of GeoBAM priors organized by continent
    INT_FILL: int
        Fill value for any missing integer values in mapped WBM data
    sos_file: Path
        path to SoS NetCDF file

    Methods
    -------
    update_data()
        Updates geoBAM priors in the SoS
    __update_level(grp, level)
        Updates data in the SoS
    """

    FLOAT_FILL = -999999999999
    INT_FILL = -999

    def __init__(self, gb_dict, sos_file, metadata_json):
        """
        Parameters
        ----------
        gb_dict: dict
            dictionary of geoBAM priors organized by continent
        sos_file: Path
            path to SoS NetCDF file
        """

        self.gb_dict = gb_dict
        self.sos_file = sos_file
        self.variable_atts = metadata_json["gbpriors"]

    def update_data(self):
        """Updates geoBAM priors in the SoS."""

        sos = Dataset(self.sos_file, 'a')

        sos.production_date = datetime.now().strftime('%d-%b-%Y %H:%M:%S')
        self.__update_level(sos, "reach")
        self.__update_level(sos, "node")
        self.__update_empty_nodes_with_filled_reaches(sos)
        self.__write_overwritten_indices(sos)

        sos.close()

    def __update_empty_nodes_with_filled_reaches(self, sos):
        """Updates nodes with empty data with reach-level data from gbpriors execution"""

        rch_grp = sos["gbpriors"]["reach"]
        nod_grp = sos["gbpriors"]["node"]

        for cnt, variable in enumerate(nod_grp.variables):
            print('update empty nodes - processing', variable, 'variable.', cnt+1, 'of', len(nod_grp.variables))

            if variable == "overwritten_indexes":
                continue    # Skip as this does not require reach-level data

            # Node level node variables vs reach-level node variables
            if len(nod_grp[variable][:]) != len(rch_grp[variable][:]):
                reaches_indices_with_gbpriors = np.where(rch_grp[variable][:].mask == False)[0]   # Non-missing reach-level data

                for reach_index in reaches_indices_with_gbpriors:
                    nodes_index = np.where(sos['nodes']['reach_id'][:] == sos['reaches']['reach_id'][reach_index])[0]
                    all_empty_node_data_index = np.where(nod_grp[variable][:].mask[nodes_index] == True)

                    if (all_empty_node_data_index[0].shape[0]) > 0:
                        nodes_index = nodes_index[all_empty_node_data_index[0]]
                        reach_data = rch_grp[variable][reach_index]
                        nod_grp[variable][nodes_index] = reach_data

    def __write_overwritten_indices(self, sos):
        """Write location of where gbpriors were overwritten.
        
        Parameters
        ----------
        sos: netcdf4.Dataset
            SOS dataset to write to
        """
        
        # Try excepts here because the variables are only created once
        if "overwritten_indexes" in sos["gbpriors"]["reach"].variables:
            oi = sos["gbpriors"]["reach"]["overwritten_indexes"]
            oi[:] = [self.gb_dict["reach"]["overwritten_indexes"]]
            self.set_variable_atts(oi, self.variable_atts["reach"]["overwritten_indexes"])
        else:
            oi = sos["gbpriors"]["reach"].createVariable("overwritten_indexes", "i4", ("num_reaches",), compression="zlib")
            oi.long_name = "GeoBAM overwritten_prior_indexes"
            oi.comment = "Indexes of geoBAM priors that were overwritten."
            oi.coverage_content_type = "qualityInformation"
            oi[:] = self.gb_dict["reach"]["overwritten_indexes"]
        
        if "overwritten_indexes" in sos["gbpriors"]["node"].variables:
            oi = sos["gbpriors"]["node"]["overwritten_indexes"]
            oi[:] = [self.gb_dict["node"]["overwritten_indexes"]]
            self.set_variable_atts(oi, self.variable_atts["node"]["overwritten_indexes"])

        else:
            oi = sos["gbpriors"]["node"].createVariable("overwritten_indexes", "i4", ("num_nodes",), compression="zlib")
            oi.long_name = "GeoBAM overwritten_prior_indexes"
            oi.comment = "Indexes of geoBAM priors that were overwritten."
            oi.coverage_content_type = "qualityInformation"
            oi[:] = self.gb_dict["node"]["overwritten_indexes"]

    def __update_level(self, sos, level):
        """Updates data in the SoS.
        
        Parameters
        ----------
        grp: netcdf4.Group
            group for data storage
        level: str
            'reach' or 'node' level-data indicator
        """
        grp = sos['gbpriors'][level]

        grp["river_type"][:] = np.nan_to_num(self.gb_dict[level]["river_type"], copy=True, nan=self.INT_FILL)
        self.set_variable_atts(grp["river_type"], self.variable_atts[level]["river_type"])

        grp["lowerbound_A0"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_A0"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["lowerbound_A0"], self.variable_atts[level]["lowerbound_A0"])

        grp["upperbound_A0"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_A0"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["upperbound_A0"], self.variable_atts[level]["upperbound_A0"])

        grp["lowerbound_logn"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_logn"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["lowerbound_logn"], self.variable_atts[level]["lowerbound_logn"])

        grp["upperbound_logn"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_logn"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["upperbound_logn"], self.variable_atts[level]["upperbound_logn"])

        grp["lowerbound_b"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_b"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["lowerbound_b"], self.variable_atts[level]["lowerbound_b"])

        grp["upperbound_b"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_b"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["upperbound_b"], self.variable_atts[level]["upperbound_b"])

        grp["lowerbound_logWb"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_logWb"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["lowerbound_logWb"], self.variable_atts[level]["lowerbound_logWb"])

        grp["upperbound_logWb"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_logWb"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["upperbound_logWb"], self.variable_atts[level]["upperbound_logWb"])

        grp["lowerbound_logDb"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_logDb"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["lowerbound_logDb"], self.variable_atts[level]["lowerbound_logDb"])

        grp["upperbound_logDb"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_logDb"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["upperbound_logDb"], self.variable_atts[level]["upperbound_logDb"])

        grp["lowerbound_logr"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_logr"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["lowerbound_logr"], self.variable_atts[level]["lowerbound_logr"])

        grp["upperbound_logr"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_logr"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["upperbound_logr"], self.variable_atts[level]["upperbound_logr"])

        grp["logA0_hat"][:] = np.nan_to_num(self.gb_dict[level]["logA0_hat"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logA0_hat"], self.variable_atts[level]["logA0_hat"])

        grp["logn_hat"][:] = np.nan_to_num(self.gb_dict[level]["logn_hat"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logn_hat"], self.variable_atts[level]["logn_hat"])

        grp["b_hat"][:] = np.nan_to_num(self.gb_dict[level]["b_hat"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["b_hat"], self.variable_atts[level]["b_hat"])

        grp["logWb_hat"][:] = np.nan_to_num(self.gb_dict[level]["logWb_hat"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logWb_hat"], self.variable_atts[level]["logWb_hat"])

        grp["logDb_hat"][:] = np.nan_to_num(self.gb_dict[level]["logDb_hat"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logDb_hat"], self.variable_atts[level]["logDb_hat"])

        grp["logr_hat"][:] = np.nan_to_num(self.gb_dict[level]["logr_hat"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logr_hat"], self.variable_atts[level]["logr_hat"])

        grp["logA0_sd"][:] = np.nan_to_num(self.gb_dict[level]["logA0_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logA0_sd"], self.variable_atts[level]["logA0_sd"])

        grp["logn_sd"][:] = np.nan_to_num(self.gb_dict[level]["logn_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logn_sd"], self.variable_atts[level]["logn_sd"])

        grp["b_sd"][:] = np.nan_to_num(self.gb_dict[level]["b_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["b_sd"], self.variable_atts[level]["b_sd"])

        grp["logWb_sd"][:] = np.nan_to_num(self.gb_dict[level]["logWb_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logWb_sd"], self.variable_atts[level]["logWb_sd"])

        grp["logDb_sd"][:] = np.nan_to_num(self.gb_dict[level]["logDb_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logDb_sd"], self.variable_atts[level]["logDb_sd"])

        grp["logr_sd"][:] = np.nan_to_num(self.gb_dict[level]["logr_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logr_sd"], self.variable_atts[level]["logr_sd"])

        grp["lowerbound_logWc"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_logWc"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["lowerbound_logWc"], self.variable_atts[level]["lowerbound_logWc"])

        grp["upperbound_logWc"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_logWc"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["upperbound_logWc"], self.variable_atts[level]["upperbound_logWc"])

        grp["lowerbound_logQc"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_logQc"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["lowerbound_logQc"], self.variable_atts[level]["lowerbound_logQc"])

        grp["upperbound_logQc"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_logQc"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["upperbound_logQc"], self.variable_atts[level]["upperbound_logQc"])

        grp["logWc_hat"][:] = np.nan_to_num(self.gb_dict[level]["logWc_hat"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logWc_hat"], self.variable_atts[level]["logWc_hat"])

        grp["logQc_hat"][:] = np.nan_to_num(self.gb_dict[level]["logQc_hat"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logQc_hat"], self.variable_atts[level]["logQc_hat"])

        grp["logQ_sd"][:] = np.nan_to_num(self.gb_dict[level]["logQ_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logQ_sd"], self.variable_atts[level]["logQ_sd"])

        grp["logWc_sd"][:] = np.nan_to_num(self.gb_dict[level]["logWc_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logWc_sd"], self.variable_atts[level]["logWc_sd"])

        grp["logQc_sd"][:] = np.nan_to_num(self.gb_dict[level]["logQc_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logQc_sd"], self.variable_atts[level]["logQc_sd"])

        grp["Werr_sd"][:] = np.nan_to_num(self.gb_dict[level]["Werr_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["Werr_sd"], self.variable_atts[level]["Werr_sd"])

        grp["Serr_sd"][:] = np.nan_to_num(self.gb_dict[level]["Serr_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["Serr_sd"], self.variable_atts[level]["Serr_sd"])

        grp["dAerr_sd"][:] = np.nan_to_num(self.gb_dict[level]["dAerr_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["dAerr_sd"], self.variable_atts[level]["dAerr_sd"])

        grp["sigma_man"][:] = np.nan_to_num(self.gb_dict[level]["sigma_man"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["sigma_man"], self.variable_atts[level]["sigma_man"])

        grp["sigma_amhg"][:] = np.nan_to_num(self.gb_dict[level]["sigma_amhg"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["sigma_amhg"], self.variable_atts[level]["sigma_amhg"])

    def set_variable_atts(self, variable, variable_dict):
        """Set the variable attribute metdata."""
        
        for name, value in variable_dict.items():
            setattr(variable, name, value)
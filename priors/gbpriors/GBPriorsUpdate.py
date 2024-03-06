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
        rch_grp = sos["gbpriors"]["reach"]
        sos.close()
        self.__update_level(rch_grp, "reach")
        sos = Dataset(self.sos_file, 'a')
        nod_grp = sos["gbpriors"]["node"]
        sos.close()
        self.__update_level(nod_grp, "node")
        self.__update_empty_nodes_with_filled_reaches(rch_grp, nod_grp)
        sos = Dataset(self.sos_file, 'a')
        # self.__update_level(nod_grp, "node")
        self.__write_overwritten_indices(sos)
        sos.close()
    
    def __update_empty_nodes_with_filled_reaches(self, rch_grp, nod_grp):
        #loop variables
        sos = Dataset(self.sos_file, 'a')
        cnt = 0
        for variable in nod_grp.variables:
            print('processing varaible', cnt, 'of', len(nod_grp.variables))
            cnt +=1
            if len(nod_grp[variable][:]) != len(rch_grp[variable][:]):
                reaches_indices_with_gbprios = np.where(rch_grp[variable][:].mask == False)[0]
                # reach_ids = rch_grp[reaches_indices_with_gbprios]
                # print(data['gbpriors']['reach']['logn_hat'][reaches_indices_with_gbprios][0])
                # loop reach ids
                for reach_index in reaches_indices_with_gbprios:
                    # print(reach_index)
                    reach_data = rch_grp[variable][reach_index]
                    
                    nodes_index = np.where(sos['nodes']['reach_id'][:] == sos['reaches']['reach_id'][reach_index])

                    all_empty_node_data_index = np.where(nod_grp[variable][nodes_index].mask == True)
                    if len(all_empty_node_data_index[0]) > 0:
                        real_indexes = []
                        for i in all_empty_node_data_index[0]:
                            # print(node_ids[0])
                            real_indexes.append(nodes_index[0][i])

                        # print('found some empty node variables at, ', nodes_index)
                        # # print(nod_grp[variable][nodes_index])
                        # print('replacing them with: ', reach_data)
                        # nod_grp[variable][all_empty_node_data_index] = reach_data
                        # print('after')
                        # print(nod_grp[variable][all_empty_node_data_index])
                        # print('maybe not actually working?')
                        # print('before')
                        # print(sos['gbpriors']['node'][variable][nodes_index])
                        sos['gbpriors']['node'][variable][real_indexes] = reach_data
                        # print('after')
                        # print(sos['gbpriors']['node'][variable][nodes_index])

        sos.close()
        
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

    def __update_level(self, grp, level):
        """Updates data in the SoS.
        
        Parameters
        ----------
        grp: netcdf4.Group
            group for data storage
        level: str
            'reach' or 'node' level-data indicator
        """
        sos = Dataset(self.sos_file, 'a')
        sos['gbpriors'][level]["river_type"][:] = np.nan_to_num(self.gb_dict[level]["river_type"], copy=True, nan=self.INT_FILL)
        self.set_variable_atts(grp["river_type"], self.variable_atts[level]["river_type"])
        
        sos['gbpriors'][level]["lowerbound_A0"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_A0"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["lowerbound_A0"], self.variable_atts[level]["lowerbound_A0"])
        
        sos['gbpriors'][level]["upperbound_A0"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_A0"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["upperbound_A0"], self.variable_atts[level]["upperbound_A0"])
        
        sos['gbpriors'][level]["lowerbound_logn"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_logn"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["lowerbound_logn"], self.variable_atts[level]["lowerbound_logn"])
        
        sos['gbpriors'][level]["upperbound_logn"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_logn"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["upperbound_logn"], self.variable_atts[level]["upperbound_logn"])
        
        sos['gbpriors'][level]["lowerbound_b"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_b"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["lowerbound_b"], self.variable_atts[level]["lowerbound_b"])

        sos['gbpriors'][level]["upperbound_b"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_b"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["upperbound_b"], self.variable_atts[level]["upperbound_b"])

        sos['gbpriors'][level]["lowerbound_logWb"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_logWb"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["lowerbound_logWb"], self.variable_atts[level]["lowerbound_logWb"])

        sos['gbpriors'][level]["upperbound_logWb"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_logWb"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["upperbound_logWb"], self.variable_atts[level]["upperbound_logWb"])

        sos['gbpriors'][level]["lowerbound_logDb"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_logDb"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["lowerbound_logDb"], self.variable_atts[level]["lowerbound_logDb"])

        sos['gbpriors'][level]["upperbound_logDb"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_logDb"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["upperbound_logDb"], self.variable_atts[level]["upperbound_logDb"])

        sos['gbpriors'][level]["lowerbound_logr"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_logr"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["lowerbound_logr"], self.variable_atts[level]["lowerbound_logr"])

        sos['gbpriors'][level]["upperbound_logr"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_logr"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["upperbound_logr"], self.variable_atts[level]["upperbound_logr"])

        sos['gbpriors'][level]["logA0_hat"][:] = np.nan_to_num(self.gb_dict[level]["logA0_hat"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logA0_hat"], self.variable_atts[level]["logA0_hat"])

        sos['gbpriors'][level]["logn_hat"][:] = np.nan_to_num(self.gb_dict[level]["logn_hat"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logn_hat"], self.variable_atts[level]["logn_hat"])

        sos['gbpriors'][level]["b_hat"][:] = np.nan_to_num(self.gb_dict[level]["b_hat"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["b_hat"], self.variable_atts[level]["b_hat"])

        sos['gbpriors'][level]["logWb_hat"][:] = np.nan_to_num(self.gb_dict[level]["logWb_hat"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logWb_hat"], self.variable_atts[level]["logWb_hat"])

        sos['gbpriors'][level]["logDb_hat"][:] = np.nan_to_num(self.gb_dict[level]["logDb_hat"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logDb_hat"], self.variable_atts[level]["logDb_hat"])

        sos['gbpriors'][level]["logr_hat"][:] = np.nan_to_num(self.gb_dict[level]["logr_hat"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logr_hat"], self.variable_atts[level]["logr_hat"])

        sos['gbpriors'][level]["logA0_sd"][:] = np.nan_to_num(self.gb_dict[level]["logA0_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logA0_sd"], self.variable_atts[level]["logA0_sd"])

        sos['gbpriors'][level]["logn_sd"][:] = np.nan_to_num(self.gb_dict[level]["logn_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logn_sd"], self.variable_atts[level]["logn_sd"])

        sos['gbpriors'][level]["b_sd"][:] = np.nan_to_num(self.gb_dict[level]["b_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["b_sd"], self.variable_atts[level]["b_sd"])

        sos['gbpriors'][level]["logWb_sd"][:] = np.nan_to_num(self.gb_dict[level]["logWb_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logWb_sd"], self.variable_atts[level]["logWb_sd"])

        sos['gbpriors'][level]["logDb_sd"][:] = np.nan_to_num(self.gb_dict[level]["logDb_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logDb_sd"], self.variable_atts[level]["logDb_sd"])

        sos['gbpriors'][level]["logr_sd"][:] = np.nan_to_num(self.gb_dict[level]["logr_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logr_sd"], self.variable_atts[level]["logr_sd"])

        sos['gbpriors'][level]["lowerbound_logWc"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_logWc"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["lowerbound_logWc"], self.variable_atts[level]["lowerbound_logWc"])

        sos['gbpriors'][level]["upperbound_logWc"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_logWc"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["upperbound_logWc"], self.variable_atts[level]["upperbound_logWc"])

        sos['gbpriors'][level]["lowerbound_logQc"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_logQc"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["lowerbound_logQc"], self.variable_atts[level]["lowerbound_logQc"])

        sos['gbpriors'][level]["upperbound_logQc"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_logQc"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["upperbound_logQc"], self.variable_atts[level]["upperbound_logQc"])

        sos['gbpriors'][level]["logWc_hat"][:] = np.nan_to_num(self.gb_dict[level]["logWc_hat"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logWc_hat"], self.variable_atts[level]["logWc_hat"])

        sos['gbpriors'][level]["logQc_hat"][:] = np.nan_to_num(self.gb_dict[level]["logQc_hat"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logQc_hat"], self.variable_atts[level]["logQc_hat"])

        sos['gbpriors'][level]["logQ_sd"][:] = np.nan_to_num(self.gb_dict[level]["logQ_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logQ_sd"], self.variable_atts[level]["logQ_sd"])

        sos['gbpriors'][level]["logWc_sd"][:] = np.nan_to_num(self.gb_dict[level]["logWc_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logWc_sd"], self.variable_atts[level]["logWc_sd"])

        sos['gbpriors'][level]["logQc_sd"][:] = np.nan_to_num(self.gb_dict[level]["logQc_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["logQc_sd"], self.variable_atts[level]["logQc_sd"])

        print('---------------werr getting written to sos-----------------------')
        # print(np.nan_to_num(self.gb_dict[level]["Werr_sd"], copy=True, nan=self.FLOAT_FILL))
        sos['gbpriors'][level]["Werr_sd"][:] = np.nan_to_num(self.gb_dict[level]["Werr_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["Werr_sd"], self.variable_atts[level]["Werr_sd"])

        sos['gbpriors'][level]["Serr_sd"][:] = np.nan_to_num(self.gb_dict[level]["Serr_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["Serr_sd"], self.variable_atts[level]["Serr_sd"])

        sos['gbpriors'][level]["dAerr_sd"][:] = np.nan_to_num(self.gb_dict[level]["dAerr_sd"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["dAerr_sd"], self.variable_atts[level]["dAerr_sd"])

        sos['gbpriors'][level]["sigma_man"][:] = np.nan_to_num(self.gb_dict[level]["sigma_man"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["sigma_man"], self.variable_atts[level]["sigma_man"])

        sos['gbpriors'][level]["sigma_amhg"][:] = np.nan_to_num(self.gb_dict[level]["sigma_amhg"], copy=True, nan=self.FLOAT_FILL)
        self.set_variable_atts(grp["sigma_amhg"], self.variable_atts[level]["sigma_amhg"])
        sos.close()

        # copy the nodes if they are empty

        # if level == 'node':
        #     for variable in grp.variables:
        #         data_in_group = grp[variable][:]
        #         if all(data_in_group.mask):
        #             grp[variable] = self.variable_atts['reach'][variable][:]


    def set_variable_atts(self, variable, variable_dict):
        """Set the variable attribute metdata."""
        
        for name, value in variable_dict.items():
            setattr(variable, name, value)
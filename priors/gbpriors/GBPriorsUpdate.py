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

    def __init__(self, gb_dict, sos_file):
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

    def update_data(self):
        """Updates geoBAM priors in the SoS."""

        sos = Dataset(self.sos_file, 'a')
        sos.production_date = datetime.now().strftime('%d-%b-%Y %H:%M:%S')
        rch_grp = sos["gbpriors"]["reach"]
        self.__update_level(rch_grp, "reach")
        nod_grp = sos["gbpriors"]["node"]
        self.__update_level(nod_grp, "node")
        self.__write_overwritten_indices(sos)
        sos.close()
        
    def __write_overwritten_indices(self, sos):
        """Write location of where gbpriors were overwritten.
        
        Parameters
        ----------
        sos: netcdf4.Dataset
            SOS dataset to write to
        """
        
        # Try excepts here because the variables are only created once
        try:
            oi = sos["gbpriors"]["reach"].createVariable("overwritten_indexes", "i4", ("num_reaches",))
            oi.comment = "Indexes of geoBAM priors that were overwritten."
            oi[:] = self.gb_dict["reach"]["overwritten_indexes"]
        except:
            oi = sos["gbpriors"]["reach"]["overwritten_indexes"]
            print('sos',oi)
            print('new', len(self.gb_dict["reach"]["overwritten_indexes"]))
            oi[:] = [self.gb_dict["reach"]["overwritten_indexes"]]
            print('sos', oi)


        
        try:
            oi = sos["gbpriors"]["node"].createVariable("overwritten_indexes", "i4", ("num_nodes",))
            oi.comment = "Indexes of geoBAM priors that were overwritten."
            oi[:] = self.gb_dict["node"]["overwritten_indexes"]

        except:
            oi = sos["gbpriors"]["node"]["overwritten_indexes"]
            print('sos',oi)
            print('new', len(self.gb_dict["node"]["overwritten_indexes"]))
            oi[:] = [self.gb_dict["node"]["overwritten_indexes"]]

    def __update_level(self, grp, level):
        """Updates data in the SoS.
        
        Parameters
        ----------
        grp: netcdf4.Group
            group for data storage
        level: str
            'reach' or 'node' level-data indicator
        """

        grp["river_type"][:] = np.nan_to_num(self.gb_dict[level]["river_type"], copy=True, nan=self.INT_FILL)
        grp["lowerbound_A0"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_A0"], copy=True, nan=self.FLOAT_FILL)
        grp["upperbound_A0"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_A0"], copy=True, nan=self.FLOAT_FILL)
        grp["lowerbound_logn"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_logn"], copy=True, nan=self.FLOAT_FILL)
        grp["upperbound_logn"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_logn"], copy=True, nan=self.FLOAT_FILL)
        grp["lowerbound_b"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_b"], copy=True, nan=self.FLOAT_FILL)
        grp["upperbound_b"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_b"], copy=True, nan=self.FLOAT_FILL)
        grp["lowerbound_logWb"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_logWb"], copy=True, nan=self.FLOAT_FILL)
        grp["upperbound_logWb"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_logWb"], copy=True, nan=self.FLOAT_FILL)
        grp["lowerbound_logDb"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_logDb"], copy=True, nan=self.FLOAT_FILL)
        grp["upperbound_logDb"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_logDb"], copy=True, nan=self.FLOAT_FILL)
        grp["lowerbound_logr"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_logr"], copy=True, nan=self.FLOAT_FILL)
        grp["upperbound_logr"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_logr"], copy=True, nan=self.FLOAT_FILL)
        grp["logA0_hat"][:] = np.nan_to_num(self.gb_dict[level]["logA0_hat"], copy=True, nan=self.FLOAT_FILL)
        grp["logn_hat"][:] = np.nan_to_num(self.gb_dict[level]["logn_hat"], copy=True, nan=self.FLOAT_FILL)
        grp["b_hat"][:] = np.nan_to_num(self.gb_dict[level]["b_hat"], copy=True, nan=self.FLOAT_FILL)
        grp["logWb_hat"][:] = np.nan_to_num(self.gb_dict[level]["logWb_hat"], copy=True, nan=self.FLOAT_FILL)
        grp["logDb_hat"][:] = np.nan_to_num(self.gb_dict[level]["logDb_hat"], copy=True, nan=self.FLOAT_FILL)
        grp["logr_hat"][:] = np.nan_to_num(self.gb_dict[level]["logr_hat"], copy=True, nan=self.FLOAT_FILL)
        grp["logA0_sd"][:] = np.nan_to_num(self.gb_dict[level]["logA0_sd"], copy=True, nan=self.FLOAT_FILL)
        grp["logn_sd"][:] = np.nan_to_num(self.gb_dict[level]["logn_sd"], copy=True, nan=self.FLOAT_FILL)
        grp["b_sd"][:] = np.nan_to_num(self.gb_dict[level]["b_sd"], copy=True, nan=self.FLOAT_FILL)
        grp["logWb_sd"][:] = np.nan_to_num(self.gb_dict[level]["logWb_sd"], copy=True, nan=self.FLOAT_FILL)
        grp["logDb_sd"][:] = np.nan_to_num(self.gb_dict[level]["logDb_sd"], copy=True, nan=self.FLOAT_FILL)
        grp["logr_sd"][:] = np.nan_to_num(self.gb_dict[level]["logr_sd"], copy=True, nan=self.FLOAT_FILL)
        grp["lowerbound_logWc"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_logWc"], copy=True, nan=self.FLOAT_FILL)
        grp["upperbound_logWc"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_logWc"], copy=True, nan=self.FLOAT_FILL)
        grp["lowerbound_logQc"][:] = np.nan_to_num(self.gb_dict[level]["lowerbound_logQc"], copy=True, nan=self.FLOAT_FILL)
        grp["upperbound_logQc"][:] = np.nan_to_num(self.gb_dict[level]["upperbound_logQc"], copy=True, nan=self.FLOAT_FILL)
        grp["logWc_hat"][:] = np.nan_to_num(self.gb_dict[level]["logWc_hat"], copy=True, nan=self.FLOAT_FILL)
        grp["logQc_hat"][:] = np.nan_to_num(self.gb_dict[level]["logQc_hat"], copy=True, nan=self.FLOAT_FILL)
        grp["logQ_sd"][:] = np.nan_to_num(self.gb_dict[level]["logQ_sd"], copy=True, nan=self.FLOAT_FILL)
        grp["logWc_sd"][:] = np.nan_to_num(self.gb_dict[level]["logWc_sd"], copy=True, nan=self.FLOAT_FILL)
        grp["logQc_sd"][:] = np.nan_to_num(self.gb_dict[level]["logQc_sd"], copy=True, nan=self.FLOAT_FILL)
        grp["Werr_sd"][:] = np.nan_to_num(self.gb_dict[level]["Werr_sd"], copy=True, nan=self.FLOAT_FILL)
        grp["Serr_sd"][:] = np.nan_to_num(self.gb_dict[level]["Serr_sd"], copy=True, nan=self.FLOAT_FILL)
        grp["dAerr_sd"][:] = np.nan_to_num(self.gb_dict[level]["dAerr_sd"], copy=True, nan=self.FLOAT_FILL)
        grp["sigma_man"][:] = np.nan_to_num(self.gb_dict[level]["sigma_man"], copy=True, nan=self.FLOAT_FILL)
        grp["sigma_amhg"][:] = np.nan_to_num(self.gb_dict[level]["sigma_amhg"], copy=True, nan=self.FLOAT_FILL)
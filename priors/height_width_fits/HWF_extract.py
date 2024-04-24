import np
import glob

class HWS_extract:

    # FlOAT_FILL = -999999999999
    # INT_FILL = -999
    CONT_DICT = { "af" : [1], "as" : [4, 3], "eu" : [2], "na" : [7, 8, 9],
        "oc" : [5], "sa" : [6] }
    

    def __init__(self, swot_dir):
        """
        Parameters
        ----------
        swot_dir: Path
            path to directory that contains SWOT NetCDF files
        """
        
        # self.gb_dict = {}
        # self.sos_file = sos_file
        # self.sos_dict = extract_sos(sos_file)
        # self.swot_dir = swot_dir
        # self.swot_time = []
        swot_files = [ Path(swot_file) for swot_file in glob.glob(f"{self.swot_dir}/{self.CONT_DICT[cont]}*_SWOT.nc") ]
        
        swot = Dataset(swot_file)
        # node_width = swot["node/width"][:].filled(np.nan)
        # node_d_x_area = swot["node/d_x_area"][:].filled(np.nan)
        # node_slope = swot["node/slope2"][:].filled(np.nan)
        # reach_width = swot["reach/width"][:].filled(np.nan)
        reach_d_x_area = swot["reach/d_x_area"][:].filled(np.nan)
        
        self.data = reach_d_x_area

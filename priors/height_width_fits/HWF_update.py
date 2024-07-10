    
import np
import glob

class HWF_update:

    # FlOAT_FILL = -999999999999
    # INT_FILL = -999
    CONT_DICT = { "af" : [1], "as" : [4, 3], "eu" : [2], "na" : [7, 8, 9],
        "oc" : [5], "sa" : [6] }
    

    def __init__(self, ):
        """
        Parameters
        ----------
        hwf_data: List
            List of HWF data for all of the reaches processed
        sos_file: Path
            path to SoS file
        """
        
            sos = Dataset(Path(self.sos_file))
        self.sos_reaches = sos["reaches"]["reach_id"][:].filled(np.nan).astype(int)
        sos.close()
    
    
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
    
    
    
    

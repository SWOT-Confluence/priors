# Third party imports
import rpy2.robjects as robjects
from rpy2.robjects import numpy2ri
from rpy2.robjects.packages import importr

# Print R warnings
robjects.r['options'](warn=1)

class GB:
    """Class that represents a run of geoBAM to extract priors.

    Serves as an API to geoBAM functions which are written in R.
    
    Attributes
    ----------
        input_data: dictionary
            dictionary of formatted input data
    """

    GEOBAM = importr("geoBAMr")

    def __init__(self, input_data):
        self.input_data = input_data
        self.geobam_data = None
        self.priors = None

    def bam_data_reach(self):
        """Runs geoBAMr::bam_data function using swot_data attribute.
        
        Returns bam_data object.
        """

        # Activate automatic conversion of numpy objects to rpy2 objects
        numpy2ri.activate()

        # Format input data for geoBAM::bam_data function
        width_matrix = robjects.r['t'](robjects.r['matrix'](self.input_data["width"]))
        slope_matrix = robjects.r['t'](robjects.r['matrix'](self.input_data["slope2"]))
        d_x_a_matrix = robjects.r['t'](robjects.r['matrix'](self.input_data["d_x_area"]))
        qhat_vector = robjects.FloatVector(self.input_data["Qhat"])

        # Run bam_data
        data = self.GEOBAM.bam_data(w = width_matrix, 
            s = slope_matrix, dA = d_x_a_matrix, 
            Qhat = qhat_vector, variant = 'manning_amhg')

        # Deactivate automatic conversion
        numpy2ri.deactivate()

        return data

    def bam_data_node(self):
        """Runs geoBAMr::bam_data function using swot_data attribute.
        
        Returns bam_data object.
        """

        # Activate automatic conversion of numpy objects to rpy2 objects
        numpy2ri.activate()

        # Format input data for geoBAM::bam_data function
        rows = self.input_data["width"].shape[0]
        cols = self.input_data["width"].shape[1]
        # nrows_reach = self.input_data["slope2"].shape[0]
        width_matrix = robjects.r['matrix'](self.input_data["width"], nrow=rows, ncol=cols)
        slope_matrix = robjects.r['matrix'](self.input_data["slope2"], nrow=rows, ncol=cols)
        d_x_a_matrix = robjects.r['matrix'](self.input_data["d_x_area"], nrow=rows, ncol=cols)
        qhat_vector = robjects.FloatVector(self.input_data["Qhat"])
        
        # Run bam_data
        data = self.GEOBAM.bam_data(w = width_matrix, 
            s = slope_matrix, dA = d_x_a_matrix, 
            Qhat = qhat_vector, variant = 'manning_amhg',
            max_xs = rows)

        # Deactivate automatic conversion
        numpy2ri.deactivate()

        return data

    def bam_priors(self, geobam_data):
        """Runs geoBAMr::bam_priors function using geobam_data parameter.
        
        Returns priors object.
        """
        
        return self.GEOBAM.bam_priors(bamdata = geobam_data, classification = "expert")
# Standard imports
from pathlib import Path
import unittest

# Third-party imports
from netCDF4 import Dataset
import numpy as np
from numpy.testing import assert_array_almost_equal, assert_array_equal

# Standard imports
from priors.gbpriors.GBPriorsGenerate import GBPriorsGenerate, extract_sos, extract_swot, check_observations_node, check_observations_reach, is_valid_node, is_valid_reach

class TestGBPriorsGenerate(unittest.TestCase):
    """Tesets methods of GBPriorsGenerate class."""

    # SWORD_FILE = Path(__file__).parent / "sword" / "na_sword_v11.nc"
    SWOT_DIR = Path(__file__).parent / "swot"
    SOS_FILE = Path(__file__).parent / "sos" / "constrained" / "na_sword_v11_SOS.nc"
    R_IND = [23540, 23541, 23542]
    N_IND = [990422, 990423, 990424, 990425, 990426, 990427, 990428, 990429, 990430, 990431, 990432, 990433, 990434, 990435, 990436, 990437, 990438, 990439, 990440, 990441, 990442, 990443, 990444, 990445, 990446, 990447, 990448, 990449, 990450, 990451, 990452, 990453, 990454, 990455, 990456, 990457, 990458, 990459, 990460, 990461, 990462, 990463, 990464, 990465, 990466, 990467, 990468, 990469, 990470, 990471, 990472, 990473, 990474, 990475, 990476, 990477, 990478, 990479, 990480, 990481, 990482, 990483, 990484, 990485, 990486, 990487, 990488, 990489, 990490, 990491, 990492, 990493, 990494, 990495, 990496, 990497, 990498, 990499, 990500, 990501, 990502, 990503, 990504, 990505, 990506, 990507, 990508, 990509, 990510, 990511, 990512, 990513, 990514, 990515, 990516, 990517, 990518, 990519, 990520, 990521, 990522, 990523, 990524, 990525, 990526, 990527, 990528, 990529, 990530, 990531, 990532, 990533, 990534, 990535, 990536, 990537, 990538, 990539, 990540, 990541, 990542, 990543, 990544, 990545, 990546, 990547, 990548, 990549, 990550, 990551, 990552, 990553, 990554, 990555, 990556, 990557, 990558, 990559, 990560, 990561, 990562, 990563, 990564, 990565, 990566, 990567, 990568, 990569, 990570, 990571, 990572, 990573, 990574, 990575, 990576, 990577, 990578, 990579, 990580, 990581, 990582, 990583, 990584, 990585, 990586, 990587, 990588, 990589, 990590, 990591, 990592, 990593, 990594, 990595, 990596, 990597, 990598, 990599, 990600, 990601, 990602, 990603, 990604, 990605, 990606, 990607, 990608, 990609, 990610, 990611, 990612, 990613, 990614, 990615, 990616, 990617, 990618, 990619, 990620, 990621, 990622, 990623, 990624, 990625, 990626, 990627, 990628, 990629, 990630, 990631, 990632]
    REACH_ID = 77449100061

    def test_extract_sos(self):
        """Tests extract_sos method of GBPriorsGenerate"""

        sos_dict = extract_sos(self.SOS_FILE)
        self.assertAlmostEqual(318.72738647, sos_dict["qhat"][self.R_IND[1]])

    def test_extract_swot(self):
        """Tests extract_swot method of GBPriorsRead"""

        sos_dict = extract_sos(self.SOS_FILE)
        qhat = np.array([sos_dict["qhat"][self.R_IND[1]]])
        
        swot_dict = extract_swot(self.SWOT_DIR / "77449100061_SWOT.nc", qhat)

        expected_width = np.array([43.208299, 52.908047, 62.901061, 55.131285, 85.893091, 64.368724, 44.241324, 50.019747, 49.556449, 56.568242, 30.668317, 66.232694, 60.893438, 57.955582, 50.442531, 58.72346, 44.137341])
        np.testing.assert_array_almost_equal(expected_width, swot_dict["node"]["width"][0,:]) 
        expected_slope = np.array([0.00010045794, 0.00010173043, 9.540541e-05, 0.00011160423, 9.765124e-05, 0.00010503138, 8.985157e-05, 9.279268e-05, 0.00010460104, 0.00010018548, 0.00013338136, 0.00010086814, 0.00011058383, 9.262967e-05, 1.900279e-05, 6.059819e-05, 8.804341e-05])
        np.testing.assert_array_almost_equal(expected_slope, swot_dict["node"]["slope2"][0,:])
        self.assertAlmostEqual(318.72738647, swot_dict["node"]["Qhat"][0])
        expected_dxa = np.array([-842.2044029, -551.766112, -789.73331382, -805.94259508, -562.88294229, -463.77205172, -539.18129047, -480.70395691, -577.45243603, -642.45921666, -569.39455673, -583.28948871, -563.43122025, -527.68729855, -596.39991641, -662.4912515, -902.31105895])
        np.testing.assert_array_almost_equal(expected_dxa, swot_dict["node"]["d_x_area"][0,:])

    def test_check_observations_valid_node(self):
        """Tests check_observations function of GBPriorsRead for valid data"""

        swot_file = self.SWOT_DIR / "77449100061_SWOT.nc"
        swot = Dataset(Path(swot_file))
        width = swot["node/width"][:].filled(np.nan)
        d_x_area = swot["node/d_x_area"][:].filled(np.nan)
        slope2 = swot["node/slope2"][:].filled(np.nan)
        qhat = np.repeat(402.06541002, width.shape[0])
        swot.close()

        data_dict = check_observations_node(width, d_x_area, slope2, qhat)
        np.testing.assert_array_equal(np.array([]), data_dict["invalid_indexes"])
        self.assertEqual((49,17), data_dict["width"].shape)
        self.assertEqual((49,17), data_dict["d_x_area"].shape)
        self.assertEqual((49,17), data_dict["slope2"].shape)
        self.assertEqual((49,), data_dict["Qhat"].shape)

    def test_check_observations_valid_reach(self):
        """Tests check_observations function of GBPriorsRead for valid data"""

        swot_file = self.SWOT_DIR / "77449100061_SWOT.nc"
        swot = Dataset(Path(swot_file))
        width = swot["reach/width"][:].filled(np.nan)
        d_x_area = swot["reach/d_x_area"][:].filled(np.nan)
        slope2 = swot["reach/slope2"][:].filled(np.nan)
        qhat = np.array([402.06541002])
        swot.close()

        data_dict = check_observations_reach(width, d_x_area, slope2, qhat)
        self.assertEqual((17,), data_dict["width"].shape)
        self.assertEqual((17,), data_dict["d_x_area"].shape)
        self.assertEqual((17,), data_dict["slope2"].shape)
        self.assertEqual((1,), data_dict["Qhat"].shape)

    def test_check_observations_invalid(self):
        """Tests check_observations function of GBPriorsRead for invalid data"""

        swot_file = self.SWOT_DIR / "77449100061_SWOT.nc"
        swot = Dataset(Path(swot_file))
        width = np.full((49,25), fill_value=np.nan)
        d_x_area = swot["node/d_x_area"][:].filled(np.nan)
        slope2 = swot["node/slope2"][:].filled(np.nan)
        qhat = np.repeat(402.06541002, width.shape[0])
        swot.close()

        data_dict = check_observations_node(width, d_x_area, slope2, qhat)
        self.assertFalse(data_dict)

    def test_is_valid_valid(self):
        """Tests is_valid function of GBPriorsRead for valid data."""

        swot_file = self.SWOT_DIR / "77449100061_SWOT.nc"
        swot = Dataset(Path(swot_file))
        width = swot["node/width"][:].filled(np.nan)
        swot.close()

        data_dict = is_valid_node(width)
        self.assertTrue(data_dict["valid"])
        self.assertCountEqual([1, 4, 7, 10, 13, 16, 19, 22], data_dict["invalid_times"][0].tolist())
        np.testing.assert_array_equal(np.array([]), data_dict["invalid_nodes"][0])

        nans = np.full((25,), fill_value=np.nan)
        width[:3,:] = nans
        data_dict = is_valid_node(width)
        self.assertCountEqual([1, 4, 7, 10, 13, 16, 19, 22], data_dict["invalid_times"][0].tolist())
        np.testing.assert_array_equal(np.array([0,1,2]), data_dict["invalid_nodes"][0])

    def test_is_valid_valid(self):
        """Tests is_valid function of GBPriorsRead for valid data."""

        swot_file = self.SWOT_DIR / "77449100061_SWOT.nc"
        swot = Dataset(Path(swot_file))
        width = swot["reach/width"][:].filled(np.nan)
        swot.close()

        data_dict = is_valid_reach(width)
        self.assertTrue(data_dict["valid"])
        self.assertCountEqual([1, 4, 7, 10, 13, 16, 19, 22], data_dict["invalid_times"])

        nans = np.full((25,), fill_value=np.nan)
        width[:3] = np.nan
        data_dict = is_valid_reach(width)
        self.assertCountEqual([0, 1, 2, 4, 7, 10, 13, 16, 19, 22], data_dict["invalid_times"])

    def test_is_valid_invalid(self):
        """Tests is_valid function of GBPriorsRead for invalid data."""

        width = np.full((49,25), fill_value=np.nan)
        data_dict = is_valid_node(width)
        self.assertFalse(data_dict["valid"])
        self.assertFalse(data_dict["invalid_nodes"])
        self.assertFalse(data_dict["invalid_times"])

    def test_run_gb(self):
        """Tests run_gb method."""
        
        # Run geoBAM priors
        gen = GBPriorsGenerate(self.SOS_FILE, self.SWOT_DIR)
        gen.run_gb()
        
        # Assert reach results
        priors = gen.gb_dict["reach"]
        self.assertEqual(1, priors["river_type"][23541])
        self.assertAlmostEqual(1.47, priors["lowerbound_A0"][23541], places=2)
        self.assertAlmostEqual(114500, priors["upperbound_A0"][23541], places=0)
        self.assertAlmostEqual(-4.60517, priors["lowerbound_logn"][23541], places=5)
        self.assertAlmostEqual(-2.995732, priors["upperbound_logn"][23541], places=5)
        self.assertAlmostEqual(0.004872167, priors["lowerbound_b"][23541], places=5)
        self.assertAlmostEqual(0.7737576, priors["upperbound_b"][23541], places=5)
        self.assertAlmostEqual(-0.1227328, priors["lowerbound_logWb"][23541], places=5)
        self.assertAlmostEqual(6.91726, priors["upperbound_logWb"][23541], places=5)
        self.assertAlmostEqual(-2.241242, priors["lowerbound_logDb"][23541], places=5)
        self.assertAlmostEqual(3.309359, priors["upperbound_logDb"][23541], places=5)
        self.assertAlmostEqual(-1.694463, priors["lowerbound_logr"][23541], places=5)
        self.assertAlmostEqual(3.885279, priors["upperbound_logr"][23541], places=5)

        self.assertAlmostEqual(5.538318, priors["logA0_hat"][23541], places=6)
        self.assertAlmostEqual(-3.293182, priors["logn_hat"][23541], places=6)
        self.assertAlmostEqual(0.1939656, priors["b_hat"][23541], places=6)
        self.assertAlmostEqual(3.57473, priors["logWb_hat"][23541], places=6)
        self.assertAlmostEqual(-0.4774545, priors["logDb_hat"][23541], places=6)
        self.assertAlmostEqual(0.722592, priors["logr_hat"][23541], places=5)
        self.assertAlmostEqual(1.831509, priors["logA0_sd"][23541], places=6)

        self.assertAlmostEqual(1.137136, priors["logn_sd"][23541], places=5)
        self.assertAlmostEqual(0.1236208, priors["b_sd"][23541], places=7)
        self.assertAlmostEqual(1.026266, priors["logWb_sd"][23541], places=6)
        self.assertAlmostEqual(0.909913, priors["logDb_sd"][23541], places=7)
        self.assertAlmostEqual(0.7968415, priors["logr_sd"][23541], places=7)

        self.assertAlmostEqual(1, priors["lowerbound_logWc"][23541])
        self.assertAlmostEqual(8, priors["upperbound_logWc"][23541])
        self.assertAlmostEqual(0.01, priors["lowerbound_logQc"][23541])
        self.assertAlmostEqual(10, priors["upperbound_logQc"][23541])
        self.assertAlmostEqual(4.427522, priors["logWc_hat"][23541], places=6)
        self.assertAlmostEqual(5.764336, priors["logQc_hat"][23541], places=6)
        self.assertAlmostEqual(0.8325546, priors["logQ_sd"][23541], places=7)
        self.assertAlmostEqual(0.00999975, priors["logWc_sd"][23541], places=6)
        self.assertAlmostEqual(0.8325546, priors["logQc_sd"][23541])
        self.assertAlmostEqual(10, priors["Werr_sd"][23541])
        self.assertAlmostEqual(1e-05, priors["Serr_sd"][23541], places=5)
        self.assertAlmostEqual(10, priors["dAerr_sd"][23541])
        self.assertAlmostEqual(0.25, priors["sigma_man"][23541])
        self.assertAlmostEqual(0.22, priors["sigma_amhg"][23541])

        # # Assert node results    # TODO SWORD v11 discrepancy
        # priors = gen.gb_dict["node"]
        # nod_i = np.where(self.REACH_ID == sword_dict["reach_node_ids"])
        # e_rt = np.array([9, 8, 11, 11, 11, 11, 12, 11, 10, 10, 10, 11, 11, 12, 10, 11, 12, 11, 11, 12, 11, 12, 12, 13, 13, 11, 11, 12, 13, 11, 11, 11, 12, 11, 12, 12, 12, 12, 12, 13, 13, 12, 8, 11, 10, 12, 12, 12, 12, 10, 10, 12, 13, 13, 13, 13, 13, 12, 13, 13, 13, 11])
        # assert_array_equal(e_rt, priors["river_type"][nod_i])
        # self.assertAlmostEqual(4.69, priors["lowerbound_A0"][1])
        # self.assertAlmostEqual(8220, priors["upperbound_A0"][1], places=0)
        # self.assertAlmostEqual(-4.60517, priors["lowerbound_logn"][1], places=5)
        # self.assertAlmostEqual(-2.995732, priors["upperbound_logn"][1], places=5)
        # self.assertAlmostEqual(0.005703884, priors["lowerbound_b"][1], places=5)
        # self.assertAlmostEqual(0.4324974, priors["upperbound_b"][1], places=5)
        # self.assertAlmostEqual(0.4219944, priors["lowerbound_logWb"][1], places=5)
        # self.assertAlmostEqual(5.860074, priors["upperbound_logWb"][1], places=5)
        # self.assertAlmostEqual(-1.80917, priors["lowerbound_logDb"][1], places=5)
        # self.assertAlmostEqual(1.765117, priors["upperbound_logDb"][1], places=5)
        # self.assertAlmostEqual(0.007552884, priors["lowerbound_logr"][1], places=5)
        # self.assertAlmostEqual(4.058324, priors["upperbound_logr"][1], places=5)
        # e_A0hat = np.array([5.774532256, 5.523458921, 6.527953643, 6.527953643, 6.527953643, 6.527953643, 6.873163834, 6.527953643, 6.446836611, 6.446836611, 6.446836611, 6.527953643, 6.527953643, 6.873163834, 6.446836611, 6.527953643, 6.873163834, 6.527953643, 6.527953643, 6.873163834, 6.527953643, 6.873163834, 6.873163834, 7.102499356, 7.102499356, 6.527953643, 6.527953643, 6.873163834, 7.102499356, 6.527953643, 6.527953643, 6.527953643, 6.873163834, 6.527953643, 6.873163834, 6.873163834, 6.873163834, 6.873163834, 6.873163834, 7.102499356, 7.102499356, 6.873163834, 5.523458921, 6.527953643, 6.446836611, 6.873163834, 6.873163834, 6.873163834, 6.873163834, 6.446836611, 6.446836611, 6.873163834, 7.102499356, 7.102499356, 7.102499356, 7.102499356, 7.102499356, 6.873163834, 7.102499356, 7.102499356, 7.102499356, 6.527953643])
        # assert_array_almost_equal(e_A0hat, priors["logA0_hat"][nod_i])
        # e_nhat = np.array([-3.240085716, -3.545592423, -3.269861085, -3.269861085, -3.269861085, -3.269861085, -3.372948626, -3.269861085, -3.401877538, -3.401877538, -3.401877538, -3.269861085, -3.269861085, -3.372948626, -3.401877538, -3.269861085, -3.372948626, -3.269861085, -3.269861085, -3.372948626, -3.269861085, -3.372948626, -3.372948626, -3.404202242, -3.404202242, -3.269861085, -3.269861085, -3.372948626, -3.404202242, -3.269861085, -3.269861085, -3.269861085, -3.372948626, -3.269861085, -3.372948626, -3.372948626, -3.372948626, -3.372948626, -3.372948626, -3.404202242, -3.404202242, -3.372948626, -3.545592423, -3.269861085, -3.401877538, -3.372948626, -3.372948626, -3.372948626, -3.372948626, -3.401877538, -3.401877538, -3.372948626, -3.404202242, -3.404202242, -3.404202242, -3.404202242, -3.404202242, -3.372948626, -3.404202242, -3.404202242, -3.404202242, -3.269861085])
        # assert_array_almost_equal(e_nhat, priors["logn_hat"][nod_i])
        # e_bhat = np.array([0.164989336, 0.176051924, 0.179925708, 0.179925708, 0.179925708, 0.179925708, 0.151049254, 0.179925708, 0.14444077, 0.14444077, 0.14444077, 0.179925708, 0.179925708, 0.151049254, 0.14444077, 0.179925708, 0.151049254, 0.179925708, 0.179925708, 0.151049254, 0.179925708, 0.151049254, 0.151049254, 0.107294865, 0.107294865, 0.179925708, 0.179925708, 0.151049254, 0.107294865, 0.179925708, 0.179925708, 0.179925708, 0.151049254, 0.179925708, 0.151049254, 0.151049254, 0.151049254, 0.151049254, 0.151049254, 0.107294865, 0.107294865, 0.151049254, 0.176051924, 0.179925708, 0.14444077, 0.151049254, 0.151049254, 0.151049254, 0.151049254, 0.14444077, 0.14444077, 0.151049254, 0.107294865, 0.107294865, 0.107294865, 0.107294865, 0.107294865, 0.151049254, 0.107294865, 0.107294865, 0.107294865, 0.179925708])
        # assert_array_almost_equal(e_bhat, priors["b_hat"][nod_i])
        # e_wbhat = np.array([3.683922384, 3.664586762, 4.032912323, 4.032912323, 4.032912323, 4.032912323, 4.357733942, 4.032912323, 4.002696788, 4.002696788, 4.002696788, 4.032912323, 4.032912323, 4.357733942, 4.002696788, 4.032912323, 4.357733942, 4.032912323, 4.032912323, 4.357733942, 4.032912323, 4.357733942, 4.357733942, 4.436574004, 4.436574004, 4.032912323, 4.032912323, 4.357733942, 4.436574004, 4.032912323, 4.032912323, 4.032912323, 4.357733942, 4.032912323, 4.357733942, 4.357733942, 4.357733942, 4.357733942, 4.357733942, 4.436574004, 4.436574004, 4.357733942, 3.664586762, 4.032912323, 4.002696788, 4.357733942, 4.357733942, 4.357733942, 4.357733942, 4.002696788, 4.002696788, 4.357733942, 4.436574004, 4.436574004, 4.436574004, 4.436574004, 4.436574004, 4.357733942, 4.436574004, 4.436574004, 4.436574004, 4.032912323])
        # assert_array_almost_equal(e_wbhat, priors["logWb_hat"][nod_i])
        # e_dbhat = np.array([-0.480357291, -0.547540836, -0.07180952, -0.07180952, -0.07180952, -0.07180952, 0.15467964, -0.07180952, 0.130087152, 0.130087152, 0.130087152, -0.07180952, -0.07180952, 0.15467964, 0.130087152, -0.07180952, 0.15467964, -0.07180952, -0.07180952, 0.15467964, -0.07180952, 0.15467964, 0.15467964, 0.299392088, 0.299392088, -0.07180952, -0.07180952, 0.15467964, 0.299392088, -0.07180952, -0.07180952, -0.07180952, 0.15467964, -0.07180952, 0.15467964, 0.15467964, 0.15467964, 0.15467964, 0.15467964, 0.299392088, 0.299392088, 0.15467964, -0.547540836, -0.07180952, 0.130087152, 0.15467964, 0.15467964, 0.15467964, 0.15467964, 0.130087152, 0.130087152, 0.15467964, 0.299392088, 0.299392088, 0.299392088, 0.299392088, 0.299392088, 0.15467964, 0.299392088, 0.299392088, 0.299392088, -0.07180952])
        # assert_array_almost_equal(e_dbhat, priors["logDb_hat"][nod_i])
        # e_rhat = np.array([1.012946569, 0.914703855, 0.964521619, 0.964521619, 0.964521619, 0.964521619, 1.007259927, 0.964521619, 1.012082534, 1.012082534, 1.012082534, 0.964521619, 0.964521619, 1.007259927, 1.012082534, 0.964521619, 1.007259927, 0.964521619, 0.964521619, 1.007259927, 0.964521619, 1.007259927, 1.007259927, 1.368493142, 1.368493142, 0.964521619, 0.964521619, 1.007259927, 1.368493142, 0.964521619, 0.964521619, 0.964521619, 1.007259927, 0.964521619, 1.007259927, 1.007259927, 1.007259927, 1.007259927, 1.007259927, 1.368493142, 1.368493142, 1.007259927, 0.914703855, 0.964521619, 1.012082534, 1.007259927, 1.007259927, 1.007259927, 1.007259927, 1.012082534, 1.012082534, 1.007259927, 1.368493142, 1.368493142, 1.368493142, 1.368493142, 1.368493142, 1.007259927, 1.368493142, 1.368493142, 1.368493142, 0.964521619])
        # assert_array_almost_equal(e_rhat, priors["logr_hat"][nod_i])
        # e_a0sd = np.array([1.21047899, 1.337220271, 1.245863462, 1.245863462, 1.245863462, 1.245863462, 1.287297345, 1.245863462, 1.359096608, 1.359096608, 1.359096608, 1.245863462, 1.245863462, 1.287297345, 1.359096608, 1.245863462, 1.287297345, 1.245863462, 1.245863462, 1.287297345, 1.245863462, 1.287297345, 1.287297345, 1.08535437, 1.08535437, 1.245863462, 1.245863462, 1.287297345, 1.08535437, 1.245863462, 1.245863462, 1.245863462, 1.287297345, 1.245863462, 1.287297345, 1.287297345, 1.287297345, 1.287297345, 1.287297345, 1.08535437, 1.08535437, 1.287297345, 1.337220271, 1.245863462, 1.359096608, 1.287297345, 1.287297345, 1.287297345, 1.287297345, 1.359096608, 1.359096608, 1.287297345, 1.08535437, 1.08535437, 1.08535437, 1.08535437, 1.08535437, 1.287297345, 1.08535437, 1.08535437, 1.08535437, 1.245863462])
        # assert_array_almost_equal(e_a0sd, priors["logA0_sd"][nod_i])
        # e_nsd = np.array([1.101795647, 1.229666517, 1.268715794, 1.268715794, 1.268715794, 1.268715794, 1.260820049, 1.268715794, 1.194241604, 1.194241604, 1.194241604, 1.268715794, 1.268715794, 1.260820049, 1.194241604, 1.268715794, 1.260820049, 1.268715794, 1.268715794, 1.260820049, 1.268715794, 1.260820049, 1.260820049, 1.199816945, 1.199816945, 1.268715794, 1.268715794, 1.260820049, 1.199816945, 1.268715794, 1.268715794, 1.268715794, 1.260820049, 1.268715794, 1.260820049, 1.260820049, 1.260820049, 1.260820049, 1.260820049, 1.199816945, 1.199816945, 1.260820049, 1.229666517, 1.268715794, 1.194241604, 1.260820049, 1.260820049, 1.260820049, 1.260820049, 1.194241604, 1.194241604, 1.260820049, 1.199816945, 1.199816945, 1.199816945, 1.199816945, 1.199816945, 1.260820049, 1.199816945, 1.199816945, 1.199816945, 1.268715794])
        # assert_array_almost_equal(e_nsd, priors["logn_sd"][nod_i])
        # e_bsd = np.array([0.100083519, 0.0962566, 0.099927194, 0.099927194, 0.099927194, 0.099927194, 0.098731293, 0.099927194, 0.10412018, 0.10412018, 0.10412018, 0.099927194, 0.099927194, 0.098731293, 0.10412018, 0.099927194, 0.098731293, 0.099927194, 0.099927194, 0.098731293, 0.099927194, 0.098731293, 0.098731293, 0.099282596, 0.099282596, 0.099927194, 0.099927194, 0.098731293, 0.099282596, 0.099927194, 0.099927194, 0.099927194, 0.098731293, 0.099927194, 0.098731293, 0.098731293, 0.098731293, 0.098731293, 0.098731293, 0.099282596, 0.099282596, 0.098731293, 0.0962566, 0.099927194, 0.10412018, 0.098731293, 0.098731293, 0.098731293, 0.098731293, 0.10412018, 0.10412018, 0.098731293, 0.099282596, 0.099282596, 0.099282596, 0.099282596, 0.099282596, 0.098731293, 0.099282596, 0.099282596, 0.099282596, 0.099927194])
        # assert_array_almost_equal(e_bsd, priors["b_sd"][nod_i])
        # e_wbsd = np.array([0.691428554, 0.812622517, 0.587954845, 0.587954845, 0.587954845, 0.587954845, 0.728133987, 0.587954845, 0.654155675, 0.654155675, 0.654155675, 0.587954845, 0.587954845, 0.728133987, 0.654155675, 0.587954845, 0.728133987, 0.587954845, 0.587954845, 0.728133987, 0.587954845, 0.728133987, 0.728133987, 0.540567805, 0.540567805, 0.587954845, 0.587954845, 0.728133987, 0.540567805, 0.587954845, 0.587954845, 0.587954845, 0.728133987, 0.587954845, 0.728133987, 0.728133987, 0.728133987, 0.728133987, 0.728133987, 0.540567805, 0.540567805, 0.728133987, 0.812622517, 0.587954845, 0.654155675, 0.728133987, 0.728133987, 0.728133987, 0.728133987, 0.654155675, 0.654155675, 0.728133987, 0.540567805, 0.540567805, 0.540567805, 0.540567805, 0.540567805, 0.728133987, 0.540567805, 0.540567805, 0.540567805, 0.587954845])
        # assert_array_almost_equal(e_wbsd, priors["logWb_sd"][nod_i])
        # e_dbsd = np.array([0.662592139, 0.665457667, 0.764674068, 0.764674068, 0.764674068, 0.764674068, 0.719504763, 0.764674068, 0.810898004, 0.810898004, 0.810898004, 0.764674068, 0.764674068, 0.719504763, 0.810898004, 0.764674068, 0.719504763, 0.764674068, 0.764674068, 0.719504763, 0.764674068, 0.719504763, 0.719504763, 0.690395904, 0.690395904, 0.764674068, 0.764674068, 0.719504763, 0.690395904, 0.764674068, 0.764674068, 0.764674068, 0.719504763, 0.764674068, 0.719504763, 0.719504763, 0.719504763, 0.719504763, 0.719504763, 0.690395904, 0.690395904, 0.719504763, 0.665457667, 0.764674068, 0.810898004, 0.719504763, 0.719504763, 0.719504763, 0.719504763, 0.810898004, 0.810898004, 0.719504763, 0.690395904, 0.690395904, 0.690395904, 0.690395904, 0.690395904, 0.719504763, 0.690395904, 0.690395904, 0.690395904, 0.764674068])
        # assert_array_almost_equal(e_dbsd, priors["logDb_sd"][nod_i])
        # e_rsd = np.array([0.678734433, 0.721545901, 0.714799269, 0.714799269, 0.714799269, 0.714799269, 0.783012838, 0.714799269, 0.73732522, 0.73732522, 0.73732522, 0.714799269, 0.714799269, 0.783012838, 0.73732522, 0.714799269, 0.783012838, 0.714799269, 0.714799269, 0.783012838, 0.714799269, 0.783012838, 0.783012838, 0.824128369, 0.824128369, 0.714799269, 0.714799269, 0.783012838, 0.824128369, 0.714799269, 0.714799269, 0.714799269, 0.783012838, 0.714799269, 0.783012838, 0.783012838, 0.783012838, 0.783012838, 0.783012838, 0.824128369, 0.824128369, 0.783012838, 0.721545901, 0.714799269, 0.73732522, 0.783012838, 0.783012838, 0.783012838, 0.783012838, 0.73732522, 0.73732522, 0.783012838, 0.824128369, 0.824128369, 0.824128369, 0.824128369, 0.824128369, 0.783012838, 0.824128369, 0.824128369, 0.824128369, 0.714799269])
        # assert_array_almost_equal(e_rsd, priors["logr_sd"][nod_i])
        # self.assertAlmostEqual(1, priors["lowerbound_logWc"][1])
        # self.assertAlmostEqual(8, priors["upperbound_logWc"][1])
        # self.assertAlmostEqual(0.01, priors["lowerbound_logQc"][1])
        # self.assertAlmostEqual(10, priors["upperbound_logQc"][1])
        # self.assertAlmostEqual(4.186711, priors["logWc_hat"][1], places=6)
        # self.assertAlmostEqual(5.764336, priors["logQc_hat"][1], places=6)
        # self.assertAlmostEqual(0.8325546, priors["logQ_sd"][1], places=7)
        # self.assertAlmostEqual(4.712493, priors["logWc_sd"][1], places=6)
        # self.assertAlmostEqual(0.8325546, priors["logQc_sd"][1])
        # self.assertAlmostEqual(10, priors["Werr_sd"][1])
        # self.assertAlmostEqual(1e-05, priors["Serr_sd"][1], places=5)
        # self.assertAlmostEqual(10, priors["dAerr_sd"][1])
        # e_sman = np.array([0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25])
        # assert_array_almost_equal(e_sman, priors["sigma_man"][nod_i])
        # e_samhg = np.array([0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22])
        # assert_array_almost_equal(e_samhg, priors["sigma_amhg"][nod_i])
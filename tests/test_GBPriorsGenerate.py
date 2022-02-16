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
    N_IND = [990518, 990519, 990520, 990521, 990522, 990523, 990524, 990525, 990526, 990527, 990528, 990529, 990530, 990531, 990532, 990533, 990534, 990535, 990536, 990537, 990538, 990539, 990540, 990541, 990542, 990543, 990544, 990545, 990546, 990547, 990548, 990549, 990550, 990551, 990552, 990553, 990554, 990555, 990556, 990557, 990558, 990559, 990560, 990561, 990562, 990563, 990564, 990565, 990566, 990567, 990568, 990569, 990570, 990571, 990572, 990573, 990574, 990575, 990576, 990577, 990578, 990579]
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

        expected_width = np.array([77.30225, 91.560597, 57.872238, 79.297152, 79.798106, 75.757512, 76.508679, 88.732264, 92.114123, 108.047061, 89.474054, 92.706104, 108.687395, 103.444156, 73.585066, 84.77175])
        np.testing.assert_array_almost_equal(expected_width, swot_dict["node"]["width"][0,:]) 
        expected_slope = np.array([7.343824e-05, 7.678499e-05, 8.688711e-05, 8.339372e-05, 8.250481e-05, 8.422326e-05, 9.242077e-05, 8.498601e-05, 6.815372e-05, 8.981572e-05, 7.792714e-05, 0.00010252201, 8.291416e-05, 0.00010075065, 7.837592e-05, 8.660165e-05])
        np.testing.assert_array_almost_equal(expected_slope, swot_dict["node"]["slope2"][0,:])
        self.assertAlmostEqual(318.72738647, swot_dict["node"]["Qhat"][0])
        expected_dxa = np.array([-46.935, -43.0, 15.417, -32.745, -22.235, -1.065, 9.145, 21.616, -54.77, -48.977, 70.987, 20.533, 5.221, 33.015, -0.553, 23.082])
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
        self.assertEqual((62,16), data_dict["width"].shape)
        self.assertEqual((62,16), data_dict["d_x_area"].shape)
        self.assertEqual((62,16), data_dict["slope2"].shape)
        self.assertEqual((62,), data_dict["Qhat"].shape)

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
        self.assertEqual((16,), data_dict["width"].shape)
        self.assertEqual((16,), data_dict["d_x_area"].shape)
        self.assertEqual((16,), data_dict["slope2"].shape)
        self.assertEqual((1,), data_dict["Qhat"].shape)

    def test_check_observations_invalid(self):
        """Tests check_observations function of GBPriorsRead for invalid data"""

        swot_file = self.SWOT_DIR / "77449100061_SWOT.nc"
        swot = Dataset(Path(swot_file))
        width = np.full((62,16), fill_value=np.nan)
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
        self.assertCountEqual([9, 10, 11, 12, 13, 14, 15, 16], data_dict["invalid_times"][0].tolist())
        np.testing.assert_array_equal(np.array([]), data_dict["invalid_nodes"][0])

        nans = np.full((25,), fill_value=np.nan)
        width[:3,:] = nans
        data_dict = is_valid_node(width)
        self.assertCountEqual([9, 10, 11, 12, 13, 14, 15, 16], data_dict["invalid_times"][0].tolist())
        np.testing.assert_array_equal(np.array([0,1,2]), data_dict["invalid_nodes"][0])

    def test_is_valid_valid(self):
        """Tests is_valid function of GBPriorsRead for valid data."""

        swot_file = self.SWOT_DIR / "77449100061_SWOT.nc"
        swot = Dataset(Path(swot_file))
        width = swot["reach/width"][:].filled(np.nan)
        swot.close()

        data_dict = is_valid_reach(width)
        self.assertTrue(data_dict["valid"])
        self.assertCountEqual([9, 10, 12, 13, 14, 15, 16], data_dict["invalid_times"])

        width[:3] = np.nan
        data_dict = is_valid_reach(width)
        self.assertCountEqual([0, 1, 2, 9, 10, 12, 13, 14, 15, 16], data_dict["invalid_times"])

    def test_is_valid_invalid(self):
        """Tests is_valid function of GBPriorsRead for invalid data."""

        width = np.full((62,25), fill_value=np.nan)
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
        self.assertEqual(13, priors["river_type"][23541])
        self.assertAlmostEqual(40.3, priors["lowerbound_A0"][23541], places=2)
        self.assertAlmostEqual(6515, priors["upperbound_A0"][23541], places=0)
        self.assertAlmostEqual(-4.60517, priors["lowerbound_logn"][23541], places=5)
        self.assertAlmostEqual(-2.995732, priors["upperbound_logn"][23541], places=5)
        self.assertAlmostEqual(0.01625238, priors["lowerbound_b"][23541], places=5)
        self.assertAlmostEqual(0.3617013, priors["upperbound_b"][23541], places=5)
        self.assertAlmostEqual(2.994607, priors["lowerbound_logWb"][23541], places=5)
        self.assertAlmostEqual(5.521861, priors["upperbound_logWb"][23541], places=5)
        self.assertAlmostEqual(-1.649479, priors["lowerbound_logDb"][23541], places=5)
        self.assertAlmostEqual(1.639749, priors["upperbound_logDb"][23541], places=5)
        self.assertAlmostEqual(0.08117157, priors["lowerbound_logr"][23541], places=5)
        self.assertAlmostEqual(3.688941, priors["upperbound_logr"][23541], places=5)

        self.assertAlmostEqual(7.102499, priors["logA0_hat"][23541], places=6)
        self.assertAlmostEqual(-3.404202, priors["logn_hat"][23541], places=6)
        self.assertAlmostEqual(0.1072949, priors["b_hat"][23541], places=6)
        self.assertAlmostEqual(4.436574, priors["logWb_hat"][23541], places=6)
        self.assertAlmostEqual(0.2993921, priors["logDb_hat"][23541], places=6)
        self.assertAlmostEqual(1.368493, priors["logr_hat"][23541], places=5)
        self.assertAlmostEqual(1.085354, priors["logA0_sd"][23541], places=6)

        self.assertAlmostEqual(1.199817, priors["logn_sd"][23541], places=5)
        self.assertAlmostEqual(0.0992826, priors["b_sd"][23541], places=7)
        self.assertAlmostEqual(0.5405678, priors["logWb_sd"][23541], places=6)
        self.assertAlmostEqual(0.6903959, priors["logDb_sd"][23541], places=7)
        self.assertAlmostEqual(0.8241284, priors["logr_sd"][23541], places=7)

        self.assertAlmostEqual(1, priors["lowerbound_logWc"][23541])
        self.assertAlmostEqual(8, priors["upperbound_logWc"][23541])
        self.assertAlmostEqual(0.01, priors["lowerbound_logQc"][23541])
        self.assertAlmostEqual(10, priors["upperbound_logQc"][23541])
        self.assertAlmostEqual(4.508255, priors["logWc_hat"][23541], places=6)
        self.assertAlmostEqual(5.764336, priors["logQc_hat"][23541], places=6)
        self.assertAlmostEqual(0.8325546, priors["logQ_sd"][23541], places=7)
        self.assertAlmostEqual(4.712493, priors["logWc_sd"][23541], places=6)
        self.assertAlmostEqual(0.8325546, priors["logQc_sd"][23541])
        self.assertAlmostEqual(10, priors["Werr_sd"][23541])
        self.assertAlmostEqual(1e-05, priors["Serr_sd"][23541], places=5)
        self.assertAlmostEqual(10, priors["dAerr_sd"][23541])
        self.assertAlmostEqual(0.25, priors["sigma_man"][23541])
        self.assertAlmostEqual(0.22, priors["sigma_amhg"][23541])
        self.assertEqual(1, priors["overwritten_indexes"][23541])

        # Assert node results
        priors = gen.gb_dict["node"]
        e_rt = np.array([13, 12, 13, 12, 12, 13, 13, 14, 13, 12, 12, 13, 13, 13, 12, 16, 11, 12, 13, 12, 12, 12, 13, 13, 12, 13, 12, 12, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 12, 13, 12, 13, 13, 14, 13, 13, 13, 13, 13, 13, 10, 12, 13, 13, 13, 13, 13, 14, 14, 13, 13, 14])
        assert_array_equal(e_rt, priors["river_type"][self.N_IND])
        self.assertAlmostEqual(1.3, priors["lowerbound_A0"][23541])
        self.assertAlmostEqual(104000, priors["upperbound_A0"][23541], places=0)
        self.assertAlmostEqual(-4.60517, priors["lowerbound_logn"][23541], places=5)
        self.assertAlmostEqual(-2.995732, priors["upperbound_logn"][23541], places=5)
        self.assertAlmostEqual(0.01097407, priors["lowerbound_b"][23541], places=5)
        self.assertAlmostEqual(0.7737576, priors["upperbound_b"][23541], places=5)
        self.assertAlmostEqual(-0.1227328, priors["lowerbound_logWb"][23541], places=5)
        self.assertAlmostEqual(6.91726, priors["upperbound_logWb"][23541], places=5)
        self.assertAlmostEqual(-2.379582, priors["lowerbound_logDb"][23541], places=5)
        self.assertAlmostEqual(2.571817, priors["upperbound_logDb"][23541], places=5)
        self.assertAlmostEqual(-2.580471, priors["lowerbound_logr"][23541], places=5)
        self.assertAlmostEqual(3.759677, priors["upperbound_logr"][23541], places=5)
        e_A0hat = np.array([7.102499, 6.873164, 7.102499, 6.873164, 6.873164, 7.102499, 7.102499, 8.007965, 7.102499, 6.873164, 6.873164, 7.102499, 7.102499, 7.102499, 6.873164, 4.432007, 6.527954, 6.873164, 7.102499, 6.873164, 6.873164, 6.873164, 7.102499, 7.102499, 6.873164, 7.102499, 6.873164, 6.873164, 7.102499, 7.102499, 7.102499, 7.102499, 7.102499, 7.102499, 7.102499, 7.102499, 7.102499, 7.102499, 6.873164, 7.102499, 6.873164, 7.102499, 7.102499, 8.007965, 7.102499, 7.102499, 7.102499, 7.102499, 7.102499, 7.102499, 6.446837, 6.873164, 7.102499, 7.102499, 7.102499, 7.102499, 7.102499, 8.007965, 8.007965, 7.102499, 7.102499, 8.007965])
        assert_array_almost_equal(e_A0hat, priors["logA0_hat"][self.N_IND])
        e_nhat = np.array([-3.404202242, -3.372948626, -3.404202242, -3.372948626, -3.372948626, -3.404202242, -3.404202242, -3.274729636, -3.404202242, -3.372948626, -3.372948626, -3.404202242, -3.404202242, -3.404202242, -3.372948626, -3.318138928, -3.269861085, -3.372948626, -3.404202242, -3.372948626, -3.372948626, -3.372948626, -3.404202242, -3.404202242, -3.372948626, -3.404202242, -3.372948626, -3.372948626, -3.404202242, -3.404202242, -3.404202242, -3.404202242, -3.404202242, -3.404202242, -3.404202242, -3.404202242, -3.404202242, -3.404202242, -3.372948626, -3.404202242, -3.372948626, -3.404202242, -3.404202242, -3.274729636, -3.404202242, -3.404202242, -3.404202242, -3.404202242, -3.404202242, -3.404202242, -3.401877538, -3.372948626, -3.404202242, -3.404202242, -3.404202242, -3.404202242, -3.404202242, -3.274729636, -3.274729636, -3.404202242, -3.404202242, -3.274729636])
        assert_array_almost_equal(e_nhat, priors["logn_hat"][self.N_IND])
        e_bhat = np.array([0.107294865, 0.151049254, 0.107294865, 0.151049254, 0.151049254, 0.107294865, 0.107294865, 0.125720128, 0.107294865, 0.151049254, 0.151049254, 0.107294865, 0.107294865, 0.107294865, 0.151049254, 0.404257196, 0.179925708, 0.151049254, 0.107294865, 0.151049254, 0.151049254, 0.151049254, 0.107294865, 0.107294865, 0.151049254, 0.107294865, 0.151049254, 0.151049254, 0.107294865, 0.107294865, 0.107294865, 0.107294865, 0.107294865, 0.107294865, 0.107294865, 0.107294865, 0.107294865, 0.107294865, 0.151049254, 0.107294865, 0.151049254, 0.107294865, 0.107294865, 0.125720128, 0.107294865, 0.107294865, 0.107294865, 0.107294865, 0.107294865, 0.107294865, 0.14444077, 0.151049254, 0.107294865, 0.107294865, 0.107294865, 0.107294865, 0.107294865, 0.125720128, 0.125720128, 0.107294865, 0.107294865, 0.125720128])
        assert_array_almost_equal(e_bhat, priors["b_hat"][self.N_IND])
        
        e_wbhat = np.array([4.436574004, 4.357733942, 4.436574004, 4.357733942, 4.357733942, 4.436574004, 4.436574004, 4.921742348, 4.436574004, 4.357733942, 4.357733942, 4.436574004, 4.436574004, 4.436574004, 4.357733942, 3.032064203, 4.032912323, 4.357733942, 4.436574004, 4.357733942, 4.357733942, 4.357733942, 4.436574004, 4.436574004, 4.357733942, 4.436574004, 4.357733942, 4.357733942, 4.436574004, 4.436574004, 4.436574004, 4.436574004, 4.436574004, 4.436574004, 4.436574004, 4.436574004, 4.436574004, 4.436574004, 4.357733942, 4.436574004, 4.357733942, 4.436574004, 4.436574004, 4.921742348, 4.436574004, 4.436574004, 4.436574004, 4.436574004, 4.436574004, 4.436574004, 4.002696788, 4.357733942, 4.436574004, 4.436574004, 4.436574004, 4.436574004, 4.436574004, 4.921742348, 4.921742348, 4.436574004, 4.436574004, 4.921742348])
        assert_array_almost_equal(e_wbhat, priors["logWb_hat"][self.N_IND])
        e_dbhat = np.array([0.299392088, 0.15467964, 0.299392088, 0.15467964, 0.15467964, 0.299392088, 0.299392088, 0.579044554, 0.299392088, 0.15467964, 0.15467964, 0.299392088, 0.299392088, 0.299392088, 0.15467964, -0.974789558, -0.07180952, 0.15467964, 0.299392088, 0.15467964, 0.15467964, 0.15467964, 0.299392088, 0.299392088, 0.15467964, 0.299392088, 0.15467964, 0.15467964, 0.299392088, 0.299392088, 0.299392088, 0.299392088, 0.299392088, 0.299392088, 0.299392088, 0.299392088, 0.299392088, 0.299392088, 0.15467964, 0.299392088, 0.15467964, 0.299392088, 0.299392088, 0.579044554, 0.299392088, 0.299392088, 0.299392088, 0.299392088, 0.299392088, 0.299392088, 0.130087152, 0.15467964, 0.299392088, 0.299392088, 0.299392088, 0.299392088, 0.299392088, 0.579044554, 0.579044554, 0.299392088, 0.299392088, 0.579044554])
        assert_array_almost_equal(e_dbhat, priors["logDb_hat"][self.N_IND])
        e_rhat = np.array([1.368493142, 1.007259927, 1.368493142, 1.007259927, 1.007259927, 1.368493142, 1.368493142, 1.217111712, 1.368493142, 1.007259927, 1.007259927, 1.368493142, 1.368493142, 1.368493142, 1.007259927, -0.247245593, 0.964521619, 1.007259927, 1.368493142, 1.007259927, 1.007259927, 1.007259927, 1.368493142, 1.368493142, 1.007259927, 1.368493142, 1.007259927, 1.007259927, 1.368493142, 1.368493142, 1.368493142, 1.368493142, 1.368493142, 1.368493142, 1.368493142, 1.368493142, 1.368493142, 1.368493142, 1.007259927, 1.368493142, 1.007259927, 1.368493142, 1.368493142, 1.217111712, 1.368493142, 1.368493142, 1.368493142, 1.368493142, 1.368493142, 1.368493142, 1.012082534, 1.007259927, 1.368493142, 1.368493142, 1.368493142, 1.368493142, 1.368493142, 1.217111712, 1.217111712, 1.368493142, 1.368493142, 1.217111712])
        assert_array_almost_equal(e_rhat, priors["logr_hat"][self.N_IND])
        e_a0sd = np.array([1.08535437, 1.287297345, 1.08535437, 1.287297345, 1.287297345, 1.08535437, 1.08535437, 1.154319081, 1.08535437, 1.287297345, 1.287297345, 1.08535437, 1.08535437, 1.08535437, 1.287297345, 2.272342301, 1.245863462, 1.287297345, 1.08535437, 1.287297345, 1.287297345, 1.287297345, 1.08535437, 1.08535437, 1.287297345, 1.08535437, 1.287297345, 1.287297345, 1.08535437, 1.08535437, 1.08535437, 1.08535437, 1.08535437, 1.08535437, 1.08535437, 1.08535437, 1.08535437, 1.08535437, 1.287297345, 1.08535437, 1.287297345, 1.08535437, 1.08535437, 1.154319081, 1.08535437, 1.08535437, 1.08535437, 1.08535437, 1.08535437, 1.08535437, 1.359096608, 1.287297345, 1.08535437, 1.08535437, 1.08535437, 1.08535437, 1.08535437, 1.154319081, 1.154319081, 1.08535437, 1.08535437, 1.154319081])
        assert_array_almost_equal(e_a0sd, priors["logA0_sd"][self.N_IND])
        e_nsd = np.array([1.199816945, 1.260820049, 1.199816945, 1.260820049, 1.260820049, 1.199816945, 1.199816945, 1.345333569, 1.199816945, 1.260820049, 1.260820049, 1.199816945, 1.199816945, 1.199816945, 1.260820049, 1.122652969, 1.268715794, 1.260820049, 1.199816945, 1.260820049, 1.260820049, 1.260820049, 1.199816945, 1.199816945, 1.260820049, 1.199816945, 1.260820049, 1.260820049, 1.199816945, 1.199816945, 1.199816945, 1.199816945, 1.199816945, 1.199816945, 1.199816945, 1.199816945, 1.199816945, 1.199816945, 1.260820049, 1.199816945, 1.260820049, 1.199816945, 1.199816945, 1.345333569, 1.199816945, 1.199816945, 1.199816945, 1.199816945, 1.199816945, 1.199816945, 1.194241604, 1.260820049, 1.199816945, 1.199816945, 1.199816945, 1.199816945, 1.199816945, 1.345333569, 1.345333569, 1.199816945, 1.199816945, 1.345333569])
        assert_array_almost_equal(e_nsd, priors["logn_sd"][self.N_IND])
        e_bsd = np.array([0.099282596, 0.098731293, 0.099282596, 0.098731293, 0.098731293, 0.099282596, 0.099282596, 0.075781266, 0.099282596, 0.098731293, 0.098731293, 0.099282596, 0.099282596, 0.099282596, 0.098731293, 0.112180583, 0.099927194, 0.098731293, 0.099282596, 0.098731293, 0.098731293, 0.098731293, 0.099282596, 0.099282596, 0.098731293, 0.099282596, 0.098731293, 0.098731293, 0.099282596, 0.099282596, 0.099282596, 0.099282596, 0.099282596, 0.099282596, 0.099282596, 0.099282596, 0.099282596, 0.099282596, 0.098731293, 0.099282596, 0.098731293, 0.099282596, 0.099282596, 0.075781266, 0.099282596, 0.099282596, 0.099282596, 0.099282596, 0.099282596, 0.099282596, 0.10412018, 0.098731293, 0.099282596, 0.099282596, 0.099282596, 0.099282596, 0.099282596, 0.075781266, 0.075781266, 0.099282596, 0.099282596, 0.075781266])
        assert_array_almost_equal(e_bsd, priors["b_sd"][self.N_IND])
        e_wbsd = np.array([0.540567805, 0.728133987, 0.540567805, 0.728133987, 0.728133987, 0.540567805, 0.540567805, 0.680678526, 0.540567805, 0.728133987, 0.728133987, 0.540567805, 0.540567805, 0.540567805, 0.728133987, 1.268865384, 0.587954845, 0.728133987, 0.540567805, 0.728133987, 0.728133987, 0.728133987, 0.540567805, 0.540567805, 0.728133987, 0.540567805, 0.728133987, 0.728133987, 0.540567805, 0.540567805, 0.540567805, 0.540567805, 0.540567805, 0.540567805, 0.540567805, 0.540567805, 0.540567805, 0.540567805, 0.728133987, 0.540567805, 0.728133987, 0.540567805, 0.540567805, 0.680678526, 0.540567805, 0.540567805, 0.540567805, 0.540567805, 0.540567805, 0.540567805, 0.654155675, 0.728133987, 0.540567805, 0.540567805, 0.540567805, 0.540567805, 0.540567805, 0.680678526, 0.680678526, 0.540567805, 0.540567805, 0.680678526])
        assert_array_almost_equal(e_wbsd, priors["logWb_sd"][self.N_IND])
        e_dbsd = np.array([0.690395904, 0.719504763, 0.690395904, 0.719504763, 0.719504763, 0.690395904, 0.690395904, 0.664391944, 0.690395904, 0.719504763, 0.719504763, 0.690395904, 0.690395904, 0.690395904, 0.719504763, 1.132642792, 0.764674068, 0.719504763, 0.690395904, 0.719504763, 0.719504763, 0.719504763, 0.690395904, 0.690395904, 0.719504763, 0.690395904, 0.719504763, 0.719504763, 0.690395904, 0.690395904, 0.690395904, 0.690395904, 0.690395904, 0.690395904, 0.690395904, 0.690395904, 0.690395904, 0.690395904, 0.719504763, 0.690395904, 0.719504763, 0.690395904, 0.690395904, 0.664391944, 0.690395904, 0.690395904, 0.690395904, 0.690395904, 0.690395904, 0.690395904, 0.810898004, 0.719504763, 0.690395904, 0.690395904, 0.690395904, 0.690395904, 0.690395904, 0.664391944, 0.664391944, 0.690395904, 0.690395904, 0.664391944])
        assert_array_almost_equal(e_dbsd, priors["logDb_sd"][self.N_IND])
        e_rsd = np.array([0.824128369, 0.783012838, 0.824128369, 0.783012838, 0.783012838, 0.824128369, 0.824128369, 0.666709963, 0.824128369, 0.783012838, 0.783012838, 0.824128369, 0.824128369, 0.824128369, 0.783012838, 0.418335751, 0.714799269, 0.783012838, 0.824128369, 0.783012838, 0.783012838, 0.783012838, 0.824128369, 0.824128369, 0.783012838, 0.824128369, 0.783012838, 0.783012838, 0.824128369, 0.824128369, 0.824128369, 0.824128369, 0.824128369, 0.824128369, 0.824128369, 0.824128369, 0.824128369, 0.824128369, 0.783012838, 0.824128369, 0.783012838, 0.824128369, 0.824128369, 0.666709963, 0.824128369, 0.824128369, 0.824128369, 0.824128369, 0.824128369, 0.824128369, 0.73732522, 0.783012838, 0.824128369, 0.824128369, 0.824128369, 0.824128369, 0.824128369, 0.666709963, 0.666709963, 0.824128369, 0.824128369, 0.666709963])
        assert_array_almost_equal(e_rsd, priors["logr_sd"][self.N_IND])
        self.assertAlmostEqual(1, priors["lowerbound_logWc"][23541])
        self.assertAlmostEqual(8, priors["upperbound_logWc"][23541])
        self.assertAlmostEqual(0.01, priors["lowerbound_logQc"][23541])
        self.assertAlmostEqual(10, priors["upperbound_logQc"][23541])
        self.assertAlmostEqual(4.48182818065996, priors["logWc_hat"][23541], places=6)
        self.assertAlmostEqual(5.76433614956258, priors["logQc_hat"][23541], places=6)
        self.assertAlmostEqual(0.832554611157698, priors["logQ_sd"][23541], places=7)
        self.assertAlmostEqual(4.71249322990639, priors["logWc_sd"][23541], places=6)
        self.assertAlmostEqual(0.832554611157698, priors["logQc_sd"][23541])
        self.assertAlmostEqual(10, priors["Werr_sd"][23541])
        self.assertAlmostEqual(1e-05, priors["Serr_sd"][23541], places=5)
        self.assertAlmostEqual(10, priors["dAerr_sd"][23541])
        e_sman = np.full((62,), 0.25)
        assert_array_almost_equal(e_sman, priors["sigma_man"][self.N_IND])
        e_samhg = np.full((62,), 0.22)
        assert_array_almost_equal(e_samhg, priors["sigma_amhg"][self.N_IND])
        e_oi = np.full(len(self.N_IND), 1, dtype=np.int32)
        assert_array_equal(e_oi, priors["overwritten_indexes"][self.N_IND])
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
        self.assertAlmostEqual(4.508255, priors["logWc_hat"][23541], places=6)
        self.assertAlmostEqual(5.764336, priors["logQc_hat"][23541], places=6)
        self.assertAlmostEqual(0.8325546, priors["logQ_sd"][23541], places=7)
        self.assertAlmostEqual(0.00999975, priors["logWc_sd"][23541], places=6)
        self.assertAlmostEqual(0.8325546, priors["logQc_sd"][23541])
        self.assertAlmostEqual(10, priors["Werr_sd"][23541])
        self.assertAlmostEqual(1e-05, priors["Serr_sd"][23541], places=5)
        self.assertAlmostEqual(10, priors["dAerr_sd"][23541])
        self.assertAlmostEqual(0.25, priors["sigma_man"][23541])
        self.assertAlmostEqual(0.22, priors["sigma_amhg"][23541])
        self.assertEqual(1, priors["overwritten_indexes"][23541])

        # Assert node results
        priors = gen.gb_dict["node"]
        e_rt = np.repeat(1, 62)
        assert_array_equal(e_rt, priors["river_type"][self.N_IND])
        self.assertAlmostEqual(1.47, priors["lowerbound_A0"][23541])
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
        e_A0hat = np.repeat(5.538318, 62)
        assert_array_almost_equal(e_A0hat, priors["logA0_hat"][self.N_IND])
        e_nhat = np.repeat(-3.293182, 62)
        assert_array_almost_equal(e_nhat, priors["logn_hat"][self.N_IND])
        e_bhat = np.repeat(0.1939656, 62)       
        assert_array_almost_equal(e_bhat, priors["b_hat"][self.N_IND])
        
        e_wbhat = np.repeat(3.57473, 62)
        assert_array_almost_equal(e_wbhat, priors["logWb_hat"][self.N_IND])
        e_dbhat = np.repeat(-0.4774545, 62)
        assert_array_almost_equal(e_dbhat, priors["logDb_hat"][self.N_IND])
        e_rhat = np.repeat(0.722592, 62)
        assert_array_almost_equal(e_rhat, priors["logr_hat"][self.N_IND])
        e_a0sd = np.repeat(1.831509, 62)
        assert_array_almost_equal(e_a0sd, priors["logA0_sd"][self.N_IND])
        e_nsd = np.repeat(1.137136, 62)
        assert_array_almost_equal(e_nsd, priors["logn_sd"][self.N_IND])
        e_bsd = np.repeat(0.1236208, 62)
        assert_array_almost_equal(e_bsd, priors["b_sd"][self.N_IND])
        e_wbsd = np.repeat(1.026266, 62)
        assert_array_almost_equal(e_wbsd, priors["logWb_sd"][self.N_IND])
        e_dbsd = np.repeat(0.909913, 62)
        assert_array_almost_equal(e_dbsd, priors["logDb_sd"][self.N_IND])
        e_rsd = np.repeat(0.7968415, 62)
        assert_array_almost_equal(e_rsd, priors["logr_sd"][self.N_IND])
        self.assertAlmostEqual(1, priors["lowerbound_logWc"][23541])
        self.assertAlmostEqual(8, priors["upperbound_logWc"][23541])
        self.assertAlmostEqual(0.01, priors["lowerbound_logQc"][23541])
        self.assertAlmostEqual(10, priors["upperbound_logQc"][23541])
        self.assertAlmostEqual(4.481828, priors["logWc_hat"][23541], places=6)
        self.assertAlmostEqual(5.764336149562581, priors["logQc_hat"][23541], places=6)
        self.assertAlmostEqual(0.8325546, priors["logQ_sd"][23541], places=7)
        self.assertAlmostEqual(0.00999975, priors["logWc_sd"][23541], places=6)
        self.assertAlmostEqual(0.8325546, priors["logQc_sd"][23541])
        self.assertAlmostEqual(10, priors["Werr_sd"][23541])
        self.assertAlmostEqual(1e-05, priors["Serr_sd"][23541], places=5)
        self.assertAlmostEqual(10, priors["dAerr_sd"][23541])
        e_sman = np.full((62,), 0.25)
        assert_array_almost_equal(e_sman, priors["sigma_man"][self.N_IND])
        e_samhg = np.full((62,), 0.22)
        assert_array_almost_equal(e_samhg, priors["sigma_amhg"][self.N_IND])
        e_oi = np.full(len(self.N_IND), 1, dtype=np.int32)
        assert_array_equal(e_oi, priors["overwritten_indexes"][self.N_IND])
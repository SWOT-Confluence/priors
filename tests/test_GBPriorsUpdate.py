# Standard imports
from pathlib import Path
import pickle
from shutil import copyfile
import unittest

# Third-party imports
from netCDF4 import Dataset
import numpy as np
from numpy.testing import assert_array_almost_equal, assert_array_equal

# Standard imports
from priors.gbpriors.GBPriorsUpdate import GBPriorsUpdate

class TestGBPriorsAppend(unittest.TestCase):
    """Tesets methods of GBPriorsAppend class."""

    SWORD_FILE = Path(__file__).parent / "sword" / "na_sword_v11.nc"
    SOS_FILE = Path(__file__).parent / "sos" / "constrained" / "na_sword_v11_SOS.nc"
    APPEND_FILE = Path(__file__).parent / "gbpriors" / "na_sword_v11_SOS.nc"
    N_IND = [990518, 990519, 990520, 990521, 990522, 990523, 990524, 990525, 990526, 990527, 990528, 990529, 990530, 990531, 990532, 990533, 990534, 990535, 990536, 990537, 990538, 990539, 990540, 990541, 990542, 990543, 990544, 990545, 990546, 990547, 990548, 990549, 990550, 990551, 990552, 990553, 990554, 990555, 990556, 990557, 990558, 990559, 990560, 990561, 990562, 990563, 990564, 990565, 990566, 990567, 990568, 990569, 990570, 990571, 990572, 990573, 990574, 990575, 990576, 990577, 990578, 990579]

    def test_update_data(self):
        """Tests update_data method."""

        # Get data to update
        copyfile(self.SOS_FILE, self.APPEND_FILE)
        with open(Path(__file__).parent / "gbpriors" / "gb_data", "rb") as pf:
            gb_dict = pickle.load(pf)
            
        # Append priors
        app = GBPriorsUpdate(gb_dict, self.APPEND_FILE)
        app.update_data()

        # Assert results
        sos_ds = Dataset(self.APPEND_FILE) 

        # Reach-level priors     
        priors = sos_ds["gbpriors"]["reach"]
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
        self.assertAlmostEqual(4.204693, priors["logWc_hat"][23541], places=6)
        self.assertAlmostEqual(5.763383, priors["logQc_hat"][23541], places=6)
        self.assertAlmostEqual(0.8325546, priors["logQ_sd"][23541], places=7)
        self.assertAlmostEqual(0.00999975, priors["logWc_sd"][23541], places=6)
        self.assertAlmostEqual(0.8325546, priors["logQc_sd"][23541])
        self.assertAlmostEqual(10, priors["Werr_sd"][23541])
        self.assertAlmostEqual(1e-05, priors["Serr_sd"][23541], places=5)
        self.assertAlmostEqual(10, priors["dAerr_sd"][23541])
        self.assertAlmostEqual(0.25, priors["sigma_man"][23541])
        self.assertAlmostEqual(0.22, priors["sigma_amhg"][23541])

        # Node level priors
        priors = sos_ds["gbpriors"]["node"]
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
        self.assertAlmostEqual(4.186711, priors["logWc_hat"][23541], places=6)
        self.assertAlmostEqual(5.763383, priors["logQc_hat"][23541], places=6)
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

        sos_ds.close()
        self.APPEND_FILE.unlink()
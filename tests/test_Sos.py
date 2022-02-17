# Standard imports
from pathlib import Path
from shutil import copyfile, rmtree
import unittest

# Third-party imports
from netCDF4 import Dataset, chartostring
import numpy as np
from numpy.testing import assert_array_almost_equal

# Local imports
from priors.sos.Sos import Sos

class test_SoS(unittest.TestCase):
    """Test SoS operations."""

    SOS_FILE1 = Path(__file__).parent / "sos" / "as_sword_v11_SOS_priors.nc"
    GRDC_FILE = Path(__file__).parent / "sos" / "working1" / "as_sword_v11_SOS_priors.nc"
    SOS_FILE2 = Path(__file__).parent / "sos" / "na_sword_v11_SOS_priors.nc"
    USGS_FILE = Path(__file__).parent / "sos" / "working2" / "na_sword_v11_SOS_priors.nc"

    def test_overwite_grades_usgs(self):
        """Test overwrite_grades method (USGS data)."""

        if not self.USGS_FILE.parent.exists(): self.USGS_FILE.parent.mkdir(parents=True, exist_ok=True)
        copyfile(self.SOS_FILE2, self.USGS_FILE)

        sos = Sos("na", "constrained", self.SOS_FILE2.parent)
        sos.sos_file = self.USGS_FILE
        sos.overwrite_grades()

        sos_ds = Dataset(self.USGS_FILE, 'r')        
        self.assertEqual(sos_ds.dimensions["num_reaches"].size, sos_ds["model"]["overwritten_indexes"][:].shape[0])
        self.assertEqual(sos_ds.dimensions["num_reaches"].size, sos_ds["model"]["bad_priors"][:].shape[0])
        expected_fdq = np.array([917.8038330078125, 436.0790100097656, 314.3160095214844, 247.20599365234375, 203.31500244140625, 171.60000610351562, 146.11500549316406, 124.59400177001953, 106.73135803222642, 91.18000030517578, 78.72100067138672, 66.8280029296875, 56.63399887084961, 46.439998626708984, 37.6609992980957, 29.732999801635742, 22.709999084472656, 17.555999755859375, 13.309000015258789, 8.947999954223633])
        assert_array_almost_equal(expected_fdq, sos_ds["model"]["flow_duration_q"][10870])
        self.assertAlmostEqual(5691.6767578125, sos_ds["model"]["max_q"][10870])
        expected_monthly = np.array([166.64034050456326, 176.7385667866015, 301.30343995215446, 272.3136427527684, 195.45283308309587, 110.98473909119238, 61.10005714401258, 41.869364272721185, 49.31205348764549, 64.6798860843678, 112.059551872546, 148.9684514206476])
        assert_array_almost_equal(expected_monthly, sos_ds["model"]["monthly_q"][10870])
        self.assertAlmostEqual(141.56002338807968, sos_ds["model"]["mean_q"][10870])
        self.assertAlmostEqual(2.265000104904175, sos_ds["model"]["min_q"][10870])
        self.assertAlmostEqual(1718.8299560546875, sos_ds["model"]["two_year_return_q"][10870])
        self.assertEqual(1, sos_ds["model"]["overwritten_indexes"][10870])
        self.assertEqual("grdc", chartostring(np.ma.getdata(sos_ds["model"]["overwritten_source"][10870])))
        self.assertEqual(1, sos_ds["model"]["bad_priors"][15831])
        self.assertEqual("usgs", chartostring(np.ma.getdata(sos_ds["model"]["bad_prior_source"][15831])))

        sos_ds.close()
        rmtree(self.USGS_FILE.parent)
        
    def test_overwite_grades_grdc(self):
        """Test overwrite_grades method (GRDC data)."""

        if not self.GRDC_FILE.parent.exists(): self.GRDC_FILE.parent.mkdir(parents=True, exist_ok=True)
        copyfile(self.SOS_FILE1, self.GRDC_FILE)

        sos = Sos("as", "constrained", self.SOS_FILE1.parent)
        sos.sos_file = self.GRDC_FILE
        sos.overwrite_grades()

        sos_ds = Dataset(self.GRDC_FILE, 'r')        
        self.assertEqual(sos_ds.dimensions["num_reaches"].size, sos_ds["model"]["overwritten_indexes"][:].shape[0])
        self.assertEqual(sos_ds.dimensions["num_reaches"].size, sos_ds["model"]["bad_priors"][:].shape[0])
        expected_fdq = np.array([16686.0, 13622.0, 12153.0, 10697.599999999999, 9500.0, 8110.0, 6733.6, 5445.4, 4215.6, 3333.4, 2633.4, 2267.600000000001, 1983.0, 1738.3999999999996, 1603.4, 1360.0, 1220.0, 1082.2, 960.0, 821.0])
        assert_array_almost_equal(expected_fdq, sos_ds["model"]["flow_duration_q"][74436])
        self.assertAlmostEqual(21433.0, sos_ds["model"]["max_q"][74436])
        expected_monthly = np.array([1128.7806451612903, 1079.6037735849056, 1515.3032258064516, 2206.648888888889, 3263.458064516129, 7822.517777777778, 12819.309677419355, 11074.606451612903, 8849.25111111111, 6103.058064516129, 2740.311111111111, 1582.0623655913978])
        assert_array_almost_equal(expected_monthly, sos_ds["model"]["monthly_q"][74436])
        self.assertAlmostEqual(5040.598466873517, sos_ds["model"]["mean_q"][74436])
        self.assertAlmostEqual(445.0, sos_ds["model"]["min_q"][74436])
        self.assertAlmostEqual(16750.0, sos_ds["model"]["two_year_return_q"][74436])
        self.assertEqual(1, sos_ds["model"]["overwritten_indexes"][74436])
        self.assertEqual("grdc", chartostring(np.ma.getdata(sos_ds["model"]["overwritten_source"][74436])))
        self.assertEqual(1, sos_ds["model"]["bad_priors"][68346])
        self.assertEqual("grdc", chartostring(np.ma.getdata(sos_ds["model"]["bad_prior_source"][68346])))

        sos_ds.close()
        rmtree(self.GRDC_FILE.parent)
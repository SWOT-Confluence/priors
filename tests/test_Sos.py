# Standard imports
from pathlib import Path
from shutil import copyfile, rmtree
import unittest

# Third-party imports
from netCDF4 import Dataset
import numpy as np
from numpy.testing import assert_array_almost_equal

# Local imports
from priors.sos.Sos import Sos

class test_SoS(unittest.TestCase):
    """Test SoS operations."""

    SOS_FILE = Path(__file__).parent / "sos" / "na_sword_v11_SOS_priors.nc"
    WORKING_FILE = Path(__file__).parent / "sos" / "working" / "na_sword_v11_SOS_priors.nc"

    def test_overwite_grades(self):
        """Test overwrite_grades method."""

        if not self.WORKING_FILE.parent.exists(): self.WORKING_FILE.parent.mkdir(parents=True, exist_ok=True)
        copyfile(self.SOS_FILE, self.WORKING_FILE)

        sos = Sos("na", "constrained", self.SOS_FILE.parent)
        sos.sos_file = self.WORKING_FILE
        sos.overwrite_grades()

        sos_ds = Dataset(self.WORKING_FILE, 'r')
        rids_sum = sos_ds["model"]["grdc"].dimensions["num_grdc_reaches"].size + sos_ds["model"]["usgs"].dimensions["num_usgs_reaches"].size
        self.assertEqual(rids_sum, sos_ds["model"].dimensions["num_overwritten"].size)
        
        # GRDC
        expected_fdq = np.array([487.04895999999997, 393.60352, 356.79168, 328.47488, 300.15808, 267.59376, 227.667072, 180.43464959999991, 140.451328, 109.33116480000005, 85.516736, 69.37616, 59.182112, 51.536576, 45.30688, 39.077184, 33.98016, 30.582144, 27.27898928, 23.746468480000008])
        assert_array_almost_equal(expected_fdq, sos_ds["model"]["flow_duration_q"][25902])
        self.assertAlmostEqual(809.8604799999999, sos_ds["model"]["max_q"][25902])
        expected_monthly = np.array([49.04572522580644, 40.21407428436578, 30.72144438709677, 30.467696933333336, 77.20958897587131, 236.66048768000002, 358.4176952124352, 349.62698153290324, 291.27491106133334, 195.7304227029831, 107.47680768888888, 66.08166778494623])
        assert_array_almost_equal(expected_monthly, sos_ds["model"]["monthly_q"][25902])
        self.assertAlmostEqual(155.62195988835538, sos_ds["model"]["mean_q"][25902])
        self.assertAlmostEqual(17.6696832, sos_ds["model"]["min_q"][25902])
        self.assertAlmostEqual(472.89056, sos_ds["model"]["two_year_return_q"][25902])

        # USGS
        expected_fdq = np.array([917.8038330078125, 436.0790100097656, 314.3160095214844, 247.20599365234375, 203.31500244140625, 171.60000610351562, 146.11500549316406, 124.59400177001953, 106.73135803222642, 91.18000030517578, 78.72100067138672, 66.8280029296875, 56.63399887084961, 46.439998626708984, 37.6609992980957, 29.732999801635742, 22.709999084472656, 17.555999755859375, 13.309000015258789, 8.947999954223633])
        assert_array_almost_equal(expected_fdq, sos_ds["model"]["flow_duration_q"][10870])
        self.assertAlmostEqual(5691.6767578125, sos_ds["model"]["max_q"][10870])
        expected_monthly = np.array([166.64034050456326, 176.7385667866015, 301.30343995215446, 272.3136427527684, 195.45283308309587, 110.98473909119238, 61.10005714401258, 41.869364272721185, 49.31205348764549, 64.6798860843678, 112.059551872546, 148.9684514206476])
        assert_array_almost_equal(expected_monthly, sos_ds["model"]["monthly_q"][10870])
        self.assertAlmostEqual(141.56002338807968, sos_ds["model"]["mean_q"][10870])
        self.assertAlmostEqual(2.265000104904175, sos_ds["model"]["min_q"][10870])
        self.assertAlmostEqual(1718.8299560546875, sos_ds["model"]["two_year_return_q"][10870])

        sos_ds.close()

        rmtree(self.WORKING_FILE.parent)

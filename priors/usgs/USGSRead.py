# Third-party imports
from netCDF4 import Dataset, chartostring
import numpy as np
import pandas as pd

class USGSRead:
    """Class that reads in USGS site data needed to download NWIS records.
    
    Attributes
    ----------
    usgs_targets: Path
        Path to USGS targets file    

    Methods
    -------
    flag()
        Flag USGS data
    read()
        Reads USGS flags and data
    """

    def __init__(self, usgs_targets):
        """
        Parameters
        ----------
        usgs_targets: Path
            Path to USGS targets file
        """

        self.usgs_targets = usgs_targets

    def flag(self, Inflag,Inq):
        """Flag USGS data.
        
        Parameters
        ----------
        Inflag: ?list 
                ?usgsquality flag
        Inq:    ?list 
                ?discharge value
        """
        Inflag[Inq<=0]=np.nan
        In = Inflag.replace(np.nan,'*', regex=True)

        # e   Value has been edited or estimated by USGS personnel and is write protected
        # &     Value was computed from affected unit values
        # E     Value was computed from estimated unit values.
        # A     Approved for publication -- Processing and review completed.
        # P     Provisional data subject to revision.
        # <     The value is known to be less than reported value and is write protected.
        # >     The value is known to be greater than reported value and is write protected.
        # 1     Value is write protected without any remark code to be printed
        # 2     Remark is write protected without any remark code to be printed
        #       No remark (blank)
        M={}
        for i in range(len(In)):
            if 'Ice' in In[i] or '*' in In[i]:
                M[i]=False
            else:
                M[i]=True

        Mask=pd.array(list(M.values()),dtype="boolean")
        return Mask

    def read(self, current_agency_ids =[]):
        """Read USGS data."""
       
        ncf = Dataset(self.usgs_targets)
        # USGS STAID ID

        # stuff commented here worked before
        # dataUSGS = chartostring(ncf["StationID"][:].filled(np.nan))
        dataUSGS = ncf["StationID"][:]
        dataUSGS = [''.join(i) for i in dataUSGS]
        # dataUSGS = [ el.strip(' ') for el in dataUSGS ]

        # SWORD Reach ID
        # reachID = chartostring(ncf["Reach_ID"][:].filled(np.nan))
        reachID = ncf["Reach_ID"][:]
        # calibration flag
        # USGScal=ncf["CAL"][:].filled(np.nan)
        USGScal=ncf["CAL"][:]


        if len(current_agency_ids)!= 0:
            print('second time through')
            mock_dataUSGS = []
            mock_reachID = []
            mock_USGScal = []
            for i in range(len(dataUSGS)):
                if dataUSGS[i] in current_agency_ids:
                    mock_dataUSGS.append(dataUSGS[i])
                    mock_reachID.append(reachID[i])
                    mock_USGScal.append(USGScal[i])
            return mock_dataUSGS, mock_reachID, mock_USGScal


        return dataUSGS, reachID, USGScal


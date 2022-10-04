# Standard imports
from datetime import date

# Third-party imports
import asyncio
import rpy2 as rp2
import numpy as np
import pandas as pd
import rpy2.robjects as ro
from os import walk

#RpyImport necessary packages
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter
r=robjects.r

## Activate R functions 
# Defining the R script and loading the instance in Python
r['source']("./Riggs/allRIGGS.R")
# Loading the function we have defined in R.
downloadQ_b = robjects.globalenv['qdownload_b']
# Loading the function we have defined in R.
downloadQ_a = robjects.globalenv['qdownload_a']
# Loading the function we have defined in R.
downloadQ_c = robjects.globalenv['qDownload_c']
# Loading the function we have defined in R.
downloadQ_j = robjects.globalenv['qDownload_j']
# Loading the function we have defined in R.
downloadQ_u = robjects.globalenv['qdownload_uk']
iserror = robjects.globalenv['is.error']
substrRight = robjects.globalenv['substrRight']

# Local imports
from Riggs.RiggsRead import RiggsRead


class RiggsPull:
    """Class that pulls  Gage data and appends it to the SoS.
    
    Attributes
    ----------
    end_date: str
        Date to end search for
    start_date: str
        Date to start search for 
    riggs_dict: dict 
        Dictionary of riggs data  
    riggs_targets: Path
        Path to USGS targets file

    Methods
    -------
    gather_records(sites)
        Creates and returns a list of dataframes for each riggs record
    get_record(site)
        Get riggs record
    pull() 
        Pulls riggs data and flags(?) and stores in riggs_dict
    """


    def __init__(self, riggs_targets, start_date, end_date):
        """
        Parameters
        ----------
        riggs_targets: Path
            Path to riggs targets file_s
        start_date: str
            Date to start search for
        end_date: str
            Date to end search for
        """
        
        self.riggs_targets = riggs_targets
        self.start_date = start_date
        self.end_date = end_date
        self.riggs_dict = {}

    async def get_record(self,site,agencyR):
        """Get rigs record.
        
        Parameter
        ---------
        site: str
            Site identifier
        """
        #Rcode pull entire record, will need to filter after DL within this function
        if 'Hidroweb' in agencyR:
            FMr=downloadQ_b(site)
            with localconverter(ro.default_converter + pandas2ri.converter):
                FMr = ro.conversion.rpy2py(FMr)
                FMr['ConvertedDate']=pd.to_datetime(FMr.Date,format= "%Y%m%d",errors='coerce')
           
            return FMr[(FMr['ConvertedDate'] >= self.start_date) & (FMr['ConvertedDate'] <=  self.end_date)]
        if 'ABOM' in agencyR:    
            FMr=downloadQ_a(site,self.start_date, self.end_date)
            if 'FMr' in locals():
                with localconverter(ro.default_converter + pandas2ri.converter):
                    FMr = ro.conversion.rpy2py(FMr)
                    FMr['ConvertedDate']=pd.to_datetime(FMr.Date)
                    return FMr[(FMr['Quality Code']>-1)]
            else:
                FMr=[]
                return FMr
        if 'WSC' in agencyR: 
            #note "value" here might be a quality filter
            FMr=downloadQ_c(site)
            if 'FMr' in locals():
                if len(FMr[0]) == 1:
                    print("nd")
                    FMr=[]   
                else:
                        with localconverter(ro.default_converter + pandas2ri.converter):
                            FMr = ro.conversion.rpy2py(FMr)                  
                            FMr['ConvertedDate']=pd.to_datetime(FMr.date)
                  
            else:
                print("nd")
                FMr=[]      
            return FMr
        if 'MLIT' in agencyR:
            FMr=downloadQ_j(site, int(self.start_date[0:4]), int(self.end_date[0:4]))
            with localconverter(ro.default_converter + pandas2ri.converter):
                  FMr = ro.conversion.rpy2py(FMr)
                  FMr['ConvertedDate']=pd.to_datetime(FMr.date)
                  return FMr
        if 'DEFRA' in agencyR:
            FMr=downloadQ_u(site)
            with localconverter(ro.default_converter + pandas2ri.converter):
                 FMr = ro.conversion.rpy2py(FMr)
                 FMr['ConvertedDate']=pd.to_datetime(FMr.Date)
                 return FMr

    async def gather_records(self, sites,agencyR):
        
        
        """Creates and returns a list of dataframes for each riggs record.
        
        Parameters
        ----------
        sites: dict
            Dictionary of riggs data needed to download a record
        """

        records = await asyncio.gather(*(self.get_record(sites[site],agencyR[site]) for site in range(len(sites))))
        return records

    def pull(self):
        """Pulls riggs data and (?) and stores in usgs_dict."""

        #define date range block here
        ALLt=pd.date_range(start=self.start_date,end=self.end_date)
        gage_read = GageReadR(self.riggs_targets)
        datariggs, reachIDR, agencyR, RIGGScal = gage_read.read()
        
        # Download records and gather a list of dataframes
        df_list = asyncio.run(self.gather_records(datariggs,agencyR))

        # generate empty arrays for nc output
        EMPTY=np.nan
        MONQ=np.full((len(datariggs),12),EMPTY)
        Qmean=np.full((len(datariggs)),EMPTY)
        Qmin=np.full((len(datariggs)),EMPTY)
        Qmax=np.full((len(datariggs)),EMPTY)
        FDQS=np.full((len(datariggs),20),EMPTY)
        TwoYr=np.full(len(datariggs),EMPTY)
        Twrite=np.full((len(datariggs),len(ALLt)),EMPTY)
        Qwrite=np.full((len(datariggs),len(ALLt)),EMPTY)

        # Extract data from NWIS dataframe records
        for i in range(len(datariggs)):
            if df_list[i].empty is False and 'Q' in df_list[i] :        
                # create boolean from quality flag       
                #Mask=gage_read.flag(df_list[i]['00060_Mean_cd'],df_list[i]['00060_Mean'])
                # pull in Q
                Q=df_list[i]['Q']
                #Q=Q[Mask]
                if Q.empty is False:
                    print(i)
                    Q=Q.to_numpy()
                    #Q=Q*0.0283168#convertcfs to meters        
                    T=df_list[i]['ConvertedDate']        
                    T=pd.DatetimeIndex(T)
                    #T=T[Mask]
                    moy=T.month
                    yyyy=T.year
                    moy=moy.to_numpy()       
                    thisT=np.zeros(len(T))
                    for j in range((len(T))):
                        thisT=np.where(ALLt==np.datetime64(T[j]))
                        Qwrite[i,thisT]=Q[j]
                        Twrite[i,thisT]=date.toordinal(T[j])
                    # with df pulled in run some stats
                    #basic stats
                    Qmean[i]=np.nanmean(Q)
                    Qmax[i]=np.nanmax(Q)
                    Qmin[i]=np.nanmin(Q)
                    #monthly means
                    Tmonn={}    
                    for j in range(12):
                        Tmonn=np.where(moy==j+1)
                        if not np.isnan(Tmonn).all() and Tmonn: 
                            MONQ[i,j]=np.nanmean(Q[Tmonn])
                            
                    #flow duration curves (n=20)
                        
                    p=np.empty(len(Q))  
                    
                    for j in range(len(Q)):
                        p[j]=100* ((j+1)/(len(Q)+1))           
                    
                    
                    thisQ=np.flip(np.sort(Q))
                    FDq=thisQ
                    FDp=p
                    FDQS[i]=np.interp(list(range(1,99,5)),FDp,FDq)
                    #FDPS=list(range(0,99,5))
                    # Two year recurrence flow
                    
                    Yy=np.unique(yyyy); 
                    Ymax=np.empty(len(Yy))  
                    for j in range(len(Yy)):
                        Ymax[j]=np.nanmax(Q[np.where(yyyy==Yy[j])]);
                
                    MAQ=np.flip(np.sort(Ymax))
                    m = (len(Yy)+1)/2
                    
                    TwoYr[i]=MAQ[int(np.ceil(m))-1]

        Mt=list(range(1,13))
        P=list(range(1,99,5))

        self.riggs_dict = {
            "data": datariggs,
            "reachId": reachIDR,
            "RIGGScal": RIGGScal, 
            "Agency": agencyR,
            "Qwrite": Qwrite,
            "Twrite": Twrite,
            "Qmean": Qmean,
            "Qmax": Qmax,
            "Qmin": Qmin,
            "MONQ": MONQ,
            "Mt": Mt,
            "P": P,
            "FDQS": FDQS,
            "TwoYr": TwoYr
        }
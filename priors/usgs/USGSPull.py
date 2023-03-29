# Standard imports
from datetime import date

# Third-party imports
import asyncio
import dataretrieval.nwis as nwis
import numpy as np
import pandas as pd
from netCDF4 import Dataset, stringtochar
from pathlib import Path
import datetime as datetime



# Local imports
from priors.usgs.USGSRead import USGSRead



def days_convert(days):


    start_date = datetime.date(1, 1, 1)

    new_date = start_date + datetime.timedelta(days=days)

    return new_date.strftime('%Y-%m-%d %H:%M:%S+00:00')


def create_sos_df(sos, date_list, index):
    df = pd.DataFrame(columns = ['datetime', '00060_Mean', '00060_Mean_cd'])
    usgs_q = sos['usgs']['usgs_q']

    df['datetime'] = date_list
    df['00060_Mean'] = usgs_q[index]/0.0283168
    # a for approved for publishing so historic data doesnt get dropped
    df['00060_Mean_cd'] = 'A'
    df = df.set_index('datetime')

    return df


def combine_dfs(sos_df, gauge_df):
    return sos_df.append(gauge_df)


def merge_historic_gauge_data(sos, date_list, gauge_df_list):

    cnt = 0
    for gauge_df in gauge_df_list:
        if gauge_df.empty is False and '00060_Mean' in gauge_df:
            try:
                sos_df = create_sos_df(sos = sos, date_list = date_list, index = cnt)
                merged_df = combine_dfs(sos_df = sos_df, gauge_df = gauge_df)
                gauge_df_list[cnt] = merged_df
                # print('merging', cnt, 'of', len(gauge_df_list))
            
            except:
                print('merge failed')
                # print(gauge_df)
                try:
                    # print(sos_df)
                    print('sos df')
                except:
                    print('no sos df')
        cnt +=1

    return gauge_df_list

class USGSPull:
    """Class that pulls USGS Gage data and appends it to the SoS.
    
    Attributes
    ----------
    end_date: str
        Date to end search for
    start_date: str
        Date to start search for 
    usgs_dict: dict 
        Dictionary of USGS data  
    usgs_targets: Path
        Path to USGS targets file

    Methods
    -------
    gather_records(sites)
        Creates and returns a list of dataframes for each NWIS record
    get_record(site)
        Get NWIS record
    pull() 
        Pulls USGS data and flags and stores in usgs_dict
    """

    def __init__(self, usgs_targets, start_date, end_date, sos_file):
        """
        Parameters
        ----------
        usgs_targets: Path
            Path to USGS targets file
        start_date: str
            Date to start search for
        end_date: str
            Date to end search for
        """
        self.usgs_targets = usgs_targets
        self.start_date = start_date
        self.end_date = end_date
        self.usgs_dict = {}
        self.sos_file = sos_file

    async def get_record(self, site):
        """Get NWIS record.
        
        Parameter
        ---------
        site: str
            Site identifier
        """

        return nwis.get_record(sites=site, service='dv', start= self.start_date, end= self.end_date)

    async def gather_records(self, sites):
        """Creates and returns a list of dataframes for each NWIS record.
        
        Parameters
        ----------
        sites: dict
            Dictionary of USGS data needed to download a record
        """

        records = await asyncio.gather(*(self.get_record(site) for site in sites))
        return records

    def pull(self):
        """Pulls USGS data and flags and stores in usgs_dict."""

        #define date range block here
        ALLt=pd.date_range(start='1980-1-1',end=self.end_date)
        gage_read = USGSRead(self.usgs_targets)

        # this pulls all the gauges in the targets
        dataUSGS, reachID, USGScal = gage_read.read()


        current_parsed_agency_ids = []
        # we want to match those up with the one in the non historical sos
        sos = Dataset(Path(self.sos_file))

        agency_ids_from_sos =  list(sos['usgs']['usgs_id'][:])
        sos.close()

        print('sos ids')
        print(agency_ids_from_sos[0])
        print('the above should match the below, if not parse it')
        print(dataUSGS[0])

        for x in agency_ids_from_sos:
            single_id = []
            for i in x:
                try:
                    single_id.append(i.decode('UTF-8'))
                except:
                    print('didnt parse')
                    print(x)

                    pass

            # print(single_id)
            single_id = ''.join(single_id)
            # print(single_id)
            current_parsed_agency_ids.append(single_id)

        # print('these should be 1413')
        # print('current_parsed_agency_ids')
        # print(current_parsed_agency_ids)
        # print('datausgs')
        # print(dataUSGS)

        # raise ValueError
        cnt = 0
        for i in range(len(dataUSGS)):
                if dataUSGS[i] in current_parsed_agency_ids:
                    cnt += 1
        print('cnt---------------------------shoudl be 1413')
        print(cnt)
        # raise ValueError




        dataUSGS, reachID, USGScal = gage_read.read(current_agency_ids=current_parsed_agency_ids)

        
        # Download records and gather a list of dataframes
        df_list = asyncio.run(self.gather_records(dataUSGS))

        # Bring in previously downloaded gauge data and merge with new data
        # sos = Dataset(self.sos_file, 'a')
        # usgs_qt = sos['usgs']['usgs_qt']
        # date_list = [days_convert(i) if i!=-999999999999.0 else i for i in usgs_qt[0].data]
        # df_list = merge_historic_gauge_data(sos, date_list, df_list) 


        # generate empty arrays for nc output
        EMPTY=np.nan
        MONQ=np.full((len(dataUSGS),12),EMPTY)
        Qmean=np.full((len(dataUSGS)),EMPTY)
        Qmin=np.full((len(dataUSGS)),EMPTY)
        Qmax=np.full((len(dataUSGS)),EMPTY)
        FDQS=np.full((len(dataUSGS),20),EMPTY)
        TwoYr=np.full(len(dataUSGS),EMPTY)
        Twrite=np.full((len(dataUSGS),len(ALLt)),EMPTY)
        Qwrite=np.full((len(dataUSGS),len(ALLt)),EMPTY)

        # Extract data from NWIS dataframe records
        for i in range(len(dataUSGS)):
            if df_list[i].empty is False and '00060_Mean' in df_list[i] :
                
                # create boolean from quality flag       
                Mask=gage_read.flag(df_list[i]['00060_Mean_cd'],df_list[i]['00060_Mean'])
                # pull in Q
                Q=df_list[i]['00060_Mean']
                Q=Q[Mask]
                if Q.empty is False:
                    Q=Q.to_numpy()
                    Q=Q*0.0283168#convertcfs to meters

                    # pull in the dataframe and format datetime
                    # would be more appropriate as a part of a function but moving it anywhere breaks the pulling functinality
                    df_list[i] = df_list[i].reset_index()
                    df_list[i]['datetime'] = pd.to_datetime(df_list[i]['datetime'],errors='coerce', format='%Y-%m-%d %H:%M:%S+00:00')
                    df_list[i] = df_list[i].set_index('datetime')


                    T=df_list[i].index.values
                    T=pd.DatetimeIndex(T)
                    T=T[Mask]
                    moy=T.month
                    yyyy=T.year
                    moy=moy.to_numpy()      
                    thisT=np.zeros(len(T))
                    for a_time in T[:10]:
                        print(a_time)
                    for j in range((len(T))):
                        try:
                            thisT=np.where(ALLt==np.datetime64(T[j]))
                            # print('6', thisT[0])
                            Qwrite[i,thisT]=Q[j]
                            Twrite[i,thisT]=date.toordinal(T[j])
                            # print(T[j], 'could be converted')
                        except:
                            # if it couldn't be converted it was a nan, the Q still gets written with a nan date just as it was in the original dataframe
                            # would be a good idea to check and be sure date is nan in sos if it didn't work
                            # print(T[j], 'couldnt be converted')
                            pass
                        # print('7', Twrite[i,thisT])
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
                        if str(Yy[j]) != 'nan': 
                            Ymax[j]=np.nanmax(Q[np.where(yyyy==Yy[j])])
                
                    MAQ=np.flip(np.sort(Ymax))
                    m = (len(Yy)+1)/2
                    
                    TwoYr[i]=MAQ[int(np.ceil(m))-1]

        Mt=list(range(1,13))
        P=list(range(1,99,5))

        self.usgs_dict = {
            "dataUSGS": dataUSGS,
            "reachId": reachID,
            "USGScal": USGScal,
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
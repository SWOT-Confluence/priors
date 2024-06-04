# Standard imports
from datetime import date

# Third-party imports
import asyncio
import rpy2 as rp2
import numpy as np
from numpy import genfromtxt
import pandas as pd
import rpy2.robjects as ro
from os import walk
from netCDF4 import Dataset, stringtochar
import datetime as datetime
from datetime import datetime as dt
import urllib as UL

#RpyImport necessary packages
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter

r=robjects.r

## Activate R functions 
# Defining the R script and loading the instance in Python

r['source']("/app/priors/Riggs/allRIGGS.R")
downloadQ_b = robjects.globalenv['qdownload_b']
downloadQ_a = robjects.globalenv['qdownload_a']
downloadQ_c = robjects.globalenv['qDownload_c']
downloadQ_j = robjects.globalenv['qDownload_j']
downloadQ_u = robjects.globalenv['qdownload_uk']
downloadQ_ch = robjects.globalenv['qdownload_ch']
downloadQ_f = robjects.globalenv['qdownload_f']
downloadQ_q = robjects.globalenv['qdownload_q']
iserror = robjects.globalenv['is.error']
substrRight = robjects.globalenv['substrRight']

#saf specific
gsd= robjects.globalenv['.get_start_date']
ged= robjects.globalenv['.get_end_date']
gcn=robjects.globalenv['.get_column_name']
ceep=robjects.globalenv['construct_endpoint']
dlsad=robjects.globalenv['download_sa_data']
downloadQ_saf=robjects.globalenv['qdownload_Saf']

# Local imports
from .RiggsRead import RiggsRead






def days_convert(days):


    start_date = datetime.date(1, 1, 1)

    new_date = start_date + datetime.timedelta(days=days)

    return new_date.strftime('%Y-%m-%d %H:%M:%S+00:00')






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


    def __init__(self, riggs_targets, start_date, end_date, cont, sos_file):
        """
        Parameters
        ----------
        riggs_targets: Path
            Path to riggs targets file_s
        start_date: str
            Date to start search for
        end_date: str
            Date to end search for
        cont: str
            String identifier for what continent the module is running on
        """
        
        self.riggs_targets = riggs_targets
        self.start_date = start_date
        self.end_date = end_date
        self.riggs_dict = {}
        self.cont = cont
        self.sos_file = sos_file
        
    def canURLpull(self,site,FMr):
        ID=FMr
        S1= "https://wateroffice.ec.gc.ca/services/real_time_data/csv/inline?stations[]="
        S2="&parameters[]=47&start_date="         
        S3="%2000:00:00&end_date="
        S4="%2023:59:59"    
        NEWt=[]
        NEWq=[]
        NEWst=[]
        # print(ID.columns)
        # print(ID)
        # raise ValueError('id here----------------------------------')
        # if (type(ID) !=list):
        #     if (~np.isnan(np.nanmax(ID['ConvertedDate']))):
        #             ORD=ID['ConvertedDate'].apply(pd.Timestamp.toordinal)               
        #             ID['ConvertedDate']=ORD
        #             STid=site            
        #             sd = ID['ConvertedDate'][-1]               
        #             sd=sd+1
        #             sd=dt.fromordinal(sd)               
        #             sd=sd.strftime("%Y-%m-%d")
        #             now=dt.now()
        #             ed=now.strftime("%Y-%m-%d")
        #             URLst=S1+STid+S2+sd+S3+ed+S4
        #             UL.request.urlretrieve(URLst,STid+".csv")
        #             CSVd= genfromtxt(STid+".csv", delimiter=',', dtype='unicode',skip_header=1)
        #             dates=[]
        #             q=[]
        #             #pull Q and days
        #             if len(CSVd)>0:
        #                 for d in range(0,len(CSVd)):               
                            
        #                     ddd = dt.strptime(CSVd[d][1][0:10], '%Y-%m-%d').date()
        #                     dates.append(ddd.toordinal())
        #                     q.append(CSVd[d][3])
                            
                            
        #                 q=np.array(q)
        #                 Udates=np.unique(dates)
                        
        #                 #make average daily q
        #                 Uq=[]
        #                 for u in  Udates:
        #                     uidx=np.where(dates==u)
        #                     Uq.append(np.mean(q[uidx].astype(np.float32)))
                            
        #                 #generate new daily series with fill for all missing dates
        #                 tsd=[]
        #                 tsq=[]  
        #                 DAYs=list(range(int(np.nanmax(ID['ConvertedDate'])+1),np.max(Udates)))
        #                 k=0
        #                 for d in range(len(DAYs)):
        #                     if DAYs[d] in Udates:
        #                         tsd.append(DAYs[d])
        #                         tsq.append(Uq[k])
        #                         k=k+1
               
                        
                                
        #                 #append new and old
        #                 tNEWt=np.append(ID['ConvertedDate'],np.array(tsd))
        #                 NEWt.append(tNEWt)
        #                 tNEWq=np.append(ID['Q'],np.array(tsq))
        #                 NEWq.append(tNEWq)
                    
        #             else:
        #                 NEWt.append(np.append(ID['Date'],np.nan))
        #                 NEWq.append(np.append(ID['Q'],np.nan))
                
        #     else: 
        #         NEWt.append(np.nan)
        #         NEWq.append(np.nan)
        # else: 
        #     NEWt.append(np.nan)
        #     NEWq.append(np.nan)
        
        # try:
        #     dim = len(NEWt[0])
        # except:
        #     print('newt is not an array, lets assume none of them are')
        #     dim = len(NEWt)

        # #below there are a ton of edge cases when the data comes in
        # NEWst=np.repeat(site,dim)
        # NEWfmr=pd.DataFrame()
        # NEWfmr['STATION_NUMBER']=NEWst
        # try:
        #     NEWfmr['Q']=NEWq[0]
        # except:
        #     NEWfmr['Q']=NEWq
        # try:
        #     int_newt = [int(i) for i in NEWt[0]]
        #     NEWfmr['date']=int_newt
        #     strd=NEWfmr['date'].apply(dt.fromordinal)
        # except:
        #     try:
        #         int_newt = [int(i) for i in NEWt]
        #         NEWfmr['date']=int_newt
        #         strd=NEWfmr['date'].apply(dt.fromordinal)
        #     except:
        #         # int_newt = [int(i) for i in NEWt]
        #         NEWfmr['date']=np.nan
        #         strd = np.nan

        # NEWfmr['ConvertedDate']=strd

        # FMr=NEWfmr
        # print(FMr)
        # print('fmr-------------------------------')
        # return FMr
    
        if ~np.isnan(np.nanmax(ID['ConvertedDate'])):
                    ORD=ID['ConvertedDate'].apply(pd.Timestamp.toordinal)
                    ID['ConvertedDate']=ORD
                    STid=site
                    sd = ID['ConvertedDate'][-1]
                    sd=sd+1
                    sd=dt.fromordinal(sd)
                    sd=sd.strftime("%Y-%m-%d")
                    now=dt.now()
                    ed=now.strftime("%Y-%m-%d")
                    URLst=S1+STid+S2+sd+S3+ed+S4
                    UL.request.urlretrieve(URLst,STid+".csv")
                    CSVd= genfromtxt(STid+".csv", delimiter=',', dtype='unicode',skip_header=1)
                    dates=[]
                    q=[]
                    #pull Q and days
                    if np.size(CSVd)>0:
                        for d in range(0,len(CSVd)):
                            ddd = dt.strptime(CSVd[d][1][0:10], '%Y-%m-%d').date()
                            dates.append(ddd.toordinal())
                            q.append(CSVd[d][3])
                        q=np.array(q)
                        Udates=np.unique(dates)
                        #make average daily q
                        Uq=[]
                        for u in  Udates:
                            uidx=np.where(dates==u)
                            Uq.append(np.mean(q[uidx].astype(np.float32)))
                        #generate new daily series with fill for all missing dates
                        tsd=[]
                        tsq=[]
                        DAYs=list(range(int(np.nanmax(ID['ConvertedDate'])+1),np.max(Udates)))
                        k=0
                        for d in range(len(DAYs)):
                            if DAYs[d] in Udates:
                                tsd.append(DAYs[d])
                                tsq.append(Uq[k])
                                k=k+1
                        #append new and old
                        tNEWt=np.append(ID['ConvertedDate'],np.array(tsd))
                        NEWt.append(tNEWt)
                        tNEWq=np.append(ID['Q'],np.array(tsq))
                        NEWq.append(tNEWq)
                        NEWst=np.repeat(STid,len(NEWt[0]))
                        NEWfmr=pd.DataFrame()
                        NEWfmr['STATION_NUMBER']=NEWst
                        NEWfmr['Q']=NEWq[0]
                        NEWfmr['date']=NEWt[0].astype(int)
                        strd=NEWfmr['date'].apply(dt.fromordinal)
                        NEWfmr['date']=strd
                        NEWfmr['ConvertedDate']=NEWfmr['date']
                        FMr=NEWfmr
                        # print('Found data through URL pull')
                        # print(FMr)
                        # print('top date:', NEWfmr['ConvertedDate'].max())
                        return FMr
                    else:
                        print('could not find new data from url pull')
                        return FMr
    

    async def get_record(self,site,agencyR):
        """Get rigs record.
        
        Parameter
        ---------
        site: str
            Site identifier
        """
        #Rcode pull entire record, will need to filter after DL within this function

        if 'DWA' in agencyR:
            print("Pulling SAfrican gages")
            try:
                FMr= downloadQ_saf(site,'discharge',self.start_date, self.end_date)
                try:
                    with localconverter(ro.default_converter + pandas2ri.converter):
                        FMr = ro.conversion.rpy2py(FMr)
                        FMr['ConvertedDate']=pd.to_datetime(FMr.date)
                        print('successfull pull', FMr)

                        return FMr[FMr['Q'] >0]
            
                except Exception as e:           
                    print('fail to pull')
                    print(e)
                    FMr=[]
                    return FMr
            except Exception as e:
                print('fail to pull')
                print(e)
                FMr=[]
                return FMr
        if 'MEFCCWP' in agencyR:
            print("Pulling Quebeck Gages")
            print(site)
            #note "value" here might be a quality filter
            FMr= downloadQ_q(site)
            if 'FMr' in locals():
                if np.size(FMr[0]) == 1:
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
        
        if 'EAU' in agencyR:
            # print("Pulling French gages")
            try:
                FMr=downloadQ_f(site)
                try:
                    with localconverter(ro.default_converter + pandas2ri.converter):
                        # print('pulling gauge')
                        FMr = ro.conversion.rpy2py(FMr)
                        FMr = FMr.rename(columns={"Date":'ConvertedDate'})                      
                        FMr =  FMr[(FMr['ConvertedDate'] >= self.start_date) & (FMr['ConvertedDate'] <=  self.end_date)]
                        # print('successuflly pulled gauge')
                        return FMr
            
                except AttributeError:
                    FMr=[]
                    return FMr
            except:
                FMr=[]
                return FMr
        if 'Hidroweb' in agencyR:
            #brazil
            # sometimes the gauge pull fails, we will try it three times
            for i in range(3):
                try:
                    FMr=downloadQ_b(site)
                    break
                except:
                    FMr = []
                    return FMr

            with localconverter(ro.default_converter + pandas2ri.converter):
                FMr = ro.conversion.rpy2py(FMr)
                try:
                    # 
                    FMr['ConvertedDate']=pd.to_datetime(FMr.Date,format= "%Y%m%d",errors='coerce')
                except:
                    FMr = []
                    return FMr

           
            return FMr[(FMr['ConvertedDate'] >= self.start_date) & (FMr['ConvertedDate'] <=  self.end_date)]


        if 'ABOM' in agencyR:    
            FMr=downloadQ_a(site,self.start_date, self.end_date)
            if 'FMr' in locals():
                with localconverter(ro.default_converter + pandas2ri.converter):
                    FMr = ro.conversion.rpy2py(FMr)
                    # sometimes the above returens a <class 'rpy2.rinterface_lib.sexp.NALogicalType'> and needs to be handled the same as no data
                    try:
                        FMr['ConvertedDate']=pd.to_datetime(FMr.Date)
                    except:
                        FMr = []
                        return []
                    # print('returned a successful gage', FMr[(FMr['Quality Code']>-1)])
                    return FMr[(FMr['Quality Code']>-1)]
            else:
                FMr=[]
                return FMr
        if 'WSC' in agencyR: 
            #note "value" here might be a quality filter
            FMr=downloadQ_c(site)
            if 'FMr' in locals():
                if np.size(FMr[0]) == 1:
                    print("nd")
                    FMr=[]   
                else:
                        with localconverter(ro.default_converter + pandas2ri.converter):
                            FMr = ro.conversion.rpy2py(FMr)                  
                            FMr['ConvertedDate']=pd.to_datetime(FMr.date)
                            FMr=self.canURLpull(site,FMr)
                  
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
            # print('pulling uk gauges')
            FMr=downloadQ_u(site)
            with localconverter(ro.default_converter + pandas2ri.converter):
                 FMr = ro.conversion.rpy2py(FMr)
                 FMr['ConvertedDate']=pd.to_datetime(FMr.Date)
                 return FMr

        if 'DGA' in agencyR:
            FMr=downloadQ_ch(site)
            with localconverter(ro.default_converter + pandas2ri.converter):
                FMr = ro.conversion.rpy2py(FMr)
                print(FMr)
                try:
                    FMr['ConvertedDate']=pd.to_datetime(FMr.Date)
                    return FMr

                except:
                    FMr = []
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
        ALLt=pd.date_range(start='1980-1-1',end=self.end_date)

        # get all reachIDR's we want to pull from non historic list
        sos = Dataset(self.sos_file, 'a')

        # this method of pulling in the gauge targets where we read the targets twice
        # lets us pull gauges that are not in the historic_q group
        gage_read = RiggsRead(Riggs_targets = self.riggs_targets, cont = self.cont)
        # read in all gauges to create agencyR list
        datariggs, reachIDR, agencyR, RIGGScal = gage_read.read()
        # current_group_agency_reach_ids = []
        print('going through these agencies')
        print(list(set(list(agencyR))))
        current_parsed_agency_ids = []
        for agency in list(set(list(agencyR))):

            agency_ids_from_sos =  list(sos[agency][f'{agency}_id'][:])
            # print(agency_ids_from_sos)

            for x in agency_ids_from_sos:
                single_id = []
                for i in x:
                    if agency in ['DEFRA','EAU'] :
                        try:
                            single_id.append(i.decode('UTF-8'))
                        except:
                            single_id.append('-')
                    else:
                        single_id.append(i)
                # print(single_id)
                try:
                    single_id = ''.join(single_id)
                except TypeError:
                    single_id = [i.decode('utf-8') for i in single_id]
                    single_id = ''.join(single_id)
                # print(single_id)
                current_parsed_agency_ids.append(single_id)



            # current_group_agency_reach_ids = current_group_agency_reach_ids + current_parsed_agency_ids




        # convert the above to match the riggs
        # print(current_group_agency_reach_ids)
        current = []

        
        # for each reach id


    # working for uk
        # for x in current_group_agency_reach_ids:
    
        #     # for each letter
        #     test = []
        #     for i in x.data:  
        #         if i != b'':
        #             test.append(i.decode('UTF-8'))
        #             # single_id = ''.join(test)
        #         else:
        #             test.append(i)
        #     print(test)
        #     site_id = ''.join(test[:-11])
        #     print(site_id)
        #     print('this should look right')
        #     current.append(site_id)


        #     if 'uk' in test[0]:
        #             split_test = test.split('/')
        #             one = split_test[-1][:8]
        #             two = split_test[-1][8:12]
        #             three = split_test[-1][12:16]
        #             four = split_test[-1][16:20]
        #             five = split_test[-1][20:]

        #             parsed_id =  '-'.join([one,two,three,four,five])
        #             url = '/'.join(split_test[:-1])
        #             test = '/'.join([url, parsed_id])


        #     print(test)
        #     single_id = ''.join(test)
        #     print(single_id)
        #     current.append(single_id)


        #     print(test)
        #     if 'uk' in test[0]:
        #         split_test = test.split('/')
        #         one = split_test[-1][:8]
        #         two = split_test[-1][8:12]
        #         three = split_test[-1][12:16]
        #         four = split_test[-1][16:20]
        #         five = split_test[-1][20:]

        #         parsed_id =  '-'.join([one,two,three,four,five])
        #         url = '/'.join(split_test[:-1])
        #         test = '/'.join([url, parsed_id])
        #     current.append(test)

        datariggs, reachIDR, agencyR, RIGGScal = gage_read.read(current_group_agency_reach_ids = current_parsed_agency_ids)
        # print(' reaches here should be good, this is after second pull, first was good')
        # print(len(reachIDR), reachIDR)
        df_list = asyncio.run(self.gather_records(datariggs, agencyR))

        # made it ot here dec 6
        # need to make merge historic gage data different for each agency, can use arg allready in place.

        # Bring in previously downloaded gauge data and merge with new data
        # Riggs module not downloading delta just pulling all non histoic data
        # usgs only one pulling delta
        if agencyR[0] in  ['usgs']:
            riggs_qt = sos[agencyR[0]][f'{agencyR[0]}_qt']
            date_list = [days_convert(i) if i!=-999999999999.0 else i for i in riggs_qt[0].data]
            df_list = merge_historic_gauge_data(sos, date_list, df_list, agencyR[0])  

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
            # print('example df')
            # print(df_list[i])
            # check that it is a dataframe
            if isinstance(df_list[i], pd.DataFrame):
                # print('found some df')
                # print(df_list[i])  

                if df_list[i].empty is False and 'Q' in df_list[i] and 'ConvertedDate' in df_list[i]:
                    # print('found df')
                    # print(df_list[i])       
                    # create boolean from quality flag       
                    #Mask=gage_read.flag(df_list[i]['00060_Mean_cd'],df_list[i]['00060_Mean'])
                    # pull in Q
                    Q=df_list[i]['Q']
                    #Q=Q[Mask]
                    if Q.empty is False:
                        # print(i)
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
            "reachId": np.array(reachIDR),
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
        print('riggs dict out')
        print(self.riggs_dict['Agency'], self.riggs_dict['Qwrite'])
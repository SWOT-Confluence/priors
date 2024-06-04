# Standard imports
from datetime import date

# Third-party imports
import asyncio
import dataretrieval.nwis as nwis
import numpy as np
import pandas as pd
from netCDF4 import Dataset, stringtochar
from pathlib import Path
from datetime import date
from datetime import datetime, timedelta
import time



# Local imports
from priors.usgs.USGSRead import USGSRead



def days_convert(days):


    start_date = date(1, 1, 1)

    new_date = start_date + timedelta(days=days)

    return new_date.strftime('%Y-%m-%d %H:%M:%S+00:00')


def create_sos_df(sos, date_list, index):
    df = pd.DataFrame(columns = ['datetime', '00060_Mean'])
    usgs_q = sos['USGS']['USGS_q']

    df['datetime'] = date_list
    df['00060_Mean'] = usgs_q[index]/0.0283168
    # a for approved for publishing so historic data doesnt get dropped
    df = df.set_index('datetime')
    df_sorted = df.sort_index()

    return df_sorted


def combine_dfs(sos_df, gauge_df):
    df = pd.concat([sos_df,gauge_df])
    sorted_df = df.sort_index()
    return sorted_df


def merge_historic_gauge_data(sos, df_list, historic_date_range, site_list, sos_site_list):



    historic_q = sos['USGS']['USGS_q'][:]
    # fake_q_len = len(date_list) - len(historic_q)
    # new_q = [888.0 for i in range(fake_q_len)]
    # new_q = [888 for i in range(len(new_date_list))]
    
    cnt = 0
    new_df_list = []
    for gauge_df in df_list:
        old_q = historic_q[np.where(np.asarray(sos_site_list) == site_list[cnt])[0][0]]
        if not any(old_q):
            raise ValueError('Missing old q...', site_list[cnt], sos_site_list[cnt])
        if gauge_df.empty is False and '00060_Mean' in gauge_df and len(gauge_df) != 0:
            new_date_list = gauge_df.index.tolist()
            new_q = gauge_df['00060_Mean'].values
            # old_q = historic_q[np.where(np.asarray(sos_site_list) == site_list[cnt])[0][0]]

        
    # Given lists
        # list_1 = [1, 2, 3, 4, 5]
        # list_2 = ['a', 'b', 'c', 'd', 'e']

        # list_3 = [8, 9, 10]
        # list_4 = ['f', 'g', 'h']

            # Combine lists with filler
            print('new q is', len(list(new_q)))
            print('middle is', (int(min(new_date_list)) - int(historic_date_range[1]))-int(1))
            print('old q is', len(list(old_q)))
            print('new date list max', int(max(new_date_list)))
            
            combined_list = list(old_q) + [np.nan] * (int(min(new_date_list)) - int(historic_date_range[1]) - int(1)) + list(new_q)

            # combined_list = list(historic_q[cnt]) + [int(888)] * (int(min(new_date_list)) - int(historic_date_range[1]) ) + list(new_q)
            # Determine the index range
            index_range = range(int(historic_date_range[0]), int(max(new_date_list)) + 1)
            # index_range = range(int(historic_date_range[0]), int(max(new_date_list)))
            print('combined list is', len(combined_list))
            print('index_range is', len(index_range))
            # Create the DataFrame
            # try:
            df = pd.DataFrame(combined_list, index=index_range, columns=['00060_Mean'])
            new_df_list.append(df)
            cnt += 1
            # except:
            #     index_range = range(int(historic_date_range[0]), int(historic_date_range[1]) + 1)
            #     new_df_list.append(pd.DataFrame(list(old_q), index=index_range, columns=['00060_mean']))
            #     cnt += 1

            # print(df)

            # print('successfull merge...')
        else:
            index_range = range(int(historic_date_range[0]), int(historic_date_range[1]) + 1)
            new_df_list.append(pd.DataFrame(list(old_q), index=index_range, columns=['00060_mean']))
            cnt += 1
            print('using historic q', pd.DataFrame(list(old_q), index=index_range, columns=['00060_mean']))
            print('here is historic q index', index_range)

    return new_df_list





















    # historic_q = sos['USGS_q'][:]
    # fake_q_len = len(date_list) - len(historic_q)
    # new_q = [888 for i in range(fake_q_len)]
    
    # cnt = 0
    # new_df_list = []
    # for gauge_df in gauge_df_list:
    #     if gauge_df.empty is False and '00060_Mean' in gauge_df:
    #         new_q = gauge_df['00060_Mean'].values
    #         # concat_q = historic_q[cnt].extend(new_q)
    #     # else:
    #         # concat_q = historic_q[cnt].extend(fake_q)
    #     # new_df['00060_Mean'] = concat_q
    #     # new_df['time'] = date_list
    #     # new_df = new_df.set_index('time')
    #     # new_df = new_df.sort_index()
    #     # new_df_list.append(new_df)
    #     # cnt += 1
        
    # # Given lists
    #     list_1 = [1, 2, 3, 4, 5]
    #     list_2 = ['a', 'b', 'c', 'd', 'e']

    #     list_3 = [8, 9, 10]
    #     list_4 = ['f', 'g', 'h']

    #     # Combine lists
    #     combined_list = historic_q[cnt] + [888.0] * (min(list_3) - max(list_1) - 1) + list_4

    #     # Determine the index range
    #     index_range = range(min(list_1), max(list_3) + 1)

    #     # Create the DataFrame
    #     df = pd.DataFrame(combined_list, index=index_range, columns=['Values'])

    #     print(df)
    
    # return new_df_list


            
            



    #         try:
    #             sos_df = create_sos_df(sos = sos, date_list = date_list, index = cnt)
    #             merged_df = combine_dfs(sos_df = sos_df, gauge_df = gauge_df)
    #             print('sos prev start', sos_df.head())
    #             print('gauge df start',gauge_df.head())
    #             print('merged_df start', )
    #             gauge_df_list[cnt] = merged_df
    #             # print('merging', cnt, 'of', len(gauge_df_list))
    #             print('merge successfull')
            
    #         except Exception as e:
    #             print('merge failed', e)
    #             # print(gauge_df)
    #             # try:
    #             #     foo = sos_df
    #             #     print('sos df')
    #             # except:
    #             #     print('no sos df')
    #     else:
    #         sos_df = create_sos_df(sos = sos, date_list = date_list, index = cnt)

    #     cnt +=1

    # return gauge_df_list

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
        try:
            df =  nwis.get_record(sites=site, service='iv', start= self.start_date, end= self.end_date)
        except Exception as e:
            print('nwis search failed...', e, site)
            # df = pd.DataFrame()
            df =  nwis.get_record(sites=site, service='dv', start= self.start_date, end= self.end_date)


        # print('took', end - start, 'to pull this gauge')
        # print('here is the df which should have columns')
        # print(df.columns)


        # q_columns = ['00060',
        #     '00060_2',
        #     '00060_from primary sensor',
        #     '00060_discharge from primary sensor'
        #     ]
        
        
        if len(df) ==0:
            df =  nwis.get_record(sites=site, service='dv', start= self.start_date, end= self.end_date)
            # filtered_columns = [s for s in list(df.columns) if s.startswith('00060') and not s.endswith('_cd')]
            # print('searching via filtered columns', df)
            
        filtered_columns = [s for s in list(df.columns) if s.startswith('00060') and not s.endswith('_cd')]
            
        
        

        # pull out q, t, and f
        found_column = False
        for column_name in filtered_columns:
            # if column_name in list(df.columns):
            found_column = True
            Q = df[column_name].values[df[column_name].values>-99]/35.3147
            T = df.index.values[df[column_name].values>-99]
            if column_name + '_cd' in df.columns:
                F=df[column_name + '_cd'].values[df[column_name].values>-99]
            else:
                try:
                    F=df['00065_cd'].values[df[column_name].values>-99]
                except:
                    F = np.nan
                    print('Coulnt not find any flags...')
        
        if not found_column:
            print('could not find columns...', list(df.columns), site)
            return (pd.DataFrame(), site)
        
        
            

            





        # for i in potential_flag_columns:
        #     try:

        
        # try:
        #     Q=df['00060'].values[df['00060'].values>-99]/35.3147
        #     T=df.index.values[df['00060'].values>-99]
        #     try:
        #         F=df['00065_cd'].values[df['00060'].values>-99]
        #     except:
        #         F=df['00060_cd'].values[df['00060'].values>-99]
        #     # print('successful pull...')
        # except Exception as e:
        #     # print(e)
        #     # print(1)
        #     try:
        #         Q=df['00060_2'].values[df['00060_2'].values>-99]/35.3147
        #         T=df.index.values[df['00060_2'].values>-99]
        #         F=df['00065_cd'].values[df['00060_2'].values>-99]
        #         try
        #             F=df['00065_cd'].values[df['00060_from primary sensor'].values>-99]
        #         except:
        #             F=df['00060_2_cd'].values[df['00060_from primary sensor'].values>-99]
        #     except Exception as e:
        #         # print(e)
        #         # print(2)
        #         try:
        #             Q=df['00060_from primary sensor'].values[df['00060_from primary sensor'].values>-99]/35.3147
        #             T=df.index.values[df['00060_from primary sensor'].values>-99]
        #             try:
        #                 F=df['00065_cd'].values[df['00060_from primary sensor'].values>-99]
        #             except:
        #                 F=df['00060_from primary sensor_cd'].values[df['00060_from primary sensor'].values>-99]
        #             print('successful pull...')
        #         except Exception as e:
        #             # print(3)
        #             # print(e)
        #             try: 
        #                 Q=df['00060_discharge from primary sensor'].values[df['00060_discharge from primary sensor'].values>-99]/35.3147
        #                 T=df.index.values[df['00060_discharge from primary sensor'].values>-99]
        #                 try:
        #                     F=df['00065_cd'].values[df['00060_discharge from primary sensor'].values>-99]
        #                 except:
        #                     F=df['00060_discharge from primary sensor_cd'].values[df['00060_discharge from primary sensor'].values>-99]
        #                 print('successful pull...')
        #             except Exception as e:
        #                 # print('setting to nans')
        #                 # print(e)
        #                 T=np.nan
        #                 Q=np.nan
        #                 F=np.nan
        #                 print('pull failed', e)
        
        if type(F) != float:
            if len(F)>1:
                newF=[]
                for flag in F:

                    if flag=='Ice' or str(flag)=='nan':
                        newF.append(False)
                    else:
                        newF.append(True)

                T=T[newF]
                Q=Q[newF]
                newF = 0


        F = 0


        UTCord=[]
        if type(T) != float:
            # print('Here is t', T)
            # print('here if q',Q )
            # types = [type(i) for i in T]
            # print('here are types', list(set(types)))
            T = [str(i) for i in T]
            for tt in T:
                ttt=datetime.strptime(tt[0:10],'%Y-%m-%d')  
                utc=ttt+timedelta(hours=int(T[0][-6:-3]))
                UTCord.append(utc.toordinal())

        uUTCord=np.unique(UTCord)
        OUTt=[]
        OUTq=[]
        for T in uUTCord:
            OUTt.append(T)
            NOWd=np.where(uUTCord==T)[0][0]
            OUTq.append(np.nanmean(Q[NOWd]))

        # dump variables for memor
        df = 0

        dfnew=pd.DataFrame()
        dfnew.index=OUTt
        dfnew['00060_Mean']=OUTq    
        # print('Successfull pull...')    
        return (dfnew, site)
        # return nwis.get_record(sites=site, service='dv', start= self.start_date, end= self.end_date)
    def split(self, list_a, chunk_size):

        for i in range(0, len(list_a), chunk_size):
            yield list_a[i:i + chunk_size]
    
    def old_data_fill(self, empty_array, sos, variable_name):

    # MONQQ = empty_array
    # 'USGS_monthly_q' = variable name

        existing_data = np.array(sos['USGS'][variable_name][:])
        if existing_data.ndim == 1:
            empty_array[:len(existing_data)] = existing_data
        elif existing_data.ndim == 2:
            rows, cols = existing_data.shape
            empty_array[:rows, :cols] = existing_data
        return empty_array




    async def gather_records(self, sites):
        """Creates and returns a list of dataframes for each NWIS record.
        
        Parameters
        ----------
        sites: dict
            Dictionary of USGS data needed to download a record
        """
        cnt = 0
        
        sites_split = self.split(sites, 10)
        records_total = []
        for a_few_sites in sites_split:
            start = time.time()


            records = await asyncio.gather(*(self.get_record(site) for site in a_few_sites))
            records_total.extend(records)

            end = time.time()

            cnt += 1
            
            
        df_total = [i[0] for i in records_total]
        site_list = [i[1] for i in records_total]
        return df_total, site_list

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

        agency_ids_from_sos =  list(sos['USGS']['USGS_id'][:])
        sos.close()

        # print('sos ids')
        # print(agency_ids_from_sos[0])
        # print('the above should match the below, if not parse it')
        # print(dataUSGS[0])

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
        # raise ValueError




        dataUSGS, reachID, USGScal = gage_read.read(current_agency_ids=current_parsed_agency_ids)

        
        # Download records and gather a list of dataframes
        df_list, site_list = asyncio.run(self.gather_records(dataUSGS))
        
        # print('there are this many gagues in the read', len(dataUSGS))
        # print('there are this many dataframes', len(df_list))

        # for df in df_list:
        #     if df.empty is False:
        #         new_date_list = df.index.tolist()
        #         print('datecheck...first new data is', new_date_list[0])
        #         print('datecheck...last new date is', new_date_list[-1])
        #         break

        # Bring in previously downloaded gauge data and merge with new data
        sos = Dataset(self.sos_file, 'a')
        usgs_qt = sos['USGS']['USGS_qt']
        
        
        
        
        
        # Flatten the array to 1D
        flattened_array = usgs_qt[:].flatten()

        # Filter out non-positive values (i.e., values <= 0)
        positive_values = flattened_array[flattened_array > 0]

        # Find the minimum value from the positive values
        if positive_values.size > 0:  # Ensure there are positive values in the array
            historic_min = positive_values.min()
            historic_max = positive_values.max()
            historic_date_range = [historic_min, historic_max]
            
            # print('datecheck historic min date', historic_min)
            # print('datecheck historic max date', historic_max)
        else:
            print("No values greater than 0 found in the array.")
            
        # # date_list = [days_convert(i) if i!=-999999999999.0 else i for i in usgs_qt[0].data]
        # historic_date_list = usgs_qt[0].data
        
        # historic_min = -999999999999.0
        # historic_max = -999999999999.0

        # for i in usgs_qt:
        #     i_array = np.array(i)  # Convert i to a numpy array if it's not already
        #     historic_min_temp = min(i_array)
        #     historic_max_temp = max(i_array)
            
        #     if historic_min_temp not in [-999999999999.0, '-999999999999.0']:
        #         historic_min = historic_min_temp
            
        #     if historic_max_temp not in [-999999999999.0, '-999999999999.0']:
        #         historic_max = historic_max_temp
            
        #     if historic_min not in [-999999999999.0, '-999999999999.0'] and historic_max not in [-999999999999.0, '-999999999999.0']:
        #         print('found historic dates')
                
        #         break
            
            
            
            # if not np.any(i_array == -999999999999.0) and not np.any(i_array == '-999999999999.0'):
            #     print('datecheck...first old data is', historic_date_list[0])
            #     print('datecheck...last old date is', historic_date_list[-1])
            #     break

        

        


        # date_list = historic_date_list.extend(new_date_list)
        # df_list = merge_historic_gauge_data(sos, df_list,  historic_date_range, site_list, dataUSGS) -------------------------
        # print('there are this many after the merge', len(df_list))
        # sos.close()


        # get list of sos dates


        # generate empty arrays for nc output
        # EMPTY=np.nan
        # MONQ=np.full((len(dataUSGS),12), EMPTY)
        # Qmean=np.full((len(dataUSGS)), EMPTY)
        # Qmin=np.full((len(dataUSGS)), EMPTY)
        # Qmax=np.full((len(dataUSGS)), EMPTY)
        # FDQS=np.full((len(dataUSGS),20), EMPTY)
        # TwoYr=np.full((len(dataUSGS)), EMPTY)
        # Twrite=np.full((len(dataUSGS),len(ALLt)), EMPTY)
        # Qwrite=np.full((len(dataUSGS),len(ALLt)), EMPTY)

        # write in old data
        EMPTY=np.nan
        MONQ=np.full((len(dataUSGS),12), EMPTY)
        MONQ = self.old_data_fill(MONQ, sos, 'USGS_monthly_q')

        Qmean=np.full((len(dataUSGS)), EMPTY)
        Qmean = self.old_data_fill(Qmean, sos, 'USGS_mean_q')

        Qmin=np.full((len(dataUSGS)), EMPTY)
        Qmin = self.old_data_fill(Qmin, sos, 'USGS_min_q')

        Qmax=np.full((len(dataUSGS)), EMPTY)
        Qmax = self.old_data_fill(Qmax, sos, 'USGS_max_q')

        FDQS=np.full((len(dataUSGS),20), EMPTY)
        FDQS = self.old_data_fill(FDQS, sos, "USGS_flow_duration_q")

        TwoYr=np.full((len(dataUSGS)), EMPTY)
        TwoYr = self.old_data_fill(TwoYr, sos, "USGS_two_year_return_q")

        Twrite=np.full((len(dataUSGS),len(ALLt)), EMPTY)
        Twrite = self.old_data_fill(Twrite, sos, "USGS_qt")

        Qwrite=np.full((len(dataUSGS),len(ALLt)), EMPTY)
        Qwrite = self.old_data_fill(Qwrite, sos, "USGS_q")


        # Extract data from NWIS dataframe records
        for i in range(len(dataUSGS)):
            if df_list[i].empty is False and '00060_Mean' in df_list[i] :
                # print(df_list[i])
                
                # create boolean from quality flag       
                # Mask=gage_read.flag(df_list[i]['00060_Mean_cd'],df_list[i]['00060_Mean'])
                # pull in Q
                Q=df_list[i]['00060_Mean']
                
                # Q=Q[Mask]
                if Q.empty is False:
                    Q=Q.to_numpy()
                    # Q=Q*0.0283168#convertcfs to meters
                    Q = np.array([float(x) if not isinstance(x, np.ma.core.MaskedConstant) else np.nan for x in Q], dtype=np.float64)
                    # print('here is q after masking', Q)
                    sid_cnt = 0
                    for aq in Q:
                        if aq != np.nan:
                            sid_cnt += 1
                            
                    # print('here is how many non masked q', sid_cnt)
                    # print('here is how many non masked q', len([i if i != np.nan for i in Q]))

                    # pull in the dataframe and format datetime
                    # would be more appropriate as a part of a function but moving it anywhere breaks the pulling functinality
                    # df_list[i] = df_list[i].reset_index()
                    # df_list[i]['datetime'] = pd.to_datetime(df_list[i]['datetime'],errors='coerce', format='%Y-%m-%d %H:%M:%S+00:00')
                    # df_list[i] = df_list[i].set_index('datetime')


                    T=df_list[i].index.values
                    
                    formatted_dates = []
                    for ordinal_str in T:
                        # Convert ordinal string to integer
                        ordinal = int(ordinal_str)
                        # Convert ordinal to datetime
                        dt = datetime.fromordinal(ordinal)
                        # Format datetime to stringf
                        formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S')
                        formatted_dates.append(formatted_date)
                        
                        
                    # T=pd.DatetimeIndex(T)
                    # T=T#[Mask]
                    # T = T.strftime('%Y-%m-%d %H:%M:%S')
                    T = pd.to_datetime(formatted_dates)
                    moy=T.month
                    yyyy=T.year
                    moy=moy.to_numpy()      
                    thisT=np.zeros(len(T))
                    for j in range((len(T))):
                        try:
                            # formatted_date1 = parsed_date1.strftime('%Y-%m-%d %H:%M:%S')
                            # formatted_date2 = parsed_date2.strftime('%Y-%m-%d %H:%M:%S')
                            # thisT=np.where(ALLt==np.datetime64(T[j]))
                            thisT=np.where(ALLt==T[j])
                            if len(thisT) == 0:
                                raise ValueError('Could not find time', T[j], 'in allt', Allt[0])
                            # print('here is where we are looking', T[j],type(T[j]), ALLt[0], type(ALLt[0]))
                            # print('this is the index that we are looking for ', thisT)
                            # print('6', thisT[0])
                            Qwrite[i,thisT]=Q[j]
                            # print('adding this avlue to qwrite', Q[j])
                            # print('this should be the same values', Qwrite[i,thisT])
                            Twrite[i,thisT]=date.toordinal(T[j])
                            # print(T[j], 'could be converted')
                        except Exception as e:
                            print('fail here', e)
                            # if it couldn't be converted it was a nan, the Q still gets written with a nan date just as it was in the original dataframe
                            # would be a good idea to check and be sure date is nan in sos if it didn't work
                            # print(T[j], 'couldnt be converted')
                            pass
                        # print('7', Twrite[i,thisT])
                    # with df pulled in run some stats
                    #basic stats
                    # all_types = list(set([type(i) for i in Q]))

                    Qmean.flags.writeable = True
                    Q.flags.writeable = True
                    # intermed = np.nanmean(Q)
                    # Create a mask for non-NaN values
                    # non_nan_mask = ~np.isnan(Q)

                    # Compute the mean of the non-NaN values
                    # intermed = np.mean(Q[non_nan_mask])
                    Qmean[i]= np.nanmean(Q)
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
        print('here is qwrite', Qwrite)

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
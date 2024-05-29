# Third-party imports
from netCDF4 import Dataset, chartostring
import numpy as np
import pandas as pd
from os import walk
import os

class RiggsRead:
    """Class that reads in Riggs site data needed to download riggs records.
    
    Attributes
    ----------
    Riggs_targets: Path
        Path to riggs targets file    

    Methods
    -------
  
    read()
        Reads Riggs data
    """

    def __init__(self, Riggs_targets, cont):
        """
        Parameters
        ----------
        Riggs_targets: Path
            Path to Riggs targets files
        """

        self.Riggs_targets = Riggs_targets
        self.cont = cont




    def read(self, current_group_agency_reach_ids = []):
        """Read Riggs data."""
                # add more file maps as we test new continents
        target_file_map = {
            'na':['Riggs_canada_.nc', 'Riggs_quebec_.nc'],
            'eu':['Riggs_uk_.nc', 'Riggs_france_.nc'],
            'as':['Riggs_japan_.nc'],
            'oc':['Riggs_australia_.nc'],
            'sa':['Riggs_brazil_.nc'],
            'af':['Riggs_safrica_.nc']
        }

        filenames = target_file_map[self.cont]

        datariggs=[]
        reachIDR=[]
        agencyR=[]
        RIGGScal=[]

        for i in range(len(filenames)):

            # ncf = Dataset('./Rtarget/'+filename)
            ncf = Dataset(self.Riggs_targets.__str__()+"/"+filenames[i])
            # print(ncf["StationID"][:])
            #agency ID      
            # st = ncf["StationID"][:].filled(np.nan) #new python methods no longer return a masked array for station id
            st = ncf["StationID"][:]
            # print(st, 'before')

            

            current = []
            if len(current_group_agency_reach_ids) != 0:
                # print('second reading pass')
                # print(current_group_agency_reach_ids)
                current_group_agency_reach_ids = [''.join(x) for x in current_group_agency_reach_ids]

            st = [''.join(x) for x in st]

            # print('-----------------------------------')
            # print(f'Found {len(current_group_agency_reach_ids)} for agency: {i}')
            # print('-----------------------------------')




            # for x in current_group_agency_reach_ids:
                
            #     test = [''.join(x) for x in st]
            #     print('test print')
            #     print(test)
            #     print('comparison print')
            #     print(test[0][0])
            #     if 'uk' in test[0][0]:
            #         print('parsing uk gauge')

            #         split_test = test.split('/')
            #         one = split_test[-1][:8]
            #         two = split_test[-1][8:12]
            #         three = split_test[-1][12:16]
            #         four = split_test[-1][16:20]
            #         five = split_test[-1][20:]

            #         parsed_id =  '-'.join([one,two,three,four,five])
            #         url = '/'.join(split_test[:-1])
            #         test = '/'.join([url, parsed_id])
            #         current.append(test)
            #     else:
            #         test = [''.join(x) for x in st]

            # st = current






            # This changed with new target files as well, there may be contries that still need this modification
            # if 'uk' in filenames[i]:
            #     if type(st[0]) == np.ndarray:
            #         st=np.rot90(st,k=-1,axes=(1,0))
            #         st=np.flipud(st)
            #         st=st.astype(str)
            #         ST=[]
                    
            #         for ids in range(len(st)):
            #             tst=st[ids]
            #             tst=np.delete(tst,np.where(tst==' '))               
            #             ST.append("".join(tst))
            #         st=ST    
            
            # else:
            #     st = [''.join(x) for x in st]
            
            
                            
            # print(st, 'after')
            #associated Sword Reach
            rid= ncf["Reach_ID"][:].filled(np.nan)
            # print('reach ids')
            # print(rid)
            #calibration flag
            cal= ncf["CAL"][:].filled(np.nan)
            # if len(current_group_agency_reach_ids)!=0:
                # print('second round matchup, defra isnt matching---------------------------')
                # print(st[0], current_group_agency_reach_ids[0])
                # print(st[-1], current_group_agency_reach_ids[-1])
                # print(type(st[-1]), type(current_group_agency_reach_ids[-1]))

            # print(f'here is all of st {len(st)}, should be 269')

            all_cal_strings = [str(int(i)) for i in cal]
            cal_cnt = 0
            for cal_string in all_cal_strings:
                if cal_string in ['0','1']:
                    cal_cnt += 1

            # print(f'there are {cal_cnt} ntr gauges for {filenames[i]}')


            
            m_cnt = 0
            for id in st:
                if id in current_group_agency_reach_ids:
                    m_cnt+=1

            # print(f'there are {len(current_group_agency_reach_ids)} for {filenames[i]}. This is how many should be pulled. however there are only {m_cnt} matches!!')

            id_cnt = 0
            cal_cnt = 0
            for j in range(len(st)):
                if st[j] in current_group_agency_reach_ids:
                    id_cnt += 1
                    if str(int(cal[j])) in ['0','1']:
                        cal_cnt += 1

            # print('id_cnt', id_cnt, 'cal_cnt', cal_cnt)






            for j in range(len(st)):
                #this is set up for multiple agencies to run in the same module
                #I may end up setting up independent modules for all of them meaning this can be simplified.

                # This logic block ensures that the if we are in the second call of read
                # then we will only read the targets of the non historic gauges
                if len(current_group_agency_reach_ids)!=0:

                    if st[j] in current_group_agency_reach_ids:
                    
                        if all_cal_strings[j] in ['0','1']:
                            # print(filenames[i])

                            if 'brazil' in filenames[i]:
                                agencyR.append('Hidroweb')
                                datariggs.append(str(int(st[j])))
                                reachIDR.append(str(int(rid[j])))
                                RIGGScal.append(int(cal[j]))
                            if 'australia' in filenames[i]:
                                agencyR.append('ABOM')
                                datariggs.append(str(st[j]))
                                reachIDR.append(str(int(rid[j])))
                                RIGGScal.append(int(cal[j]))
                            if 'Canada' in filenames[i] or "canada" in filenames[i]:
                                agencyR.append('WSC')
                                datariggs.append(str(st[j]))
                                reachIDR.append(str(int(rid[j])))
                                RIGGScal.append(int(cal[j]))
                            if 'japan' in filenames[i]:
                                #some unserialize gages in the list so need to check for NaN                   
                                agencyR.append('MLIT')
                                datariggs.append(str(st[j]))
                                reachIDR.append(str(int(rid[j])))
                                RIGGScal.append(int(cal[j]))
                            if 'uk' in filenames[i]:
                                # print('found defra')
                                agencyR.append('DEFRA')
                                datariggs.append(str(st[j]))
                                reachIDR.append(str(int(rid[j])))
                                RIGGScal.append(int(cal[j]))
                            if 'chile' in filenames[i]:
                                agencyR.append('DGA')
                                if len(st[j])<8:
                                    st[j]='0'+ st[j]
                                datariggs.append(str(st[j]))
                                reachIDR.append(str(int(rid[j])))
                                RIGGScal.append(int(cal[j]))
                            if 'france' in filenames[i]:
                                agencyR.append('EAU')
                                datariggs.append(str(st[j]))
                                reachIDR.append(str(int(rid[j])))
                                RIGGScal.append(int(cal[j]))
                            if 'quebec' in filenames[i]:
                                agencyR.append('MEFCCWP')
                                datariggs.append(str(st[j]))
                                reachIDR.append(str(int(rid[j])))
                                RIGGScal.append(int(cal[j]))
                            if 'safrica' in filenames[i]:
                                agencyR.append('DWA')
                                datariggs.append(str(st[j]))
                                reachIDR.append(str(int(rid[j])))
                                RIGGScal.append(int(cal[j]))

                else:
                        if 'brazil' in filenames[i]:
                            agencyR.append('Hidroweb')
                            datariggs.append(str(int(st[j])))
                            reachIDR.append(str(int(rid[j])))
                            RIGGScal.append(int(cal[j]))
                        if 'australia' in filenames[i]:
                            agencyR.append('ABOM')
                            datariggs.append(str(st[j]))
                            reachIDR.append(str(int(rid[j])))
                            RIGGScal.append(int(cal[j]))
                        if 'Canada' in filenames[i] or "canada" in filenames[i]:
                            agencyR.append('WSC')
                            datariggs.append(str(st[j]))
                            reachIDR.append(str(int(rid[j])))
                            RIGGScal.append(int(cal[j]))
                        if 'japan' in filenames[i]:
                            #some unserialize gages in the list so need to check for NaN                   
                            agencyR.append('MLIT')
                            datariggs.append(str(st[j]))
                            reachIDR.append(str(int(rid[j])))
                            RIGGScal.append(int(cal[j]))
                        if 'uk' in filenames[i]:
                            agencyR.append('DEFRA')
                            datariggs.append(str(st[j]))
                            reachIDR.append(str(int(rid[j])))
                            RIGGScal.append(int(cal[j]))
                        if 'chile' in filenames[i]:
                            agencyR.append('DGA')
                            if len(st[j])<8:
                                st[j]='0'+ st[j]
                            datariggs.append(str(st[j]))
                            reachIDR.append(str(int(rid[j])))
                            RIGGScal.append(int(cal[j]))
                        if 'france' in filenames[i]:
                                agencyR.append('EAU')
                                datariggs.append(str(st[j]))
                                reachIDR.append(str(int(rid[j])))
                                RIGGScal.append(int(cal[j]))
                        if 'quebec' in filenames[i]:
                                agencyR.append('MEFCCWP')
                                datariggs.append(str(st[j]))
                                reachIDR.append(str(int(rid[j])))
                                RIGGScal.append(int(cal[j]))
                        if 'safrica' in filenames[i]:
                                agencyR.append('DWA')
                                datariggs.append(str(st[j]))
                                reachIDR.append(str(int(rid[j])))
                                RIGGScal.append(int(cal[j]))
        # print('most of the below should not be 23303200391')
        # print(agencyR, reachIDR)

        # print(f'at the end or reading, there are {len(agencyR)} in the dict')
        # print(f'at the end or reading, there are {len(datariggs)} in the dict')
        # print(f'at the end or reading, there are {len(reachIDR)} in the dict')
        # print(f'at the end or reading, there are {len(RIGGScal)} in the dict')

        return datariggs, reachIDR, agencyR, RIGGScal
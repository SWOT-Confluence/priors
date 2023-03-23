# Third-party imports
from netCDF4 import Dataset, chartostring
import numpy as np
import pandas as pd
from os import walk

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
            'na':['Riggs_Canada_P.nc'],
            'eu':['Riggs_uk_P.nc', 'Riggs_france_.nc'],
            'as':['Riggs_japan_.nc'],
            'oc':['Riggs_australia_.nc'],
            'sa':['Riggs_chile_.nc', 'Riggs_brazil_.nc'],
            'af':['no_target']
        }

        filenames = target_file_map[self.cont]

        datariggs=[]
        reachIDR=[]
        agencyR=[]
        RIGGScal=[]
        for i in  range(len(filenames)):
            ncf = Dataset(self.Riggs_targets.__str__()+"/"+filenames[i])
            #ncf = Dataset('./Rtarget/'+filenames[i])
           
            #agency ID      
            st = ncf["StationID"][:].filled(np.nan)
            if type(st[0]) == np.ndarray:
                st=np.rot90(st,k=-1,axes=(1,0))
                st=np.flipud(st)
                st=st.astype(str)
                ST=[]
                
                for ids in range(len(st)):
                    tst=st[ids]
                    tst=np.delete(tst,np.where(tst==' '))               
                    ST.append("".join(tst))
                st=ST                    
                
            #associated Sword Reach
            rid= ncf["Reach_ID"][:].filled(np.nan)
            #calibration flag
            cal= ncf["CAL"][:].filled(np.nan)

            for j in range(len(st)):
                #this is set up for multiple agencies to run in the same module
                #I may end up setting up independent modules for all of them meaning this can be simplified.

                # This logic block ensures that the if we are in the second call of read
                # then we will only read the targets of the non historic gauges
                if len(current_group_agency_reach_ids)!=0:
                    if st[j] in current_group_agency_reach_ids:
                        if str(int(cal[j])) in ['0','1']:
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
                            if 'Canada' in filenames[i]:
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
                        if 'Canada' in filenames[i]:
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

        return datariggs, reachIDR, agencyR, RIGGScal
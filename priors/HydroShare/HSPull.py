# -*- coding: utf-8 -*-
"""
Created on Mon Feb 27 13:15:05 2023

@author: coss.31
"""
#standard imports
import requests
from zipfile import ZipFile
from io import BytesIO
from numpy import genfromtxt
import numpy as np
from os import  scandir, remove
from shutil import rmtree
from datetime import datetime as dt
from datetime import date as dtd
import pandas as pd

#third party imports
from hsclient import HydroShare
#from Clara
#"https:/www.hydroshare.org/hsapi/resource/a0a51f97bd064896b91ac0e23926468e/__;!!KGKeukY!1wHawDYfAK-I7ewHZg4WfibYP8yRayvGclS54hkJz6IaPYI4PvRI4bMTxyDVGZlNGOPo3Mze3xfLzgsHWO8$"
#authenticate
# UN=#needme
# PW=#needme
#RI='96f1928c95834539ba260ab65ea8db8e'
RI='38feeef698ca484b907b7b3eb84ad05b'
URLst='https://www.hydroshare.org/hsapi/resource/' + RI +'/'
SWORDVERSION='16'#need a solution to this being hard coded

DLpath='./resources'
DLpathL='./List'      
class HSp:
    
    def __init__(self):
        self.HydroShare_dict={}
        
    def pull(self):
            UN="SteveCossSWOT"
            PW="9Jn3FJNJs!!KXDj"
            RI='38feeef698ca484b907b7b3eb84ad05b'
            URLst='https://www.hydroshare.org/hsapi/resource/' + RI +'/'
            DLpath='/opt/hydroshare'
            DLpathL='/opt/hydroshare'      

            def remove_files(DLdir):
                """Remove files found in directory.
                
               
                """
            
                with scandir(DLdir) as entries:
                    HS_files = [ entry for entry in entries if entry.name ]
                
                for HS_file in HS_files: 
                    rmtree(DLdir +"/" +HS_file.name)
            """ clear previous pull"""
            remove_files(DLpathL)
            remove_files(DLpath)
            
            
            r=requests.get(URLst)

            print('here are urls', URLst)
            
            
            r.headers.get('Content-Type')
            z = ZipFile(BytesIO(r.content))    
            file = z.extractall( DLpathL)
            csvpath=DLpathL+"/"+RI+"/data/contents/collection_list_"+RI+".csv"
            Collection= genfromtxt(csvpath, delimiter=',', dtype='unicode',skip_header=1, usecols=np.arange(0,5))
            # df = pd.read_csv(csvpath)
            # Collection = df.values.astype('U')
            #log in
            hs = HydroShare(UN,PW)
            #dl all resources
            Sf=[]
            Rid=[]
            Nid=[]
            x=[]
            y=[]
            T=[]
            Q=[]
            Qu=[]
            WSE=[]
            WSEu=[]
            W=[]
            Wu=[]
            CXA=[]
            CXAu=[]
            MxV=[]
            MxVu=[]
            MeanV=[]
            MeanVu=[]
            MxD=[]
            MxDu=[]
            MeanD=[]
            MeanDu=[]
            
            for resource in Collection: #this will bug id there is fewer than 2 resources 
                print('resource')
                print(resource)
                if '_TEST' not in  resource[0][0:4]:#this is modified so the data will get pulled "TEST" is prepended to all the dummy data, this will skip it when fixed 
                    Tstr=resource[2]
                    res = hs.resource(Tstr)
                    
                    NOWres=res.download(DLpath)
                    z = ZipFile(DLpath+'/'+Tstr+'.zip')   
                    
                    file =z.extractall(DLpath)
                    z.close()
                    remove(DLpath+'/'+Tstr+'.zip')
                    csvpath= DLpath+'/'+ Tstr + '/data/contents/'
                    
                    with scandir(csvpath) as entries:
                        RES_files = [ entry for entry in entries if entry.name ]
                    
                    for RES_file in RES_files: 
                        if  'template'not in RES_file.name: 
                             if RES_file.name[-5:-1] != 'ipynb':
                                 if RES_file.name[0:6] != 'readme':
                                    if RES_file.name[-4:]=='.csv':                                
                        
                                        RESlist= genfromtxt(csvpath+"/" +RES_file.name, delimiter=',',dtype ='unicode',skip_header=1)
                                        if np.all(RESlist[:,2]==SWORDVERSION):
                                            c=1
                                            for measurement in RESlist:               
                                                if len(measurement[0])>0:
                                                    try:                                                    
                                                                                                    #print(c)
                                                        c=c+1
                                                        Sf.append(RES_file.name)
                                                        Rid.append(measurement[0].astype(np.int64))
                                                        Nid.append(measurement[1].astype(np.int64))
                                                        x.append(measurement[3].astype(np.float32))
                                                        y.append(measurement[4].astype(np.float32))
                                                        date=measurement[5].strip()
                                                        date=date.strip("'")
                                                        d=dt.strptime( date, '%d-%m-%Y')
                                                        T.append(d.toordinal())
                                                        Q.append(measurement[6].astype(float))
                                                        Qu.append(measurement[7].astype(float))
                                                        WSE.append(measurement[8].astype(float))
                                                        WSEu.append(measurement[9].astype(float))
                                                        W.append(measurement[10].astype(float))
                                                        Wu.append(measurement[11].astype(float))
                                                        CXA.append(measurement[12].astype(float))
                                                        CXAu.append(measurement[13].astype(float))
                                                        MxV.append(measurement[14].astype(float))
                                                        MxVu.append(measurement[15].astype(float))
                                                        MeanV.append(measurement[16].astype(float))
                                                        MeanVu.append(measurement[17].astype(float))
                                                        MxD.append(measurement[18].astype(float))
                                                        MxDu.append(measurement[19].astype(float))
                                                        MeanD.append(measurement[20].astype(float))
                                                        MeanDu.append(measurement[21].astype(float))
                                                    except Exception as e:
                                                        print(e)
                                                        print('missing fill or incorect data fromat in' + RES_file.name )

                                                        
                                        else:
                                            print('wrong SWORD in ' +RES_file.name)
                                            
            
            
            data_id=[]
            data_rid=[]
            data_Nid=[]
            data_x=[]
            data_y=[]
            data_t=[]
            data_q=[]
            data_qu=[]
            data_WSE=[]
            data_WSEu=[]
            data_W=[]
            data_Wu=[]
            data_CXA=[]
            data_CXAu=[]
            data_MxV=[]
            data_MxVu=[]
            data_MeanV=[]
            data_MeanVu=[]
            data_MxD=[]
            data_MxDu=[]
            data_MeanD=[]
            data_MeanDu=[]
            
            print('Concatinate')            
            Ureach=np.unique(Rid)
            for reach in Ureach:
                TR=np.where(Rid==reach)
                #sort all reach data by time
                Tnp=np.array(T)
                Rtid=np.argsort(Tnp[TR],axis=None)
                
                idx=list(TR[0])
                data_id.append(np.array(Sf)[TR][0])
                data_rid.append(reach)
                data_Nid.append(np.array(Nid)[idx])
                data_x.append(np.array(x)[idx][0])
                data_y.append(np.array(y)[idx][0])
                data_t.append(np.array(T)[idx])
                data_q.append(np.array(Q)[idx])
                data_qu.append(np.array(Qu)[idx])
                data_WSE.append(np.array(WSE)[idx])
                data_WSEu.append(np.array(WSEu)[idx])
                data_W.append(np.array(W)[idx])
                data_Wu.append(np.array(Wu)[idx])
                data_CXA.append(np.array(CXA)[idx])
                data_CXAu.append(np.array(CXAu)[idx])
                data_MxV.append(np.array(MxV)[idx])
                data_MxVu.append(np.array(MxVu)[idx])
                data_MeanV.append(np.array(MeanV)[idx])
                data_MeanVu.append(np.array(MeanVu)[idx])
                data_MxD.append(np.array(MxD)[idx])
                data_MxDu.append(np.array(MxDu)[idx])
                data_MeanD.append(np.array(MeanD)[idx])
                data_MeanDu.append(np.array(MeanDu)[idx])
            
                
                
                
                
                
            # generate empty arrays for nc output
            st=dtd.fromordinal(min(T))
            et=dtd.fromordinal(max(T))
            ALLt=pd.date_range(start=st,end=et)
            EMPTY=np.nan
            MONQ=np.full((len(data_rid),12),EMPTY)
            Qmean=np.full((len(data_rid)),EMPTY)
            Qmin=np.full((len(data_rid)),EMPTY)
            Qmax=np.full((len(data_rid)),EMPTY)
            FDQS=np.full((len(data_rid),20),EMPTY)
            TwoYr=np.full(len(data_rid),EMPTY)
            Twrite=np.full((len(data_rid),len(ALLt)),EMPTY)
            Qwrite=np.full((len(data_rid),len(ALLt)),EMPTY)
            Mt=list(range(1,13))
            P=list(range(1,99,5))
            
            # process recrds for dictionary
            for i in range(len(data_rid)):
                        
                # pull in Q
                Q=data_q[i]
               
                if Q.size >0:
                    print(i)       
                    t=data_t[i]
                    T=[]
                    for time in t:
                        
                        T.append(pd.Timestamp.fromordinal(time))             
                   
                    T=pd.DatetimeIndex(T)        
                    moy=T.month
                    yyyy=T.year
                    moy=moy.to_numpy()       
                    thisT=np.zeros(len(T))
                    for j in range((len(T))):
                        thisT=np.where(ALLt==np.datetime64(T[j]))
                        Qwrite[i,thisT]=Q[j]
                        Twrite[i,thisT]=dtd.toordinal(T[j])
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
                            
                    if len(Q)>21:    #do not FDQ on fewer than 21 datum
                        for j in range(len(Q)):
                            p[j]=100* ((j+1)/(len(Q)+1))         
                            thisQ=np.flip(np.sort(Q))
                            FDq=thisQ
                            FDp=p
                            FDQS[i]=np.interp(list(range(1,99,5)),FDp,FDq)
                                    #FDPS=list(range(0,99,5))
                                    # Two year recurrence flow
                                
                    
                    Yy=np.unique(yyyy) 
                    Ymax=np.empty(len(Yy))  
                    for j in range(len(Yy)):
                        Ymax[j]=np.nanmax(Q[np.where(yyyy==Yy[j])]);
                        
                        MAQ=np.flip(np.sort(Ymax))
                        m = (len(Yy)+1)/2
                                
                        TwoYr[i]=MAQ[int(np.ceil(m))-1]
            
                    
                    Mt=list(range(1,13))
                    P=list(range(1,99,5))
            
            self.HydroShare_dict = {
                    "data": data_id,
                    "reachId":  np.array(data_rid),            
                    "Qwrite": Qwrite,
                    "Twrite": Twrite,
                    "Qmean": Qmean,
                    "Qmax": Qmax,
                    "Qmin": Qmin,
                    "MONQ": MONQ,
                    "Mt": Mt,
                    "P": P,
                    "FDQS": FDQS,
                    "TwoYr": TwoYr,
                    "Agency":['HydroSharePull']*len(data_id)
                }
            
           
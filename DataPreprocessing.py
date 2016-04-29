
# coding: utf-8

# In[49]:

import pandas as pd
from pandas import *
from datetime import datetime
import os


# In[61]:

def get_data_frames(path):
    output_path = os.path.join(path, "df_output1")
    if not os.path.exists(output_path):
        os.mkdir(output_path)
    
    for doc in os.listdir(path):                
        if doc.endswith(".csv"):               # loop through every csv file at the target directory
            data=pd.read_csv(os.path.join(path,doc) )
            home_id = doc.split('_')[0]        # get home_id from doc name (the part before '_', 0=number, 1=out.csv)
            for column in data.ix[:,2:]:       # subset from 3rd column to last column
                    df=pd.DataFrame({'Timestamp':data['localminute'],
                                'Value':data[column],
                                'Houseid':home_id})
					# transform 'localminute' to meaningful datetime format
					# e.g. transform 00:10:00-05 to 05:10:00
                    df['Timestamp']=pd.to_datetime(df['Timestamp'])  
                    df.set_index(['Timestamp'])  
                    
					# create 3 new columns shows the corresponding dayofweek (0-6), date, and time
                    df['weekday'] = df['Timestamp'].dt.dayofweek
                    df['Date']=df['Timestamp'].dt.date
                    df['Time']=df['Timestamp'].dt.time 
                    
					# transform the matrix to the format we want
                    dt=df[df['weekday']<5].pivot(index='Date', columns='Time', values='Value')              
                    dtw=df[df['weekday']>4].pivot(index='Date', columns='Time', values='Value')  
                    
                    # use index to get subset (: means from .. to.., :: means every ..)
					# options: '::1', '::5', '::10'
                    dt1 = dt.iloc[:, ::5]
                    dtw1 = dtw.iloc[:, ::5]


                    dt1.to_csv(os.path.join(output_path, home_id+"_"+column+"_weekday_out.csv"))

                    dtw1.to_csv(os.path.join(output_path, home_id+"_"+column+"_weekend_out.csv"))
                    


        


# In[62]:

get_data_frames('/home/minbaev/Downloads/SC/test/')


# In[ ]:




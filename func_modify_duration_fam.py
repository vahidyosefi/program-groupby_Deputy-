# -*- coding: utf-8 -*-
"""
Created on Sun Jan  9 14:37:54 2022

@author: vahid

for modify duration fam to float
this task cause to sum duartion
"""
def func_modify_duration_fam (df2):
    
    
    import pandas as pd
    import numpy as np
    import datetime

    df0=pd.DataFrame()


##### input data
#

######## sepeher **************

  



######aio*************
  

#*********************

#anten*******************

#*************************

#fam***************

       
    
    # df2.to_excel(r'D:\python\EPG_vahid\input\source\Estandard\telewbion_Sv1.xlsx', index=False)
    print('فام')
#***********************

#lenz***************

#***********************
#tva***************


    
####******************************
#iranseda**************************
  
    
#*********************** extract date

    dfsp1 = df2
    
    df1_1 = pd.read_excel(r'D:\back up\EPG\14001018\test\fam\EPG_sarasari_fam_15.xlsx')
    dfsp1 = df1_1
    dfsp1 ['Duration'] = pd.to_datetime(dfsp1.Duration)
    # dfsp1['سال'] = dfsp1['TIME'].dt.year
    # dfsp1['ماه'] = dfsp1['TIME'].dt.month
    # dfsp1['ماه'] = dfsp1['ماه'].apply(lambda x: '{0:0>2}'.format(x))
    # dfsp1['روز'] = dfsp1['TIME'].dt.day
    # dfsp1['روز'] = dfsp1['روز'].apply(lambda x: '{0:0>2}'.format(x))
    dfsp1['ساعت'] = dfsp1['Duration'].dt.hour
    dfsp1['ساعت'] = dfsp1['ساعت'].apply(lambda x: '{0:0>2}'.format(x))
    dfsp1['دقیقه'] = dfsp1['Duration'].dt.minute
    dfsp1['دقیقه'] = dfsp1['دقیقه'].apply(lambda x: '{0:0>2}'.format(x))
    dfsp1['ثانیه'] = dfsp1['Duration'].dt.second
    dfsp1['ثانیه'] = dfsp1['ثانیه'].apply(lambda x: '{0:0>2}'.format(x))
    
    dfsp1['ساعت'] = dfsp1['ساعت'].astype(str).astype(float)
    dfsp1['دقیقه'] = dfsp1['دقیقه'].astype(str).astype(float)
    dfsp1['ثانیه'] = dfsp1['ثانیه'].astype(str).astype(float)
    
    dfsp1['Duration_1']  = dfsp1['ساعت']   + dfsp1['دقیقه'] /60 + dfsp1['ثانیه']/3600
    del dfsp1['ساعت']
    del dfsp1['دقیقه']
    del dfsp1['ثانیه']
    del dfsp1['Duration']
    
    dfsp1['Duration'] = dfsp1['Duration_1']
    del dfsp1['Duration_1']
    df_sarasari_fam_duration = dfsp1
    
    dfsp1.to_excel(r'D:\back up\EPG\14001018\test\fam\EPG_sarasari_fam_15_modify.xlsx', index=False)
    
    print('اصلاح شده چاپ شد')
  
    return df_sarasari_fam_duration

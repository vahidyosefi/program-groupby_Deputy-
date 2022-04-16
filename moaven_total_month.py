# -*- coding: utf-8 -*-
"""
Created on Sun Jan 10 15:09:46 2021

@author: vahid
"""

import pandas as pd
import numpy as np
import datetime
import os
import time
import pyodbc
from pyodbc import *
import psycopg2
import pandas.io.sql as psql
from sqlalchemy import create_engine


start = time.time() 

# df0 = pd.DataFrame()
# total = pd.DataFrame()


######################## read data from dabase
connection = psycopg2.connect(user="postgres",
                            password="12344321",
                            host="10.32.141.17",
                            port="5432",
                            database="Moein01")
cursor = connection.cursor()

input0= psql.read_sql('select * from public."StatisticsMonthOperators"', connection)
print(len(input0))

# VOD_input= pd.read_excel(r'D:\back up\EPG\14001111\VOD\VOD_input.xlsx')
# print('فراخونی محتوای درخواستی')

# input0 = input0.append()

input0["type"] = 'VOD'
input0["channel"] = 'فیلم و سریال'
input0["Operator"] = input0["Operators"]
input0["m_date_new"] = input0["DateTime"]
input0["حوزه"] = input0["Operators"]
input0["Duration_min"] = input0["Duration"]

# Duration_min
# input0["Row"] =  input0["DateTime"]

input0 = input0.rename(columns={'نام برنامه اولیه': 'Name'})
input0 = input0.rename(columns={'نام برنامه': 'modify'}) 
#####change the program name


input0['DateTime'] = input0['DateTime'].astype(str)
input1 = input0
# df_EPG_8 = df_EPG
input0['Row'] = input0['DateTime'].str[:6]

input0 ['Row'] = input0 ['Row'].replace({'201911':'25'})
input0 ['Row'] = input0 ['Row'].replace({'201912':'26'})
input0 ['Row'] = input0 ['Row'].replace({'202001':'27'})
input0 ['Row'] = input0 ['Row'].replace({'202002':'28'})
input0 ['Row'] = input0 ['Row'].replace({'202003':'29'})
input0 ['Row'] = input0 ['Row'].replace({'202004':'30'})
input0 ['Row'] = input0 ['Row'].replace({'202005':'31'})
input0 ['Row'] = input0 ['Row'].replace({'202006':'32'})
input0 ['Row'] = input0 ['Row'].replace({'202007':'33'})
input0 ['Row'] = input0 ['Row'].replace({'202008':'34'})
input0 ['Row'] = input0 ['Row'].replace({'202009':'35'})
input0 ['Row'] = input0 ['Row'].replace({'202010':'36'})
input0 ['Row'] = input0 ['Row'].replace({'202011':'37'})
input0 ['Row'] = input0 ['Row'].replace({'202012':'38'})
input0 ['Row'] = input0 ['Row'].replace({'202101':'39'})
input0 ['Row'] = input0 ['Row'].replace({'202102':'40'})
input0 ['Row'] = input0 ['Row'].replace({'202103':'41'})
input0 ['Row'] = input0 ['Row'].replace({'202104':'42'})
input0 ['Row'] = input0 ['Row'].replace({'202105':'43'})
input0 ['Row'] = input0 ['Row'].replace({'202106':'44'})
input0 ['Row'] = input0 ['Row'].replace({'202107':'45'})
input0 ['Row'] = input0 ['Row'].replace({'202108':'46'})
input0 ['Row'] = input0 ['Row'].replace({'202109':'47'})
input0 ['Row'] = input0 ['Row'].replace({'202110':'48'})
input0 ['Row'] = input0 ['Row'].replace({'202111':'49'})
input0 ['Row'] = input0 ['Row'].replace({'202112':'50'})
input0 ['Row'] = input0 ['Row'].replace({'202201':'51'})
input0 ['Row'] = input0 ['Row'].replace({'202202':'52'})
input0 ['Row'] = input0 ['Row'].replace({'202203':'53'})
input0 ['Row'] = input0 ['Row'].replace({'202204':'54'})
input0 ['Row'] = input0 ['Row'].replace({'202205':'55'})
input0 ['Row'] = input0 ['Row'].replace({'202206':'56'})
input0 ['Row'] = input0 ['Row'].replace({'202207':'57'})
input0 ['Row'] = input0 ['Row'].replace({'202208':'58'})
input0 ['Row'] = input0 ['Row'].replace({'202209':'59'})
input0 ['Row'] = input0 ['Row'].replace({'202210':'60'})
input0 ['Row'] = input0 ['Row'].replace({'202211':'61'})
input0 ['Row'] = input0 ['Row'].replace({'202212':'62'})
input0 ['Row'] = input0 ['Row'].replace({'202301':'63'})
input0 ['Row'] = input0 ['Row'].replace({'202302':'64'})
input0 ['Row'] = input0 ['Row'].replace({'202303':'65'})
input0 ['Row'] = input0 ['Row'].replace({'202304':'66'})
input0 ['Row'] = input0 ['Row'].replace({'202305':'67'})
input0 ['Row'] = input0 ['Row'].replace({'202306':'68'})
input0 ['Row'] = input0 ['Row'].replace({'202307':'69'})
input0 ['Row'] = input0 ['Row'].replace({'202308':'70'})
input0 ['Row'] = input0 ['Row'].replace({'202309':'71'})
input0 ['Row'] = input0 ['Row'].replace({'202310':'72'})
input0 ['Row'] = input0 ['Row'].replace({'202310':'73'})
input0 ['Row'] = input0 ['Row'].replace({'202311':'74'})
input0 ['Row'] = input0 ['Row'].replace({'202312':'75'})

max_value = input0['Row'].max()
print(max_value)

# max_value = 48
input_max_value  = input0.query("Row == @max_value")

input_max_value =  input_max_value[['channel','Row','Operator','Visit','Duration_min','m_date_new','type','حوزه']]







print('اتمام برنامه محتوا درخواستی')



input_max_value.to_excel(r'D:\back up\EPG\14010124\VOD\VOD_3.xlsx', index=False)


############################ write file to postgres

# VOD_input= pd.read_excel(r'D:\back up\EPG\14001111\VOD\VOD_input.xlsx')

# engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
# con=engine.connect()

# VOD_input.to_sql('StatisticsMonthOperators',con,if_exists='append', index=False)


# start = time.time() 

###########################################################         sarasari______month
sarasari_08 = pd.DataFrame()
sarasari_02 = pd.DataFrame()
df_sarasari_with_fam = pd.DataFrame()

sarasari_00= pd.read_excel(r'D:\python\EPG_vahid\progress\merge\match merge\sarasari_140012.xlsx')

# connection = psycopg2.connect(user="postgres",
#                             password="12344321",
#                             host="10.32.141.17",
#                             port="5432",
#                             database="Vahid01")
# cursor = connection.cursor()

# sarasari_00= psql.read_sql('select * from public.sarasari_1', connection)

print('فراخونی سراسری')

engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Vahid01',pool_size=20, max_overflow=100,)
con=engine.connect()

sarasari_00.to_sql('sarasari_1',con,if_exists='append', index=False)

print('Write EPG_sarasari in Postgres SQL')


# date

sarasari_00['تاریخ'] = sarasari_00['تاریخ'].astype(str)
sarasari_01 = sarasari_00
# df_EPG_8 = df_EPG
sarasari_01['m_date'] = sarasari_01['تاریخ'].str[:8]
# df_EPG_8['m_date'] = df_EPG_8['m_date'].astype(str).astype(int)

#rename
sarasari_01 = sarasari_01.rename(columns={'نام شبکه': 'channel', 'نام برنامه': 'program', 'ردیف': 'Row','اپراتور':'Operator','تعداد بازدید': 'Visit','مدت بازدید':'Duration'})
# sarasari_03.to_excel(r'D:\back up\EPG\14001018\test\EPG_sarasari_03.xlsx', index=False)
#group by

sarasari_02 =  sarasari_01[['channel','program','Row','Operator','Visit','Duration','m_date']]

# df_sarasari_fam  = sarasari_02.query("Operator== 'فام'")
# df_sarasari_fam.to_excel(r'D:\back up\EPG\14001018\test\fam\EPG_sarasari_fam.xlsx', index=False)

#### use function for edit duration , this task cause duraion change format to str

# from func_modify_duration_fam import *
# df_sarasari_fam_duration= func_modify_duration_fam(df_sarasari_fam)

    # df0=df0.append(df5)

# df_sarasari_without_fam  = sarasari_02.query("Operator!= 'فام'")
# sarasari_04 = df_sarasari_fam_duration.append(df_sarasari_without_fam)
# sarasari_05 = sarasari_04

## change format to float

# sarasari_02["Duration"] = sarasari_02["Duration"].str.replace('/', '.').astype(float)

# sarasari_02["Duration"] = sarasari_02["Duration"].astype(float)

# sarasari_02.dftype()
sarasari_03 = sarasari_02.copy()

#### group by visit and duration

sarasari_gp = sarasari_02.groupby(['channel','Operator','Row','m_date']).agg({'Visit':'sum','Duration':'sum'}).reset_index()
sarasari_gp['m_date_new'] = sarasari_gp['m_date'].astype(str) + '01'
sarasari_gp["Duration_min"] = sarasari_gp["Duration"] * 60
del sarasari_gp['Duration']

sarasari_gp.loc[sarasari_gp['channel'].str.contains('ورزش'), 'tag'] = 'ورزشی'
sarasari_gp.loc[sarasari_gp['channel'].str.contains('آموزش'), 'tag'] = 'آموزشی'
sarasari_gp.loc[sarasari_gp['channel'].str.contains('مستند'), 'tag'] = 'مستند'

####remove chanel Bronmarzi
# df_sarasari_without_Bronmarzi_1  = sarasari_gp.query("channel!= 'العالم'")
# df_sarasari_without_Bronmarzi_2  = df_sarasari_without_Bronmarzi_1.query("channel!= 'العالم'")
# df_sarasari_without_Bronmarzi_3  = df_sarasari_without_Bronmarzi_2.query("channel!= 'الکوثر'")
# df_sarasari_without_Bronmarzi_4  = df_sarasari_without_Bronmarzi_3.query("channel!= 'آی فیلم'")
# df_sarasari_without_Bronmarzi_5  = df_sarasari_without_Bronmarzi_4.query("channel!= 'جام جم 1'")
# df_sarasari_without_Bronmarzi_6  = df_sarasari_without_Bronmarzi_5.query("channel!= 'پرس تی وی'")


sarasari_gp["type"] = 'سراسری'
sarasari_gp["حوزه"] = sarasari_gp["tag"]


print('اتمام برنامه سزاسزی')



sarasari_gp =  sarasari_gp[['channel','Row','Operator','Visit','Duration_min','m_date_new','type','حوزه']]

sarasari_gp.to_excel(r'D:\back up\EPG\14010124\EPG\sarasari_gp.xlsx', index=False)


# end = time.time()
# # total time taken
# mo = (end - start)/60
# print ('مدت زمان اجرا برنامه به دقیقه برای دسته بندی سراسری',mo)
# print(f"Runtime of the program is {end - start}")
##########################################
######################################################################BronMarzi

EPG_BronMarzi= pd.read_excel(r'D:\python\EPG_vahid\progress\merge\match merge\bronmarzi.xlsx')

# connection = psycopg2.connect(user="postgres",
#                             password="12344321",
#                             host="10.32.141.17",
#                             port="5432",
#                             database="Vahid01")
# cursor = connection.cursor()

# EPG_BronMarzi= psql.read_sql('select * from public."EPG_BronMarzi"', connection)

print(len(EPG_BronMarzi))
print('فراخونی برون مرزی')

engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Vahid01',pool_size=20, max_overflow=100,)
con=engine.connect()

EPG_BronMarzi.to_sql('EPG_BronMarzi',con,if_exists='append', index=False)

print('Write BronMarzi in Postgres SQL')


EPG_BronMarzi['تاریخ'] = EPG_BronMarzi['تاریخ'].astype(str)
EPG_BronMarzi_00 = EPG_BronMarzi
# df_EPG_8 = df_EPG
EPG_BronMarzi['m_date'] = EPG_BronMarzi['تاریخ'].str[:8]

#rename
EPG_BronMarzi = EPG_BronMarzi.rename(columns={'نام شبکه': 'channel', 'نام برنامه': 'program', 'ردیف': 'Row','اپراتور':'Operator','تعداد بازدید': 'Visit','مدت بازدید':'Duration'})

# EPG_radio_01.to_excel(r'D:\back up\EPG\14001018\test\EPG_ekhtesasi_01.xlsx', index=False)
#group by

EPG_BronMarzi = EPG_BronMarzi.groupby(['channel','Operator','Row','m_date']).agg({'Visit':'sum','Duration':'sum'}).reset_index()

EPG_BronMarzi['m_date_new'] = EPG_BronMarzi['m_date'].astype(str) + '01'

EPG_BronMarzi["Duration_min"] = EPG_BronMarzi["Duration"] * 60
del EPG_BronMarzi['Duration']
EPG_BronMarzi["type"] = 'برون مرزی'
EPG_BronMarzi["حوزه"] = ''

EPG_BronMarzi =  EPG_BronMarzi[['channel','Row','Operator','Visit','Duration_min','m_date_new','type','حوزه']]

EPG_BronMarzi.to_excel(r'D:\back up\EPG\14010124\EPG\EPG_BronMarzi.xlsx', index=False)




######################################################################   radio
EPG_radio_08 = pd.DataFrame()

EPG_radio_00= pd.read_excel(r'D:\python\EPG_vahid\progress\merge\match merge\radio.xlsx')
# EPG_radio_00= pd.read_csv(r'D:\python\EPG_vahid\progress\merge\match merge\radio_.xlsx')
# connection = psycopg2.connect(user="postgres",
#                             password="12344321",
#                             host="10.32.141.17",
#                             port="5432",
#                             database="Vahid01")
# cursor = connection.cursor()
# EPG_radio_00= psql.read_sql('Select * from public."EPG_Radio"', connection)


print('فراخونی رادیویی')

engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Vahid01',pool_size=20, max_overflow=100,)
con=engine.connect()

EPG_radio_00.to_sql('EPG_Radio',con,if_exists='append', index=False)

print('Write EPG_radio in Postgres SQL')




# date

EPG_radio_00['تاریخ'] = EPG_radio_00['تاریخ'].astype(str)
EPG_radio_01 = EPG_radio_00
EPG_radio_01['m_date'] = EPG_radio_01['تاریخ'].str[:8]
#rename

EPG_radio_01 = EPG_radio_01.rename(columns={'نام شبکه': 'channel', 'نام برنامه': 'program', 'ردیف': 'Row','اپراتور':'Operator','تعداد بازدید': 'Visit','مدت بازدید':'Duration'})

# EPG_radio_01.to_excel(r'D:\back up\EPG\14001018\test\EPG_ekhtesasi_01.xlsx', index=False)
#group by

EPG_radio_gp = EPG_radio_01.groupby(['channel','Operator','Row','m_date']).agg({'Visit':'sum','Duration':'sum'}).reset_index()

EPG_radio_gp['m_date_new'] = EPG_radio_gp['m_date'].astype(str) + '01'
EPG_radio_gp["Duration_min"] = EPG_radio_gp["Duration"] * 60
del EPG_radio_gp['Duration']
EPG_radio_gp["type"] = 'رادیو'
EPG_radio_gp["حوزه"] = ''

print('اتمام برنامه رادیویی')



EPG_radio_gp =  EPG_radio_gp[['channel','Row','Operator','Visit','Duration_min','m_date_new','type','حوزه']]
EPG_radio_gp.to_excel(r'D:\back up\EPG\14010124\EPG\EPG_radio_gp.xlsx', index=False)









####################################################################     Ostani
EPG_ostani_08 = pd.DataFrame()

EPG_ostani_00= pd.read_excel(r'D:\python\EPG_vahid\progress\merge\match merge\ostani.xlsx')
# EPG_ostani_00= pd.read_csv(r'D:\back up\EPG\14001018\EPG_ostani_.csv')

# connection = psycopg2.connect(user="postgres",
#                             password="12344321",
#                             host="10.32.141.17",
#                             port="5432",
#                             database="Vahid01")
# cursor = connection.cursor()
# EPG_ostani_00= psql.read_sql('select * from public."EPG_Ostani"', connection)

print('فراخونی استانی')

engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Vahid01',pool_size=20, max_overflow=100,)
con=engine.connect()

EPG_ostani_00.to_sql('EPG_Ostani',con,if_exists='append', index=False)

print('Write EPG_ostani in Postgres SQL')



# date

EPG_ostani_00['تاریخ'] = EPG_ostani_00['تاریخ'].astype(str)
EPG_ostani_01 = EPG_ostani_00
# df_EPG_8 = df_EPG
EPG_ostani_01['m_date'] = EPG_ostani_01['تاریخ'].str[:8]
# df_EPG_8['m_date'] = df_EPG_8['m_date'].astype(str).astype(int)

#rename

EPG_ostani_01 = EPG_ostani_01.rename(columns={'نام شبکه': 'channel', 'نام برنامه': 'program', 'ردیف': 'Row','اپراتور':'Operator','تعداد بازدید': 'Visit','مدت بازدید':'Duration'})


########merge

EPG_ostani_01['channel'] = EPG_ostani_01['channel'].replace({'استانی افلاک': 'استانی لرستان - افلاک'})
EPG_ostani_01['channel'] = EPG_ostani_01['channel'].replace({'استانی دنا': 'استانی کهگیلویه و بویر احمد - دنا'})
EPG_ostani_01['channel'] = EPG_ostani_01['channel'].replace({'استانی سهند': 'استانی آذربایجان شرقی - سهند'})

# EPG_ostani_01.to_excel(r'D:\back up\EPG\14001018\test\EPG_ostani_01.xlsx', index=False)
#group by

EPG_ostani_gp = EPG_ostani_01.groupby(['channel','Operator','Row','m_date']).agg({'Visit':'sum','Duration':'sum'}).reset_index()

EPG_ostani_gp['m_date_new'] = EPG_ostani_gp['m_date'].astype(str) + '01'
EPG_ostani_gp["Duration_min"] = EPG_ostani_gp["Duration"] * 60
del EPG_ostani_gp['Duration']
EPG_ostani_gp["type"] = 'استانی'
EPG_ostani_gp["حوزه"] = ''

print('اتمام برنامه استانی')


EPG_ostani_gp =  EPG_ostani_gp[['channel','Row','Operator','Visit','Duration_min','m_date_new','type','حوزه']]
EPG_ostani_gp.to_excel(r'D:\back up\EPG\14010124\EPG\EPG_ostani_gp.xlsx', index=False)


#################################################################     Ekhtesasi

EPG_ekhtesasi_00= pd.read_excel(r'D:\python\EPG_vahid\progress\merge\match merge\ekhtesasi.xlsx')
# EPG_ekhtesasi_00= pd.read_csv(r'D:\back up\EPG\14001018\EPG_ekhtesasi_.csv')

# connection = psycopg2.connect(user="postgres",
#                             password="12344321",
#                             host="10.32.141.17",
#                             port="5432",
#                             database="Vahid01")
# cursor = connection.cursor()
# EPG_ekhtesasi_00= psql.read_sql('select * from public."EPG_Ekhtesasi"', connection)

print('فراخونی اختصاصی')



engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Vahid01',pool_size=20, max_overflow=100,)
con=engine.connect()

EPG_ekhtesasi_00.to_sql('EPG_Ekhtesasi',con,if_exists='append', index=False)

print('Write EPG_Ekhtesasi in Postgres SQL')


# date

EPG_ekhtesasi_00['تاریخ'] = EPG_ekhtesasi_00['تاریخ'].astype(str)
EPG_ekhtesasi_01 = EPG_ekhtesasi_00
# df_EPG_8 = df_EPG
EPG_ekhtesasi_01['m_date'] = EPG_ekhtesasi_01['تاریخ'].str[:8]
# df_EPG_8['m_date'] = df_EPG_8['m_date'].astype(str).astype(int)

#rename
EPG_ekhtesasi_01 = EPG_ekhtesasi_01.rename(columns={'نام شبکه': 'channel', 'نام برنامه': 'program', 'ردیف': 'Row','اپراتور':'Operator','تعداد بازدید': 'Visit','مدت بازدید':'Duration'})
########merge

# EPG_ekhtesasi_01.to_excel(r'D:\back up\EPG\14001018\test\EPG_ekhtesasi_01.xlsx', index=False)
#group by

EPG_ekhtesasi_gp = EPG_ekhtesasi_01.groupby(['channel','Operator','Row','m_date']).agg({'Visit':'sum','Duration':'sum'}).reset_index()

EPG_ekhtesasi_gp['m_date_new'] = EPG_ekhtesasi_gp['m_date'].astype(str) + '01'

EPG_ekhtesasi_gp['tag'] = ''
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('اسپرت'), 'tag'] = 'ورزشی'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('اسپورت'), 'tag'] = 'ورزشی'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('استقلال'), 'tag'] = 'ورزشی'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('پرسپولیس'), 'tag'] = 'ورزشی'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('سپاهان‌ تی‌وی'), 'tag'] = 'ورزشی'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('مس رفسنجان'), 'tag'] = 'ورزشی'

EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('آقای'), 'tag'] = 'انتخابات'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('آرا'), 'tag'] = 'انتخابات'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('انتخابات'), 'tag'] = 'انتخابات'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('انتخاب'), 'tag'] = 'انتخابات'

EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('کنسرت'), 'tag'] = 'کنسرت'

EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('اکونومی'), 'tag'] = 'اقتصادی'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('اکو'), 'tag'] = 'اقتصادی'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('کیپاد'), 'tag'] = 'بانک'

EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('بورس'), 'tag'] = 'بورس'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('شتاب'), 'tag'] = 'بورس'

EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('بابک'), 'tag'] = 'سرگرمی'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('دُرفا'), 'tag'] = 'سرگرمی'

EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('کودک'), 'tag'] = 'کودک'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('دیجیتون'), 'tag'] = 'کودک'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('شاپرک'), 'tag'] = 'کودک'

EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('کاروان عشق'), 'tag'] = 'مذهبی'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('آستان قدس رضوی'), 'tag'] = 'مذهبی'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('محفل'), 'tag'] = 'مذهبی'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('لنز و ماه'), 'tag'] = 'مذهبی'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('حرم رضوی'), 'tag'] = 'مذهبی'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('باغ رمضان'), 'tag'] = 'مذهبی'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('حبیب'), 'tag'] = 'مذهبی'

EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('فیلم'), 'tag'] = 'فیلم'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('نما'), 'tag'] = 'فیلم'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('تیوا یک'), 'tag'] = 'فیلم'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('تیوا دو'), 'tag'] = 'فیلم'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('امروز'), 'tag'] = 'فیلم'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('اشاره'), 'tag'] = 'فیلم'

EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('آیو جهان‌بین'), 'tag'] = 'مستند'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('سرباز ماهر'), 'tag'] = 'مهارت آموزی'
EPG_ekhtesasi_gp.loc[EPG_ekhtesasi_gp['channel'].str.contains('جوان'), 'tag'] = 'نوجوان'


EPG_ekhtesasi_gp["Duration_min"] = EPG_ekhtesasi_gp["Duration"] * 60
del EPG_ekhtesasi_gp['Duration']
EPG_ekhtesasi_gp["type"] = 'اختصاصی'
EPG_ekhtesasi_gp["حوزه"] = EPG_ekhtesasi_gp["tag"]


EPG_ekhtesasi_gp =  EPG_ekhtesasi_gp[['channel','Row','Operator','Visit','Duration_min','m_date_new','type','حوزه']]
EPG_ekhtesasi_gp.to_excel(r'D:\back up\EPG\14010124\EPG\EPG_ekhtesasi_gp.xlsx', index=False)

print('اتمام برنامه اختصاصی')

# len(EPG)_ekhtesasi_gp)




###########################merge EPG

EPG = pd.DataFrame()

EPG = EPG.append(sarasari_gp)
EPG = EPG.append(EPG_BronMarzi)
EPG = EPG.append(EPG_radio_gp)
EPG = EPG.append(EPG_ostani_gp)
EPG = EPG.append(EPG_ekhtesasi_gp)

EPG.to_excel(r'D:\back up\EPG\14010124\EPG\EPG_gp_2.xlsx', index=False)

EPG_VOD = EPG.append(input_max_value)

EPG_VOD.to_excel(r'D:\back up\EPG\14010124\EPG_VOD_gp_2.xlsx', index=False)

print('چاپ جامع برنامه')

engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Vahid01',pool_size=20, max_overflow=100,)
con=engine.connect()

# EPG_VOD.to_sql('EPG_VOD',con,if_exists='append', index=False)

EPG_VOD.to_sql('EPG_VOD',con,if_exists='replace', index=False)

print('اتمام برنامه')
len(EPG_VOD)






end = time.time()
# total time taken
mo = (end - start)/60
print ('مدت زمان اجرا برنامه به دقیقه',mo)
print(f"Runtime of the program is {end - start}")



connection = psycopg2.connect(user="postgres",
                            password="12344321",
                            host="10.32.141.17",
                            port="5432",
                            database="Vahid01")
cursor = connection.cursor()
test_EPG_VOD =  psql.read_sql('select * from public."EPG_VOD"', connection)

len(test_EPG_VOD)





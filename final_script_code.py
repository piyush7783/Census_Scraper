#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re
import csv
import time
import xlrd
import os, sys
import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from urllib.request import Request, urlopen


# In[ ]:


final_url="http://censusindia.gov.in/2011census/dchb/DCHB.html"

try:
    def connection():
        return(requests.get(final_url,headers={'User-Agent': 'Mozilla/5.0'}))

    req= connection()
except:
    time.sleep(10)
    req= connection()
bsobj=BeautifulSoup(req.text,"lxml")
table_var =bsobj.findAll("td",{"valign":"top"})
var= table_var[1]
state_var= var.findAll("td",{"class":"style16"})
district_var= var.findAll("td",{"class":"style7"})
village_var= var.findAll("td",{"class":"style8"})

for i in var:
    try:
        var1=i.findAll("tr",{"align":"center"})
        var1=var1[2:]
    except:
        pass
    
master=[]
for i in var1:
    var2=i.findAll("td")
    state_name=var2[0].get_text()
    dist_file="http://censusindia.gov.in/2011census/dchb/"+str((var2[-3].findAll("a"))[0].get("href"))
    vill_file="http://censusindia.gov.in/2011census/dchb/"+str( (var2[-2].findAll("a"))[0].get("href"))
    master.append([state_name,dist_file,vill_file])

#Creating the folder and writing its content....................
for i in range(len(master)):
    if not os.path.exists(master[i][0]):
        os.makedirs(master[i][0])
        if not os.path.exists(master[i][0]+"/District"):
            os.makedirs(master[i][0]+"/District")
        
        if not os.path.exists(master[i][0]+"/Village"):
            os.makedirs(master[i][0]+"/Village")
    else:
        if not os.path.exists(master[i][0]+"/District"):
            os.makedirs(master[i][0]+"/District")
        if not os.path.exists(master[i][0]+"/Village"):
            os.makedirs(master[i][0]+"/Village")
    
    r = requests.get(master[i][1],headers={'User-Agent': 'Mozilla/5.0'})
    workbook = xlrd.open_workbook(file_contents=r.content)
    sheet = workbook.sheet_by_index(0) 
    sheet.cell_value(0, 0) 
    n=sheet.nrows
    heading=(sheet.row_values(0))
    combined=[]
    for j in range(1,n):
        combined.append(sheet.row_values(j))
        
    result=pd.DataFrame(combined,columns= heading)
    result.to_csv(master[i][0]+"/District/"+master[i][0]+" District.csv", encoding='utf-8', index=False)
    
    r1 = requests.get(master[i][2],headers={'User-Agent': 'Mozilla/5.0'})
    workbook = xlrd.open_workbook(file_contents=r1.content)
    sheet1 = workbook.sheet_by_index(0) 
    sheet1.cell_value(0, 0) 
    n1=sheet1.nrows 
    heading1=(sheet1.row_values(0))
    combined1=[]
    for j in range(1,n1):
        combined1.append(sheet1.row_values(j))
        
    result1=pd.DataFrame(combined1,columns= heading1)
    result1.to_csv(master[i][0]+"/Village/"+master[i][0]+" Village.csv", encoding='utf-8', index=False)

master1=[]
path = "../file downloader script"
dirs = os.listdir( path )

for file in dirs:
    if(".ipynb" not in file):
        master1.append(file)

engine = create_engine('mysql+pymysql://root:@localhost/dataset')
for i in range(len(master1)):
    # For Town 
    df=pd.read_csv(master1[i]+"/District/"+master1[i]+" District.csv")
    column_name= df.columns.tolist()
    column_map={column_name[j]:"var_"+str(j) for j in range(len(column_name))}
    for k in range(len(column_name)):
        df=df.rename(columns = {column_name[k] : column_map.get(column_name[k])})
    df.to_sql('Town', con=engine, if_exists='append',index=False)
df1= column_map.items()
df1=list(df1)
lst=[]
for i in df1:
    lst.append(list(i))
df2=pd.DataFrame(lst)
df2.to_sql('Town_map', con=engine, if_exists='append',index=False)

for i in range(len(master1)):
    #For Village
    df=pd.read_csv(master1[i]+"/Village/"+master1[i]+" Village.csv")
    column_name= df.columns.tolist()
    column_map={column_name[j]:"var_"+str(j) for j in range(len(column_name))}
    for k in range(len(column_name)):
        df=df.rename(columns = {column_name[k] : column_map.get(column_name[k])})
    df.to_sql('Village', con=engine, if_exists='append',index=False)
df1= column_map.items()
df1=list(df1)
lst=[]
for i in df1:
    lst.append(list(i))
df2=pd.DataFrame(lst)
df2.to_sql('Village_map', con=engine, if_exists='append',index=False)
print("All the data has stored successfully..................")


import pandas as pd
import requests
import time 
import json
import configparser
from sqlalchemy import create_engine

def connect_MySQL():
    #MySQL connection information
    config=configparser.ConfigParser()
    config.read('config.ini')
    
    mysql_engine=create_engine(f"{config['MySQL']['driver']}://{config['MySQL']['username']}:{config['MySQL']['password']}@{config['MySQL']['host']}/{config['MySQL']['database']}") 
    return mysql_engine

def connect_AWSRedshift():
    #AWSRedshift connection information
    config=configparser.ConfigParser()
    config.read('config.ini')
  
    AWSRedshift_engine=create_engine(f"{config['AWSRedshift']['driver']}://{config['AWSRedshift']['username']}:{config['AWSRedshift']['password']}@{config['AWSRedshift']['host']}:{config['AWSRedshift']['port']}/{config['AWSRedshift']['database']}")
    return AWSRedshift_engine

def Get_CoSoBanLe_data():
    url = f"https://drugbank.vn/services/drugbank/api/public/co-so-kinh-doanh"
    payload={
        'page':1,
        'size':20,
        'sort':'id,asc'
    }

    headers = {
        'authority': 'drugbank.vn',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9,vi;q=0.8',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
        'x-kl-ajax-request': 'Ajax_Request'
    }
    df_csbl=pd.DataFrame()
    for i in range(10):
        payload['page']=i 
        response = requests.request("GET", url, headers=headers, data=payload)
        data_temp=response.json()
        df_temp=pd.DataFrame(data_temp)
        df_csbl=pd.concat([df_csbl,df_temp],ignore_index=True)

    print('Get all data from CoSoBanLe sucessfully')
    return df_csbl

def Get_CoSoPhanPhoi_data():
    url = f"https://drugbank.vn/services/drugbank/api/public/co-so-phan-phoi"
    payload={
        'page':1,
        'size':20,
        'sort':'id,asc'
    }

    headers = {
        'authority': 'drugbank.vn',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9,vi;q=0.8',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
        'x-kl-ajax-request': 'Ajax_Request'
    }
    df_cspp=pd.DataFrame()
    for i in range(10):
        payload['page']=i 
        response = requests.request("GET", url, headers=headers, data=payload)
        data_temp=response.json()
        df_temp=pd.DataFrame(data_temp)
        df_cspp=pd.concat([df_cspp,df_temp],ignore_index=True)
    
    print('Get all data from CoSoPhanPhoi sucessfully')    
    return df_cspp

def Get_CoSoSanXuat_data():
    url = f"https://drugbank.vn/services/drugbank/api/public/co-so-san-xuat"
    payload={
        'page':1,
        'size':20,
        'sort':'id,asc'
    }

    headers = {
        'authority': 'drugbank.vn',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9,vi;q=0.8',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
        'x-kl-ajax-request': 'Ajax_Request'
    }
    df_cssx=pd.DataFrame()
    for i in range(10):
        payload['page']=i 
        response = requests.request("GET", url, headers=headers, data=payload)
        data_temp=response.json()
        df_temp=pd.DataFrame(data_temp)
        df_cssx=pd.concat([df_cssx,df_temp],ignore_index=True)
     
    print('Get all data from CoSoSanXuat sucessfully')  
    return df_cssx

def Clean_and_Load_data():
    df_csbl=Get_CoSoBanLe_data()
    df_cspp=Get_CoSoPhanPhoi_data()
    df_cssx=Get_CoSoSanXuat_data()
    
    #Clean CoSoBanLe data
    del df_csbl['stt']
    df_csbl['issueDate']=pd.to_datetime(df_csbl['issueDate'], errors='coerce', utc=True).dt.strftime('%Y-%m-%d')
    df_csbl['ngayCapCchn']=pd.to_datetime(df_csbl['ngayCapCchn'], errors='coerce', utc=True).dt.strftime('%Y-%m-%d')
    print('Clean CoSoBanLe data successfully')
    #Clean CoSoPhanPhoi data
    df_cspp['phoneContact']=df_cspp['phoneContact'].str.replace('.','').replace(' ','')
    df_cspp['phoneContact']=df_cspp['phoneContact'].str.extract('(\d+)')
    df_cspp['phoneNumber']=df_cspp['phoneNumber'].str.replace('.','').replace(' ','')
    df_cspp['phoneNumber']=df_cspp['phoneNumber'].str.extract('(\d+)')
    
    df_cspp['cchnDate']=pd.to_datetime(df_cspp['cchnDate'], errors='coerce', utc=True).dt.strftime('%Y-%m-%d')
    df_cspp['dkkdDate']=pd.to_datetime(df_cspp['dkkdDate'], errors='coerce', utc=True).dt.strftime('%Y-%m-%d')
    df_cspp['gdpDate']=pd.to_datetime(df_cspp['gdpDate'], errors='coerce', utc=True).dt.strftime('%Y-%m-%d')
    df_cspp['expirationGdpDate']=pd.to_datetime(df_cspp['expirationGdpDate'], errors='coerce', utc=True).dt.strftime('%Y-%m-%d')
    df_cspp['expirationGdpDate']=pd.to_datetime(df_cspp['expirationGdpDate'], errors='coerce', utc=True).dt.strftime('%Y-%m-%d')
    print('Clean CoSoPhanPhoi data successfully')
    #Clean CoSoSanXuat data
    del df_cssx['images']
    df_cssx['dateCCHN']=pd.to_datetime(df_cssx['dateCCHN'], errors='coerce', utc=True).dt.strftime('%Y-%m-%d')
    df_cssx['dateDkkd']=pd.to_datetime(df_cssx['dateDkkd'], errors='coerce', utc=True).dt.strftime('%Y-%m-%d')
    df_cssx['dateGdp']=pd.to_datetime(df_cssx['dateGdp'], errors='coerce', utc=True).dt.strftime('%Y-%m-%d')
    df_cssx['expirationDateGdp']=pd.to_datetime(df_cssx['expirationDateGdp'], errors='coerce', utc=True).dt.strftime('%Y-%m-%d')
    print('Clean CoSoSanXuat data successfully')
    #Load data to MySQL database
    mysql_engine=connect_MySQL()
    
    df_csbl.to_sql(name='coso_banle',con=mysql_engine,if_exists='append',index=False)
    print('Insert data from CoSoBanLe to table coso_banle successfully')
    time.sleep(2)
    
    df_cspp.to_sql(name='coso_phanphoi',con=mysql_engine,if_exists='append',index=False)
    print('Insert data from CoSoBanLe to table coso_phanphoi successfully')
    time.sleep(2)
    
    df_cssx.to_sql(name='coso_sanxuat',con=mysql_engine,if_exists='append',index=False)
    print('Insert data from CoSoBanLe to table coso_sanxuat successfully')
    time.sleep(2)
    
    #Load data to excel file
    df_csbl.to_excel('CoSoBanLe.xlsx')
    print('Insert data from CoSoBanLe to excel successfully')
    
    df_cspp.to_excel('CoSoPhanPhoi.xlsx')
    print('Insert data from CoSoPhanPhoi to excel successfully')
    
    df_cssx.to_excel('CoSoSanXuat.xlsx')
    print('Insert data from CoSoSanXuat to excel successfully')
       
if __name__ == "__main__":
    start=time.perf_counter()
    
    Clean_and_Load_data()
    
    end=time.perf_counter()
    print(f'Execute time: {end-start}')
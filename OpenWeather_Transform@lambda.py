import json
import pandas as pd
import boto3
from datetime import datetime
from io import StringIO

s3_client=boto3.client('s3')
Buc='openweather-etl-harsha'

def delete_weather_csv(weather_path):
    try:
        weather_file=[]
        res=s3_client.list_objects(Bucket=Buc,Prefix=weather_path)
            
        if 'Contents' not in res:
            return
        for c in res['Contents']:
            if c['Key'].split('.')[-1]=='csv':
                weather_file.append(c['Key'].split('/')[-1])
        for file in weather_file:
            s3_client.delete_object(Bucket=Buc,Key=weather_path+file)
    except Exception as e:
        print(e)

def delete_city_csv(city_path):
    try:
        city_file=[]
        res=s3_client.list_objects(Bucket=Buc,Prefix=city_path)
            
        if 'Contents' not in res:
            return
        for c in res['Contents']:
            if c['Key'].split('.')[-1]=='csv':
                city_file.append(c['Key'].split('/')[-1])
        for file in city_file:
            s3_client.delete_object(Bucket=Buc,Key=city_path+file)
    except Exception as e:
        print(e)

def city_weather(weather_city_list):
    weather_list=[]
    for file in weather_city_list:
        for row in file:
            weather_dict={
                'id':row['id'],'max_temp':row['main']['temp_max'],'min_temp':row['main']['temp_min'],
                  'pressure':row['main']['pressure'],'sea_level':row['main']['sea_level'],'humidity':row['main']['humidity'],'description':row['weather'][0]['description'],
            'sunrise':row['sys']['sunrise'],'sunset':row['sys']['sunset'],'timezone':row['timezone']
            }
            weather_list.append(weather_dict)
    weather_df=pd.DataFrame(weather_list)
    weather_df.rename(columns={'id':'city_id','max_temp':'max_temp C','min_temp':'min_temp C','pressure':'pressure hPa','sea_level':'sea_level hPa','humidity':'humidity %',
                     'sunrise':'sunrise UTC','sunset':'sunset UTC','timezone':'timezone UTC'
                    },inplace=True)
    weather_df.drop_duplicates(subset=['city_id'],keep='first',inplace=True)
    timezone_utc=[]
    for time in weather_df['timezone UTC']:
        hour=time/3600
        if hour>0:
            t='UTC+' + str(int(hour))
            timezone_utc.append(t)
        else:
            t='UTC' + str(int(hour))
            timezone_utc.append(t)
    weather_df['timezone UTC']=timezone_utc
    sunrise_utc=[]
    for rise in weather_df['sunrise UTC']:
        rise_obj=datetime.fromtimestamp(rise)
        utc_rise=rise_obj.strftime('%Y-%m-%d %H:%M:%S')
        sunrise_utc.append(utc_rise)
    weather_df['sunrise UTC']=sunrise_utc
    sunset_utc=[]
    for set in weather_df['sunset UTC']:
        set_obj=datetime.fromtimestamp(set)
        utc_set=set_obj.strftime('%Y-%m-%d %H:%M:%S')
        sunset_utc.append(utc_set)
    weather_df['sunset UTC']=sunset_utc
    return weather_df
    
def city_details(weather_city_list):
    city_list=[]
    for file in weather_city_list:
        for row in file:
            city_dict={
            'id':row['id'],'city':row['name'],'country':row['sys']['country'],'longitude':row['coord']['lon'],'latitude':row['coord']['lat'],'timezone':row['timezone']
            }
            city_list.append(city_dict)
    city_df=pd.DataFrame(city_list)
    city_df.rename(columns={'timezone':'timezone UTC'},inplace=True)
    timezone_utc=[]
    for time in city_df['timezone UTC']:
        hour=time/3600
        if hour>0:
            t='UTC+'+str(int(hour))
            timezone_utc.append(t)
        else:
            t='UTC'+str(int(hour))
            timezone_utc.append(t)
    city_df['timezone UTC']=timezone_utc
    return city_df

            
def lambda_handler(event, context):
    delete_weather_csv('transformed/weather_data/')
    delete_city_csv('transformed/city_data/')
    file_path_json='raw_folder/to_process/'
    file_keys=[]
    weather_city_list=[]
    for file in s3_client.list_objects(Bucket=Buc,Prefix=file_path_json)['Contents']:
        ky=file['Key']
        if file['Key'].split('.')[-1]=='json':
            file_keys.append(file['Key'].split('/')[-1])
            s3_obj=s3_client.get_object(Bucket=Buc,Key=ky)
            content_binary=s3_obj['Body'].read().decode('utf-8')
            weather_city=json.loads(content_binary)
            weather_city_list.append(weather_city)
    weather_df=city_weather(weather_city_list)
    city_df=city_details(weather_city_list)

    io_weather=StringIO()
    weather_df.to_csv(io_weather,index=False)
    
    try:
        s3_client.put_object(
        Bucket=Buc,
        Key='transformed/weather_data/openweather_transformed_'+str(datetime.now())+'.csv',
        Body=io_weather.getvalue()
        )
    except Exception as e:
        print(e)
    io_city=StringIO()
    city_df.to_csv(io_city,index=False)
    
    try:
        s3_client.put_object(
        Bucket=Buc,
        Key='transformed/city_data/openweather_transformed_'+str(datetime.now())+'.csv',
        Body=io_city.getvalue()
        )
    except Exception as e:
        print(e)
        
    for file in file_keys:
        s3_client.copy_object(Bucket=Buc,CopySource={'Bucket':Buc,'Key':'raw_folder/to_process/'+file},Key='raw_folder/processed/'+file)
        s3_client.delete_object(Bucket=Buc,Key='raw_folder/to_process/'+file)
        
    
        
        
    
    
    
    
            
    
    

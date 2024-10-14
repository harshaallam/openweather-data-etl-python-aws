import json
import pandas as pd
from datetime import datetime
import boto3
import requests
import os


def lambda_handler(event, context):
    
    client_key=os.environ.get('client_key')
    url=os.environ.get('url')
    cities=['Washington','Chicago','Dallas','Atlanta','San Francisco','Los Angeles','Houston','San Diego','San Antonio','Austin','Philadelphia','New York','London','Visakhapatnam','Bengaluru','Hyderabad','Chennai','Delhi','Mumbai','Pune','Dubai','Riyadh','Abu Dhabi','Doha','Muscat',
    'Gaza','Beirut','Tehran','Jerusalem','Manama','Baghdad','Kabul','Lahore','Karachi','Islamabad','Dhaka','Tokyo','Seoul','Beijing','Shanghai','Taipei','Busan','Cairo','Lagos','Nairobi']
    weather_city_list=[]
    for city in cities:
        try:
            param={
                'q':city,
                'appid':client_key,
                'units':'metrics'
            }
            weather_res=requests.get(url,params=param)
            if weather_res.status_code==200:
                weather_city=weather_res.json()
                if weather_city.get('cod')==200:
                    weather_city_list.append(weather_city)
            else:
                print(f"city not available: {city}")
        except Exception as e:
            print(f"{city} not availble {e}")
    
    file_name='openweather_cities_data_'+str(datetime.now())+'.json'
    
    client_obj=boto3.client('s3')
    client_obj.put_object(
            Bucket='openweather-etl-harsha',
            Key='raw_folder/to_process/'+file_name,
            Body=json.dumps(weather_city_list)
        )
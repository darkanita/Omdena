import pandas as pd
import herepy
from io import StringIO 
import boto3
import datetime
from datetime import datetime
import numpy as np

pd.options.mode.chained_assignment = None 

def load_data(url):
    '''
        Load dataset from storage, file csv
    '''
    filepath = url
    data = pd.read_csv(filepath)
    
    return data

def get_location_coord(lat,lon,loc, app_id,app_code):
    '''
        Get address and position information by coordinates or location, app_id and app_code are generate when you sing up in here.com in freemium version. 
        If latitude coordinate is null, get information by location data.
    '''

    geocoderApi = herepy.GeocoderApi(app_id, app_code)
    geocoderReverseApi = herepy.GeocoderReverseApi(app_id, app_code)
    address = None
    position = None

    if lat is None or np.isnan(lat):
        response = geocoderApi.free_form(loc)
        if response.as_dict()['Response']['View']:
            address = response.as_dict()['Response']['View'][0]['Result'][0]['Location']['Address']
            position = response.as_dict()['Response']['View'][0]['Result'][0]['Location']['DisplayPosition']
    else:
        response = geocoderReverseApi.retrieve_addresses([lat, lon])
        if response.as_dict()['Response']['View']:
            address = response.as_dict()['Response']['View'][0]['Result'][0]['Location']['Address']
            position = response.as_dict()['Response']['View'][0]['Result'][0]['Location']['DisplayPosition']
        else:
            response = geocoderApi.free_form(loc)
            if response.as_dict()['Response']['View']:
                address = response.as_dict()['Response']['View'][0]['Result'][0]['Location']['Address']
                position = response.as_dict()['Response']['View'][0]['Result'][0]['Location']['DisplayPosition']       
    
    return address,position

def upload_data(data,bucket,key,aws_access_key_id,aws_secret_access_key):
    '''
        Upload data to storage S3, aws_acces_key_id and aws_secret_access_key are provided by AWS.
        This function is only if you require save the information in SW. 
    '''

    client = boto3.client('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    obj = client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue(), ACL='public-read')

    return obj

def format_date(data,column,formatDate='%Y-%m-%d %H:%M:%S', sourceDate=['%d %b %Y, %I %M %p']):
    '''
        Format column date in format defined in formatDate, the date can be differents formats, you need to list they in sourceDate
    '''
    for row in range(len(data[column])): 
        try:
            data[column][row]= pd.to_datetime(data[column][row])
        except:
            for valformat in sourceDate:
                try:
                    data[column][row] = datetime.strptime(data[column][row], valformat).strftime(formatDate)
                except:
                    print(data[column][row])
    
    data['YEAR'] = data[column].dt.year
    data['MONTH'] = data[column].dt.month
    data['DAY'] = data[column].dt.day
    data['HOUR'] = data[column].dt.hour
    data['DAYOFWEEK'] = [datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S').strftime('%A') for date in data[column]]

    return data

def drop_duplicates(data,key):
    data = data.drop_duplicates(key).reset_index(drop=True)
    return data


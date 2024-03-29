import pandas as pd
import herepy
from io import StringIO 
import boto3
import datetime
from datetime import datetime
import numpy as np
import nltk
from nltk import sent_tokenize
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
import sys
import ast
from googletrans import Translator
import time
#from langdetect import detect 
#from translate import Translator
import string
import re
from autocorrect import spell
from tqdm import tqdm

nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

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

    data[column] = pd.to_datetime(data[column])
    data['YEAR'] = data[column].dt.year
    data['MONTH'] = data[column].dt.month
    data['DAY'] = data[column].dt.day
    data['HOUR'] = data[column].dt.hour
    data['DAYOFWEEK'] = [datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S').strftime('%A') for date in data[column]]
    
    return data

def drop_duplicates(data,key):
    '''
        Drop duplicates by the columns in key.
    '''
    data = data.drop_duplicates(key).reset_index(drop=True)
    
    return data

def add_data_location(data,app_id,app_code,columns=['LATITUDE','LONGITUDE','LOCATION']):
    '''
        Complete information with coordinates geographics or with location.
    '''
    data['ADDRESS'] = None
    data['POSITION'] = None
    data['COUNTRY'] = None
    data['STATE'] = None
    data['COUNTY'] = None
    data['LABEL'] = None
    data['CITY'] = None
    data['DISTRICT'] = None
    data['STREET'] = None
    problems = []
    for row in range(len(data['LATITUDE'])):
        lat = data[columns[0]][row]
        lon = data[columns[1]][row]
        loc = data[columns[2]][row]
        try:
            address, position = get_location_coord(lat,lon,loc,app_id,app_code)
            data['ADDRESS'][row] = address
            data['POSITION'][row] = position
            data['COUNTRY'][row] = address['AdditionalData'][0]['value']
            data['STATE'][row] = address['AdditionalData'][1]['value']
            data['COUNTY'][row] = address['AdditionalData'][2]['value']
            data['LABEL'][row] = address['Label']
            if 'City' in address.keys():
                data['CITY'][row] = address['City']
            if 'District' in address.keys():
                data['DISTRICT'][row] = address['District']
            if 'Street' in address.keys():
                data['STREET'][row] = address['Street']
            if np.isnan(data[columns[0]][row]):
                data[columns[0]][row] = position['Latitude']
                data[columns[1]][row] = position['Longitude'] 
        except:
            problems.append(row) 
    print(problems)
    new_problems = []
    for row in problems:
        try:
            address, position = get_location_coord(lat,lon,loc,app_id,app_code)
            data['ADDRESS'][row] = address
            data['POSITION'][row] = position
            data['COUNTRY'][row] = address['AdditionalData'][0]['value']
            data['STATE'][row] = address['AdditionalData'][1]['value']
            data['COUNTY'][row] = address['AdditionalData'][2]['value']
            data['LABEL'][row] = address['Label']
            if 'City' in address.keys():
                data['CITY'][row] = address['City']
            if 'District' in address.keys():
                data['DISTRICT'][row] = address['District']
            if 'Street' in address.keys():
                data['STREET'][row] = address['Street']
            if np.isnan(data[columns[0]][row]):
                data[columns[0]][row] = position['Latitude']
                data[columns[1]][row] = position['Longitude'] 
        except:
            new_problems.append(row)
    
    return data, new_problems


def create_category_columns(data,column='CATEGORY'):
    '''
        Create column for category
    '''   
    category = []
    for row in range(len(data[column])):
        for cat in data[column][row].split(','):
            if cat != ' ':
                cat = cat.replace('Harrassment','Harassment')
                cat = cat.replace('Indecent exposure', 'Indecent Exposure/Masturbation in public')
                cat = cat.replace('Taking pictures without permission','Taking pictures')
                cat = cat.replace('Ogling/Lewd Facial Expressions/Staring','Ogling/Facial Expressions/Staring')
                if cat.strip() not in category:
                    category.append(cat.strip())
                    if cat.strip() not in data.columns:
                        data[cat.strip()] = 0
        
                data[cat.strip()][row] = 1 
    
    data['NUMBER_CAT'] = [len(val[:-2].split(',')) for val in data[column]]
    
    return data, category

def normalize_text(data,column='INCIDENT TITLE'):
    porter = PorterStemmer()
    WNlemma = nltk.WordNetLemmatizer()
    stop_words = set(stopwords.words('english'))
    for row in range(len(data[column])):
        try:
            data[column+' SENTENCES'][row] = str(sent_tokenize(data[column][row]))
            #data[column+' TOKENS'] = str(word_tokenize(data[column][row]))
        except Exception as e:
            #print("SENTENCES"+str(e))
            data[column+' SENTENCES'][row] = str([])
            #print(data[column][row])
    for row in range(len(data[column])):
        try:
            #data[column+' SENTENCES'] = str(sent_tokenize(data[column][row]))
            data[column+' TOKENS'][row] = str(word_tokenize(data[column][row]))
        except Exception as e:
            #print("TOKENS "+str(e))
            data[column+' TOKENS'][row] = str([])
            #print(data[column][row])
    
    words_total = []
    table = str.maketrans('', '', string.punctuation)
    pattern = '[0-9]'
    for row in range(len(data[column])):
        words = []
        for word in ast.literal_eval(data[column+' TOKENS'][row]):
            word = str(re.sub(pattern, '', word))
            word = word.lower()
            if word.isalpha() and not word in stop_words and len(word)>2:
                try:
                    value = porter.stem(WNlemma.lemmatize(word,pos='v'))
                    words_total.append(value)
                except Exception as e:
                    value = None
                    #print("NORMALIZE "+str(e))
                    #print(word)
                words.append(value)
        data[column+' WORDS'][row] = str(words)

    #data[column+' SENTENCES'] = [str(sent_tokenize(text)) for text in data[column]]
    #data[column+' TOKENS'] = [word_tokenize(text) for text in data[column]]
    #data[column+' WORDS'] = [[porter.stem(WNlemma.lemmatize(word.lower())) for word in text if word.isalpha() and not word in stop_words] 
    return data,words_total

def detect_language(text,translator,problems,row,tries = 1):
    try:
        return translator.detect(text).lang, problems
    except Exception as e:
        print(str(e))
    except:    
            if tries > 5:
                print(text,row)
                problems.append(row)
                return None, problems
            tri = tries + 1
            print("Fail, trying again Number. {0} ".format(tries))
            return detect_language(text,translator,problems,row,tries = tri)

def translate_text(text,translator,problems,row,tries = 1):
    try:
        return translator.translate(text), problems
    except Exception as e:
        print(str(e))
    except:    
            if tries > 5:
                print(text,row)
                problems.append(row)
                return None, problems
            tri = tries + 1
            print("Fail, trying again Number. {0} ".format(tries))
            return translate_text(text,translator,problems,row,tries = tri)

def translate_columns(data,column='INCIDENT TITLE', spell=False, tries = 1):
    problems = []
    #translator= Translator(to_lang="English")
    for row in tqdm(range(len(data[column]))):
        pattern_wd_eng = (r'[A-Za-z0-9.,]+')
        try:
            translator = Translator(service_urls=['translate.google.com','translate.google.co.kr','translate.google.co.in','translate.google.com.br','translate.google.co.id','translate.google.co.th',])
            if data[column][row]:
                data[column][row] = str(' '.join(re.findall(pattern_wd_eng, data[column][row])))
                #lang = translator.detect(data[column][row]).lang
                lang,problems = detect_language(data[column][row],translator,problems,row)
                data['language'][row] = lang
                if lang != 'en' :
                    to_translate = sent_tokenize(data[column][row])
                    if spell:
                        to_translate = [spell(val) for val in to_translate]
                    
                    traduced, problems = translate_text(to_translate,translator,problems,row)
                    if traduced:
                        new_text = [val.text for val in traduced]
                        data[column][row] = str(' '.join(new_text))
                    else:
                        data[column][row] = None
            else:
                data[column][row] = None
        except Exception as e:
            print(str(e)) 
    return data,problems
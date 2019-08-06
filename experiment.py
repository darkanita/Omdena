from data import *

pd.options.mode.chained_assignment = None 

# Conection to HERE.COM
app_id = 'aBCYHqNarl95prQ6M6RN'
app_code = 'MGGRNCaXh4WHjFgnef7SlA'

# Conection to AWS S3
aws_access_key_id='YOUR KEY'
aws_secret_access_key='YOUR KEY'

bucket = 'darkanita'
key = 'Safety_GPS.csv'

urlDataSet = 'https://darkanita.s3-sa-east-1.amazonaws.com/Safecity+Reports+-+28072019.csv'
dataSet = load_data(urlDataSet)
print(dataSet.shape)
dataSet = format_date(dataSet,'INCIDENT DATE')
dataSet = drop_duplicates(dataSet,['#'])
print(dataSet.shape)
dataSet = drop_duplicates(dataSet,['INCIDENT TITLE', 'INCIDENT DATE', 'LOCATION', 'DESCRIPTION','CATEGORY', 'LATITUDE', 'LONGITUDE', 'More Info'])
print(dataSet.shape)
dataSet,category = create_category_columns(dataSet)
print(dataSet.shape)
dataSet,problems = add_data_location(dataSet,app_id,app_code)
print(dataSet.shape)
print(dataSet.head())
obj = upload_data(dataSet,bucket,key,aws_access_key_id,aws_secret_access_key)
print(obj)
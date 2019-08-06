from data import *

pd.options.mode.chained_assignment = None 

# Conection to HERE.COM
app_id = 'aBCYHqNarl95prQ6M6RN'
app_code = 'MGGRNCaXh4WHjFgnef7SlA'

# Conection to AWS S3
aws_access_key_id='YOUR ACCESS KEY ID
aws_secret_access_key='YOUR SECRET ACCESS KEY'

urlDataSet = 'https://darkanita.s3-sa-east-1.amazonaws.com/Safecity+Reports+-+28072019.csv'
dataSet = load_data(urlDataSet)
dataSet.shape
dataSet = format_date(dataSet,'INCIDENT DATE')
dataSet = drop_duplicates(dataSet,['INCIDENT TITLE', 'INCIDENT DATE', 'LOCATION', 'DESCRIPTION','CATEGORY', 'LATITUDE', 'LONGITUDE', 'More Info'])
dataSet.shape
dataSet = drop_duplicates(dataSet,['#'])
dataSet.shape
dataSet,problems = add_data_location(dataSet,app_id,app_code)
dataSet.shape
dataSet,category = create_category_columns(dataSet)
dataSet.shape
dataSet.head()
obj = upload_data(dataSet,bucket,key,aws_access_key_id,aws_secret_access_key)
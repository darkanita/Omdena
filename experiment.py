from data import *

pd.options.mode.chained_assignment = None 

bucket = 'darkanita'
key = 'Safety_GPS.csv'

def first_preprocessing(data):
    dataSet = format_date(dataSet,'INCIDENT DATE')
    dataSet = drop_duplicates(dataSet,['#'])
    print(dataSet.shape)
    dataSet = drop_duplicates(dataSet,['INCIDENT TITLE', 'INCIDENT DATE', 'LOCATION', 'DESCRIPTION','CATEGORY', 'LATITUDE', 'LONGITUDE', 'More Info'])
    print(dataSet.shape)
    dataSet,category = create_category_columns(dataSet)
    print(dataSet.shape)
    print(category)
    dataSet,problems = add_data_location(dataSet,app_id,app_code)
    print(problems)
    print(dataSet.shape)
    return data

def main(aws_access_key_id,aws_secret_access_key,app_id,app_code):
    #urlDataSet = 'https://darkanita.s3-sa-east-1.amazonaws.com/Safecity+Reports+-+28072019.csv'
    #dataSet = load_data(urlDataSet)
    #print(dataSet.shape)
    #dataSet = first_preprocessing(dataSet)
    urlDataSet = 'https://darkanita.s3-sa-east-1.amazonaws.com/Safety_GPS1.csv'
    dataSet = load_data(urlDataSet)
    print(dataSet.shape)
    dataSet,problems = add_data_location(dataSet,app_id,app_code)
    print(dataSet.shape)
    print(len(problems)
    dataSet,problems = add_data_location(dataSet,app_id,app_code,problems)
    print(dataSet.shape)
    print(len(problems)
    print(dataSet.head())
    print(dataSet['COUNTRY'].unique())
    obj = upload_data(dataSet,bucket,key,aws_access_key_id,aws_secret_access_key)
    print(obj)

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description="Prepare dataset")

    parser.add_argument('--aws_access_key_id', '-aws_id',
        help="aws_access_key_id"
    )

    parser.add_argument('--aws_secret_access_key', '-aws_key',
        help="aws_secret_access_key"
    )

    parser.add_argument('--here_id', '-app_id',
        help="app_id"
    )

    parser.add_argument('--here_code', '-app_code',
        help="app_code"
    )
    
    args = parser.parse_args()
    main(args.aws_access_key_id, args.aws_secret_access_key, args.here_id,args.here_code)
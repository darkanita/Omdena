from data import *

pd.options.mode.chained_assignment = None 

bucket = 'darkanita'
key = 'Safety_GPS1.csv'

def first_preprocessing(data,app_id,app_code):
    data = format_date(data,'INCIDENT DATE')
    data = drop_duplicates(data,['#'])
    print(data.shape)
    data = drop_duplicates(data,['INCIDENT TITLE', 'INCIDENT DATE', 'LOCATION', 'DESCRIPTION','CATEGORY', 'LATITUDE', 'LONGITUDE', 'More Info'])
    print(data.shape)
    data,category = create_category_columns(data)
    print(data.shape)
    print(category)
    data,problems = add_data_location(data,app_id,app_code)
    print(problems)
    print(data.shape)
    return data

def main(aws_access_key_id,aws_secret_access_key,app_id,app_code):
    #urlDataSet = 'https://darkanita.s3-sa-east-1.amazonaws.com/Safecity+Reports+-+28072019.csv'
    #dataSet = load_data(urlDataSet)
    #print(dataSet.shape)
    #dataSet = first_preprocessing(dataSet,app_id,app_code)
    urlDataSet = 'https://darkanita.s3-sa-east-1.amazonaws.com/Safety_GPS1.csv'
    dataSet = load_data(urlDataSet)
    #dataSet_India = dataSet[dataSet['COUNTRY']=='India']
    #dataSet_India = dataSet_India[dataSet_India['CITY'].isin(['Delhi','Mumbai'])]
    print(dataSet.shape)
    #dataSet,problemsT = translate_columns(dataSet,'INCIDENT TITLE')
    #print(problemsT)
    #obj = upload_data(dataSet,bucket,key,aws_access_key_id,aws_secret_access_key)
    #print(obj)
    #dataSet,problemsD = translate_columns(dataSet,'DESCRIPTION')
    #print(problemsD)
    #obj = upload_data(dataSet,bucket,key,aws_access_key_id,aws_secret_access_key)
    #print(obj)
    dataSet,wordsT = normalize_text(dataSet,'INCIDENT TITLE')
    print(list(set(wordsT)))
    #obj = upload_data(dataSet,bucket,key,aws_access_key_id,aws_secret_access_key)
    #print(obj)
    dataSet,wordsD = normalize_text(dataSet,'DESCRIPTION')
    print(list(set(wordsD)))
    print(dataSet[['INCIDENT TITLE','INCIDENT TITLE WORDS']].head())
    print(dataSet[['DESCRIPTION','DESCRIPTION WORDS']].head())
    #print(dataSet[dataSet['DESCRIPTION WORDS'].isna()]['DESCRIPTION'].head())
    #print(dataSet_India['INCIDENT TITLE'].unique()[:100])
    #print(dataSet['STATE'].unique())
    #print(dataSet['COUNTY'].unique())
    #print(dataSet['CITY'].unique())
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
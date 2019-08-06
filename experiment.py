from data import *

pd.options.mode.chained_assignment = None 


urlDataSet = 'https://darkanita.s3-sa-east-1.amazonaws.com/Safecity+Reports+-+28072019.csv'
dataSet = load_data(urlDataSet)


dataSet = format_date(dataSet,'INCIDENT DATE')

dataSet.head()
import pandas as pd
import json
import subprocess

df = pd.read_excel('lit_long_zeros.xlsx')
de = pd.read_excel('fot_locations1.xlsx')

df = df.drop(columns=['structureID', 'area', 'siteName', 'structType', 'addressLine1', 'addressLine2', 'city',
                      'postcode', 'lit', 'buType', 'owned_or_managed', 'groundElevation', 'heightNoAppurt', 'heightWithAppurt',
                      'last_TIA_Inspection_Data','TIA_ExemptIndicator', '_24MonthEligible?', 'pre-ConstructionSiteWalk1_CM',
                      'pre-ConstructionSiteWalk2_CM', 'pre-ConstructionSiteWalk3_CM', 'vegetation1', 'vegetation2',
                      'constructionGC1', 'constructionGC2', 'constructionGC3'])

df['fot_longlat'] = [str(df['long'][x])+','+str(df['lat'][x]) for x in range(len(df))]
de['fot_longlat'] = [str(de['fot_long'][x])+','+str(de['fot_lat'][x]) for x in range(len(de))]

dur = pd.DataFrame()
dis = pd.DataFrame()

def distance_calculate(j):
    try:
        str1 = "http://0.0.0.0:5000/route/v1/driving/" + fot_loc + ";" + str(df["fot_longlat"][j]) + "?steps=false"
        process = subprocess.run(['curl', str1], check=True, stdout=subprocess.PIPE, universal_newlines=True)
        output = process.stdout
        datastore = json.loads(output)  ##Loading into Json
        dstnc = (datastore['routes'][0]['distance'])/1609   ## Read Distance from Json
    except:
        dstnc = "no path"
    return (dstnc)

def duration_calculate(j):
    try:
        str1 = "http://0.0.0.0:5000/route/v1/driving/" + fot_loc + ";" + str(df["fot_longlat"][j]) + "?steps=false"
        process = subprocess.run(['curl', str1], check=True, stdout=subprocess.PIPE, universal_newlines=True)
        output = process.stdout
        datastore = json.loads(output)  ##Loading into Json
        drtn = (datastore['routes'][0]['duration'])/3600 ## Read Duration from Json
    except:
        drtn = "no path"
    return(drtn)

from multiprocessing import Pool
import timeit, sys

for i in range(len(de['fot_longlat'])):
#for i in range(2):
    distance_list = []
    duration_list = []
    startTime = timeit.default_timer()
    eu_reco = []
    if __name__ == '__main__':
        agents = 16
        chunksize = 16
        fot_loc = str(de['fot_longlat'][i])
        with Pool(processes=agents) as pool:
            distance_list.append(pool.map(distance_calculate, range(0,len(df['fot_longlat'])), chunksize))
            duration_list.append(pool.map(duration_calculate, range(0,len(df['fot_longlat'])), chunksize))

    stopTime = timeit.default_timer()-startTime
    round_time = round(stopTime,6)
    #print(round_time)
    dis[de['city'][i]] = distance_list[0]
    dur[de['city'][i]] = duration_list[0]
    dis.to_csv('distance.csv')
    dur.to_csv('duration.csv')
    #print(i)

dis.to_csv('distance.csv')
dur.to_csv('duration.csv')
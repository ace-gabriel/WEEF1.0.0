import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
from lib import *
from rent import *
from appr import *
from stats import *
from irr import *
import geopy.distance as dist


es = Elasticsearch(['http://localhost:59890'])


NEARBY_RANGE_KM = "2mi"
NEARBY_RANGE_SCORE = "3mi"     # return 20 rooms nearby the centroid
CITY_ROOMS = 6000
ROOMS_LENGTH = 1000

query_city = {
            "query": {
              "bool": {
                "must": [],
                "must": [{"match_phrase":{"city": "Seattle"}}],
                "must_not": [{"match_phrase":{"room_type":{"query": ""}}}]
              }
            }
        }

res_city = es.search(body = query_city, size = CITY_ROOMS)

print("length response", len(res_city['hits']['hits']))
city_dataframe = Get_Raw_City_Data(res_city['hits']['hits'])

num_houses = len(city_dataframe)
print("Len database", num_houses)

scores, risk_scores, appr_scores, cost_scores, rent_scores, cap_scores = [],[],[],[],[],[]

for i in range(0, len(city_dataframe)):

    item = {'RoomType': city_dataframe.loc[city_dataframe.index.values[i]]['RoomType'], 'Beds': city_dataframe.loc[city_dataframe.index.values[i]]['Beds'], 'Baths':
    city_dataframe.loc[city_dataframe.index.values[i]]['Baths'], 'size': city_dataframe.loc[city_dataframe.index.values[i]]['Size'], 'Revenue': city_dataframe.loc[city_dataframe.index.values[i]]['Revenue'],
    'Cap': city_dataframe.loc[city_dataframe.index.values[i]]['Cap'], 'Appr': city_dataframe.loc[city_dataframe.index.values[i]]['Appr'], 'listing_price':city_dataframe.loc[city_dataframe.index.values[i]]['listing_price']}

    data2 = calcscore(res_city, item)
    print("Scoring properties...%d items left." % (num_houses - i))
    scores.append(data2['score'])
    risk_scores.append(data2['risk-score'])
    appr_scores.append(data2['appr-score'])
    cost_scores.append(data2['cost-score'])
    rent_scores.append(data2['rent-score'])
    cap_scores.append(data2['cap-score'])


city_dataframe['WeHome_Score'] = scores
city_dataframe['risk_Score'] = risk_scores
city_dataframe['appr_Score'] = appr_scores
city_dataframe['cost_Score'] = cost_scores
city_dataframe['rent_Score'] = rent_scores
city_dataframe['cap_Score'] = cap_scores

result = city_dataframe.sort_values('WeHome_Score', ascending = False)

# Get top 80 recommendations

recommendations = pd.DataFrame(columns=['Address', 'Url', 'WEEP_Score'])
recommendations['Address'] = result['Address'][:]
recommendations['Url'] = result['Url'][:]
recommendations['WEEP_Score'] = result['WeHome_Score'][:]

# up and up and up and up

for i in range(0, len(city_dataframe)):

    item = {

        "to_update": True,
        "Suggested_Rent": float(city_dataframe.loc[city_dataframe.index.values[i]]['WeHome_Rent']),
        "address": city_dataframe.loc[city_dataframe.index.values[i]]['Address'],
        "Beds": float(city_dataframe.loc[city_dataframe.index.values[i]]['Beds']),
        "Baths": float(city_dataframe.loc[city_dataframe.index.values[i]]['Baths']),
        "listing_price": float(city_dataframe.loc[city_dataframe.index.values[i]]['listing_price']),
        "RoomType": city_dataframe.loc[city_dataframe.index.values[i]]['RoomType'],
        "size": float(city_dataframe.loc[city_dataframe.index.values[i]]['Size']),
        'Score': float(city_dataframe.loc[city_dataframe.index.values[i]]['WeHome_Score']),
        'cost_score': float(city_dataframe.loc[city_dataframe.index.values[i]]['cost_Score']),
        'rent_score': float(city_dataframe.loc[city_dataframe.index.values[i]]['rent_Score']),
        'risk_score': float(city_dataframe.loc[city_dataframe.index.values[i]]['risk_Score']),
        'cap_score': float(city_dataframe.loc[city_dataframe.index.values[i]]['cap_Score']),
        'appr_score': float(city_dataframe.loc[city_dataframe.index.values[i]]['appr_Score']),
        'Appr': float(city_dataframe.loc[city_dataframe.index.values[i]]['Appr']),
        'Ratio': float(city_dataframe.loc[city_dataframe.index.values[i]]['Ratio']),
        'Revenue': float(city_dataframe.loc[city_dataframe.index.values[i]]['Revenue']),
        "centroid_point": city_dataframe.loc[city_dataframe.index.values[i]]['location_point'],
        "centroid_map": city_dataframe.loc[city_dataframe.index.values[i]]['location'],
        'centroid': city_dataframe.loc[city_dataframe.index.values[i]]['location'],
        "home_id": city_dataframe.loc[city_dataframe.index.values[i]]['home_id'],
        "source_name": "zillow",
        "Appr_des": city_dataframe.loc[city_dataframe.index.values[i]]['Appr'],
        "yearbuilt": float(city_dataframe.loc[city_dataframe.index.values[i]]['YearBuilt']),
        "city": city_dataframe.loc[city_dataframe.index.values[i]]['City'],
        "lot_size": None,
        "state": city_dataframe.loc[city_dataframe.index.values[i]]['State'],
        "zipcode": city_dataframe.loc[city_dataframe.index.values[i]]['Zipcode'],
        "pict_urls": city_dataframe.loc[city_dataframe.index.values[i]]['pict_url'],
        "Irr": float(city_dataframe.loc[city_dataframe.index.values[i]]['Irr']),
        "Cap": float(city_dataframe.loc[city_dataframe.index.values[i]]['Cap']),
        "area.id": str(city_dataframe.loc[city_dataframe.index.values[i]]['area.id']),
        "neighborhood.id": str(city_dataframe.loc[city_dataframe.index.values[i]]['neighborhood.id']),
        "neighborhood.name": str(city_dataframe.loc[city_dataframe.index.values[i]]['neighborhood.name'])

    }


    if item.get('to_update'):

        print("Start inserting..")
        upsert_to_es(item, es = es)
        print('%dth item has been successfully updated' % (i))

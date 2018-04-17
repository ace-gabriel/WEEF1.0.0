import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
from rent import *
from appr import *
import geopy.distance as dist
from irr import *
from settings import HOME_INDEX,HOME_TYPE

NEARBY_RANGE_KM = "2mi"
NEARBY_RANGE_SCORE = "3mi"
ROOMS_LENGTH = 1000
CITY_ROOMS = 6000

def Get_Raw_City_Data(db):
    # all houses with rent data
    # select all the rent_status houses

    es = Elasticsearch(['http://localhost:59890'])

    rents, apprs, irrs, drop_index, area_ids, n_ids = [], [], [], [], [], []

    array = list(i for i in range(len(db)) if isinstance(db[i]['_source']['rent'], float) and not pd.isnull(
        db[i]['_source']['beds']) and not pd.isnull(db[i]['_source']['baths']) and not pd.isnull(
        db[i]['_source']['year_built']) and db[i]['_source']['year_built'] != 'null' and db[i]['_source']['size'] > 0
        and db[i]['_source']['year_built'] > 1990 and not pd.isnull(db[i]['_source']['house_price_dollar']) and db[i]['_source']['house_price_dollar'] > 100000
        and isinstance(db[i]['_source']['increase_ratio'], float) and db[i]['_source']['increase_ratio'] != 'NaN')

    for i in array:
        db[i]['_source']['url'] = 'https://www.zillow.com/homedetails/' + str(db[i]['_source']['source_id']) + '_zpid/'

    dbs = pd.DataFrame(columns=['Address', 'YearBuilt', 'Rent', 'Beds', 'Baths', 'Size', 'RoomType', 'location', 'Irr', 'area.id', 'neighborhood.id', 'neighborhood.name'])
    print("Length of the database: ", len(array))
    dbs['YearBuilt'] = list(db[i]['_source']['year_built'] for i in array)
    dbs['location'] = list(str(db[i]['_source']['location']['coordinates'][1]) + ', ' + str(db[i]['_source']['location']['coordinates'][0]) for i in array)
    dbs['Size_Price'] = dbs['Rent'] / dbs['Size']
    dbs['Appr'] = list(db[i]['_source']['increase_ratio'] for i in array)
    dbs['City'] = list(db[i]['_source']['city'] for i in array)
    dbs['State'] = list(db[i]['_source']['state'] for i in array)
    dbs['Zipcode'] = list(db[i]['_source']['zipcode'] for i in array)
    dbs['pict_url'] = list(db[i]['_source']['pict_urls'] for i in array)
    dbs['Beds'] = list(db[i]['_source']['beds'] for i in array)
    dbs['Baths'] = list(db[i]['_source']['baths'] for i in array)
    dbs['Size'] = list(db[i]['_source']['size'] for i in array)
    dbs['RoomType'] = list(db[i]['_source']['room_type'] for i in array)
    dbs['listing_price'] = list(db[i]['_source']['house_price_dollar'] for i in array)
    dbs['Address'] = list(db[i]['_source']['addr'] for i in array)
    dbs['Appr'] *= 100
    dbs['Revenue'] = dbs['Rent'] * 12 - dbs['listing_price'] * 0.035
    dbs['Ratio'] = 100 * ((dbs['Rent'] * 12) / dbs['listing_price'])
    dbs['Cap'] = dbs['Appr'] + dbs['Ratio'] - 3.5
    dbs['Url'] = list(db[i]['_source']['url'] for i in array)
    dbs['Status'] = list(db[i]['_source']['status'] for i in array)
    dbs['location_point'] = list(db[i]['_source']['location_point'] for i in array)
    dbs['home_id'] = list(db[i]['_source']['source_id'] for i in array)
    dbs['neighborhood.name'] = list(db[i]['_source']['neighborhood']['name'] for i in array)


    num_houses = len(dbs)

    for i in range(0, len(dbs)):
        item = {'RoomType': dbs.loc[dbs.index.values[i]]['RoomType'], 'Beds': dbs.loc[dbs.index.values[i]]['Beds'], 'Baths':
            dbs.loc[dbs.index.values[i]]['Baths'], 'size': dbs.loc[dbs.index.values[i]]['Size']}

        q = {
              "query": {
                "bool": {
                  "must": [],
                  "filter": {
                      "geo_distance" : {
                                "distance" : NEARBY_RANGE_SCORE,
                                "location_point" : dbs.loc[dbs.index.values[i]]['location'],
                            }
                  },
                  #"must": [{"match_phrase":{"status": 2}}],
                  "must_not": [{"match_phrase":{"room_type":{"query": ""}}}]
                }
              },
              "sort":[
                {
                  "_geo_distance" : {
                    "location_point" : dbs.loc[dbs.index.values[i]]['location'],
                    "order": "asc",
                    "unit": "mi"
                  }
                }
              ]
            }

        r = es.search(
                      body=q,
                      size=CITY_ROOMS,
                      )

        print("Evaluating properties...%d items left." % (num_houses - i))
        print("Rent", calcrent(r, item))
        print("Apprs", calcappr(r, item))


        for i in range(0, 5):
            if float(r['hits']['hits'][i]['_source']['area']['id']) != 0:
                area_id = r['hits']['hits'][i]['_source']['area']['id']
            if float(r['hits']['hits'][i]['_source']['neighborhood']['id']) != 0:
                neighbor_id = r['hits']['hits'][i]['_source']['neighborhood']['id']

        area_ids.append(area_id)
        n_ids.append(neighbor_id)
        rents.append(calcrent(r, item))
        apprs.append(calcappr(r, item))


    for i in range(0, len(rents)):
        if not isinstance(rents[i], float) or pd.isnull(rents[i]) or not isinstance(apprs[i], float):
            drop_index.append(i)

    dbs['area.id'] = area_ids
    dbs['neighborhood.id'] = n_ids
    dbs['WeHome_Rent'] = rents
    dbs['Appr'] = apprs
    dbs = dbs.drop(drop_index)

    dbs['Revenue'] = dbs['WeHome_Rent'] * 12 - dbs['listing_price'] * 0.035
    dbs['Ratio'] = 100 * ((dbs['WeHome_Rent'] * 12) / dbs['listing_price'])
    dbs['Cap'] = dbs['Appr'] + dbs['Ratio'] - 3.5

    for i in range(0, len(dbs)):

        print(dbs.loc[dbs.index.values[i]]['listing_price'], dbs.loc[dbs.index.values[i]]['WeHome_Rent'])

        print('Irr:', cashflow(dbs.loc[dbs.index.values[i]]['listing_price'], dbs.loc[dbs.index.values[i]]['WeHome_Rent']))

        irrs.append(cashflow(dbs.loc[dbs.index.values[i]]['listing_price'], dbs.loc[dbs.index.values[i]]['WeHome_Rent']))


    dbs['Irr'] = irrs


    return dbs



def Get_Nearby_Properties(point, frame):

    buffer = frame

    dis = []

    for i in range(0, len(buffer)):
        pt1 = buffer.loc[buffer.index.values[i]]['location']
        distance = dist.vincenty(pt1, point).miles
        if distance > 0 and distance <= 3.0:
            dis.append(distance)
        else:
            dis.append('N/A')

    buffer['distance'] = dis
    drop_index = list(i for i in range(0, len(dis)) if not isinstance(dis[i], float))
    buffer = buffer.drop(drop_index)
    result = buffer.sort_values('distance', ascending = "False")

    return result


def upsert_to_es(item, es):
    # preprocess
    item['centroid_point'] = item['centroid']
    item['centroid_map'] = {
        "type": "point",
        "coordinates": list(map(lambda x: float(x), item['centroid'].split(',')[::-1]))
    }
    item['Appr_des'] = round(float(item['Appr']) / 100, 6)

    # define mapping
    mapping = {
        "Suggested_Rent": "rent",
        "address": "addr",
        "Beds": "beds",
        "Baths": "baths",
        "listing_price": "house_price_dollar",
        "RoomType": "room_type",
        "size": "size",
        "Score": "score",
        "centroid_point": "location_point",
        "centroid_map": "location",
        "home_id": "source_id",
        "source_name": "source_name",
        "Appr_des": "increase_ratio",
        "yearbuilt": "year_built",
        "city": "city",
        "lot_size": "lot_size",
        "state": "state",
        "zipcode": "zipcode",
        "pict_urls": "pict_urls"
    }

    # exchange item
    item_new = {}
    for k, v in mapping.items():
        item_new[v] = item[k]

    # more field + financial data
    item_new['sale_rent_ratio'] = round(1 / float(item['Ratio']), 6)
    item_new['area'] = {"id": item['area.id']}
    item_new['neighborhood'] = {"id": item['neighborhood.id'], "name": item['neighborhood.name']}
    item_new['score_radar'] = {"score_appreciation": 60,
                               "score_cost": 60,
                               "score_rental": 60,
                               "score_anti_risk": 60,
                               "score_airbnb": 60
                               }

    item_new['score'] = item['Score']
    item_new['score_radar']['score_appreciation'] = item['appr_score'] * 5
    item_new['score_radar']['score_cost'] = item['cost_score'] * 5
    item_new['score_radar']['score_rental'] = item['rent_score'] * 5
    item_new['score_radar']['score_anti_risk'] = item['risk_score'] * 5
    item_new['score_radar']['score_airbnb'] = item['cap_score'] * 5  # TODO

    item_new['airbnb_rent'] = None

    item_new['rental_return_rate_net'] = (item['Revenue'] / item['listing_price']) * 100
    item_new['rental_return_annual'] = item['Revenue']

    item_new['insurance'] = item['Suggested_Rent'] * 0.056
    item_new['maintainance'] = item['Suggested_Rent'] * 0.04
    item_new['tax'] = item['listing_price'] * 0.01025
    item_new['pm_long'] = item['Suggested_Rent'] * 0.08
    item_new['pm_short'] = 0
    item_new['irr'] = item['Irr']
    item_new['cap'] = item['Cap']

    # upsert to elasticsearch

    print(str(item_new['source_id']) + '_' + item_new['source_name'], item_new['addr'])
    try:

        print(item_new)
        es.update(index=HOME_INDEX,
                  doc_type=HOME_TYPE,
                  id=str(item_new['source_id']) + '_' + item_new['source_name'],
                  body={'doc': item_new,
                        'doc_as_upsert': True}
                  )
    except Exception as e:
        print("error when update zillow id:{},{}".format(item_new['source_id'], e))

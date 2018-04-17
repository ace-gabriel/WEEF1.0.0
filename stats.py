import numpy as np
import pandas as pd



EXCELLENT_SCORE = 20
GOOD_SCORE = 18
MEDIUM_SCORE = 16
LOW_SCORE = 14
BAD_SCORE = 12

def GetData(db):
    # all houses with rent data
    # select all the rent_status houses


    array = list(i for i in range(len(db)) if isinstance(db[i]['_source']['increase_ratio'], float) and not pd.isnull(db[i]['_source']['increase_ratio']) and db[i]['_source']['size'] > 0
        and db[i]['_source']['status'] == 2 and db[i]['_source']['increase_ratio'] != 'NaN' and not pd.isnull(
            db[i]['_source']['beds']) and not pd.isnull(db[i]['_source']['baths']) and db[i]['_source']['size'] > 0 and isinstance(db[i]['_source']['rent'], float)
            and not pd.isnull(db[i]['_source']['house_price_dollar']))

    for i in array:
        db[i]['_source']['url'] = 'https://www.zillow.com/homedetails/' + str(db[i]['_source']['source_id']) + '_zpid/'

    dbs = pd.DataFrame(columns=['Address', 'RoomType', 'Appr', 'Rent', 'listing_price', 'Beds', 'Baths', 'Size', 'Revenue', 'Ratio', 'Cap', 'Score', 'url'])

    dbs['Appr'] = list(db[i]['_source']['increase_ratio'] for i in array)
    dbs['Beds'] = list(db[i]['_source']['beds'] for i in array)
    dbs['Baths'] = list(db[i]['_source']['baths'] for i in array)
    dbs['Size'] = list(db[i]['_source']['size'] for i in array)
    dbs['RoomType'] = list(db[i]['_source']['room_type'] for i in array)
    dbs['listing_price'] = list(db[i]['_source']['house_price_dollar'] for i in array)
    dbs['Rent'] = list(db[i]['_source']['rent'] for i in array)
    dbs['Address'] = list(db[i]['_source']['addr'] for i in array)
    dbs['Appr'] *= 100
    dbs['Revenue'] = dbs['Rent'] * 12 - dbs['listing_price'] * 0.035
    dbs['Ratio'] = 100 * ((dbs['Rent'] * 12) / dbs['listing_price'])
    dbs['Cap'] = dbs['Appr'] + dbs['Ratio'] - 3.5
    dbs['url'] = list(db[i]['_source']['url'] for i in array)
    dbs['status'] = list(db[i]['_source']['status'] for i in array)

    return dbs

def GetTargets(indexes, frames, item):

    arr = []

    # within 5 miles..first get all the houses with same roomtype
    for i in indexes:

        if (frames.loc[i]['RoomType'] == item['RoomType']) or (frames.loc[i]['Beds'] == item['Beds'] and int(item['Baths']) == int(frames.loc[i]['Baths'])):
            arr.append(i)

    return pd.DataFrame(list(frames.loc[i] for i in arr))


def Calc_Stats_Finance(frame):

    result = {'Revenue' : {}, 'listing_price' : {}, 'Appr' : {}, 'Ratio' : {}, 'Cap' : {}}

    percentile = ['90', '75', '50', '25']

    for item in percentile:

        result['Revenue'][item] = np.percentile(frame['Revenue'], int(item))
        result['listing_price'][item] = int(np.percentile(frame['listing_price'], int(item)))
        result['Appr'][item] = float(np.percentile(frame['Appr'], int(item)))
        result['Ratio'][item] = float(np.percentile(frame['Ratio'], int(item)))
        result['Cap'][item] = float(np.percentile(frame['Cap'], int(item)))


    return result


def GetScore(stats, data, name):

    if float(data) < stats[name]['25']:
        return BAD_SCORE
    elif float(data) < stats[name]['50'] and float(data) >= stats[name]['25']:
        return LOW_SCORE
    elif float(data) < stats[name]['75'] and float(data) >= stats[name]['50']:
        return MEDIUM_SCORE
    elif float(data) < stats[name]['90'] and float(data) >= stats[name]['75']:
        return GOOD_SCORE
    else:
        return EXCELLENT_SCORE

def calcscore(res_score, item):

    db = GetData(res_score['hits']['hits'])

    target = GetTargets(db.index.values, db, item)
    stats = Calc_Stats_Finance(db)

    def score(stats = stats, fact1 = 'listing_price', fact2 = 'Appr', fact3 = 'Revenue', fact4 = 'Cap', item = item):

        cost_score = 32 - GetScore(stats, item[fact1], fact1)
        appreciation_score = GetScore(stats, item[fact2], fact2)
        cap_score = GetScore(stats, item[fact4], fact4)

        if appreciation_score == EXCELLENT_SCORE:
            risk_score = BAD_SCORE
        elif appreciation_score == GOOD_SCORE:
            risk_score = LOW_SCORE
        elif appreciation_score == MEDIUM_SCORE:
            risk_score = MEDIUM_SCORE
        elif appreciation_score == LOW_SCORE:
            risk_score = GOOD_SCORE
        else:
            risk_score = EXCELLENT_SCORE

        rent_score = GetScore(stats, item[fact3], fact3)
        risk_score = 32 - appreciation_score
        total_score = cost_score + appreciation_score + risk_score + rent_score + cap_score
        #print(total_score, cost_score, appreciation_score, risk_score, rent_score, cap_score)
        #print(np.median(frame_cost_score), np.median(frame_appr_score), np.median(frame_risk_score), np.median(frame_rent_score), np.median(frame_cap_score))

        return {'item-total-score': total_score, 'cost-score': cost_score, 'appreciation-score': appreciation_score,
                 'risk-score': risk_score, 'rent-score': rent_score, 'cap-score': cap_score }

    data = score()
    result = {'score': data['item-total-score'], 'cost-score': data['cost-score'],
    'appr-score': data['appreciation-score'],'risk-score': data['risk-score'], 'rent-score': data['rent-score'],'cap-score': data['cap-score']}

    return result

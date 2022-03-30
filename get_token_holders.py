import grequests
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import pandas as pd
from datetime import datetime
import sys

def progress(count,total):
    sys.stdout.write(f'progress: {count} of {total}\r')
    sys.stdout.flush()
    
def get_box_amounts(items,token_id):
    boxes = []
    for box in items:
        for asset in box['assets']:
            if asset['tokenId'] == token_id:
                amount = asset['amount']
        boxes.append({
                'boxId' : box['boxId'],
                'address' : box['address'],
                'amount' : amount})
    return boxes

def get_holders(token_id):
    offset = 0
    r = requests.get(url=f'https://api.ergoplatform.com/api/v1/boxes/unspent/byTokenId/{token_id}?limit=1')
    total = r.json()['total']
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504, 520, 525], raise_on_redirect=True,
                    raise_on_status=True)
    s.mount('http://', HTTPAdapter(max_retries=retries))
    s.mount('https://', HTTPAdapter(max_retries=retries))
    boxes = []
    responses = []
    while offset <= total:
        progress(offset,total)
        urls = []
        while offset <= total and len(urls) < 20:
            urls.append(f'https://api.ergoplatform.com/api/v1/boxes/unspent/byTokenId/{token_id}?limit=100&offset={offset}')
            offset += 100
        rs = (grequests.get(u, session=s) for u in urls)
        rl = grequests.map(rs)
        responses = responses + rl
    for r in responses:
        if r.status_code != 200:
            print(r.status_code)
        data = r.json()
        boxes= boxes + get_box_amounts(data['items'],token_id)
    df = pd.DataFrame(list(boxes)).drop_duplicates()
    df2 = df.groupby('address').sum()
    df2['percentage'] = round((df2['amount'] / df.sum()['amount']),6)
    progress(total,total)
    return df2

tokens = {
    '🍆💦':'185e217d80d797800bfa699afda708ee101ae664f8ea237d9fc3a3824b7c3ecb',
    'MiGoreng':'0779ec04f2fae64e87418a1ad917639d4668f78484f45df962b0dec14a2591d2',
    'Ergold':'e91cbc48016eb390f8f872aa2962772863e2e840708517d1ab85e57451f91bed',
    'COMET':'0cd8c9f416e5b1ca9f986a7f10a84191dfb85941619e49e53c0dc30ebf83324b'
    }

for token in tokens:
    token_id = tokens[token]
    start = datetime.now()
    df = get_holders(token_id)
    df.sort_values(by=['amount'],ascending=False).to_csv(f'data/{token}.csv',index=True)
    print('complete')
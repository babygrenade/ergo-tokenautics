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

def get_transaction_ids(items):
    transaction_ids = []
    for item in items:
        transaction_ids.append(item['transactionId'])
    return transaction_ids

# boxes.append moved under if condition... test with main function but should work
def get_box_amounts(items,token_id):
    boxes = []
    for box in items:
        for asset in box['assets']:
            if asset['tokenId'] == token_id:
                boxes.append({
                        'boxId' : box['boxId'],
                        'address' : box['address'],
                        'amount' : asset['amount']})
    return boxes

# gets a list of transaction ids for a token id
def get_transaction_list(token_id):
    offset = 0
    r = requests.get(url=f'https://api.ergoplatform.com/api/v1/boxes/byTokenId/{token_id}?limit=1')
    total = r.json()['total']

    retries = Retry(total=5, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504, 520, 525], raise_on_redirect=True,
                    raise_on_status=True)

    transaction_ids = []
    responses = []
    while offset <= total:
        progress(offset,total)
        urls = []
        while offset <= total and len(urls) < 5:
            urls.append(f'https://api.ergoplatform.com/api/v1/boxes/byTokenId/{token_id}?limit=100&offset={offset}')
            offset += 100
        with requests.Session() as s:
            s.mount('http://', HTTPAdapter(max_retries=retries))
            s.mount('https://', HTTPAdapter(max_retries=retries))
            rs = (grequests.get(u, session=s) for u in urls)
            rl = grequests.map(rs)
        responses = responses + rl
    for r in responses:
        if r.status_code != 200:
            print(r.status_code)
        data = r.json()
        total = data['total']
        transaction_ids = transaction_ids + get_transaction_ids(data['items'])
    progress(total,total)
    return list(set(transaction_ids))

def map_transaction_edges(data,token_id):
    outputs = get_box_amounts(data['outputs'],token_id)
    inputs = get_box_amounts(data['inputs'],token_id)
    height = data['inclusionHeight']

    # Handle token creation
    if len(inputs) == 0:
        df_out = pd.DataFrame(outputs).groupby('address').sum().reset_index()
        df = df_out[['address','amount']].rename(columns={'address':'target','amount':'weight'})
        df['source'] = 'token-creation'
        df['height'] = height
        return df
    #Handle all token inputs burned
    if len(outputs) == 0:
        df_in = pd.DataFrame(inputs).groupby('address').sum().reset_index()
        df = df_in[['address','amount']].rename(columns={'address':'source','amount':'weight'})
        df['target'] = 'burned'
        df['height'] = height
        return df

    df_out = pd.DataFrame(outputs).groupby('address').sum().reset_index()
    df_in = pd.DataFrame(inputs).groupby('address').sum().reset_index()


    #reduce by amount in input for addresses in input and output boxes
    df = df_out.merge(df_in, how='left', on='address', suffixes=('_out','_in'))
    df['weight'] = df['amount_out'] - df['amount_in'].fillna(0)

    df = df_in.merge(df, how='cross', suffixes=('_in','_out')).rename(columns={'address_in': 'source', 'address_out': 'target'})

    #filter out returned boxes
    df2 = df[df['source'] != df['target']].copy()

    #Handle partial burned
    total_out = df_out.sum()['amount']
    total_in = df_in.sum()['amount']
    if total_out < total_in:
        burned = df[df['amount_out'] < df['amount_in']].copy()
        burned['weight'] = total_in - total_out
        burned['target'] = 'burned'
        df2 = pd.concat([df2,burned])

    df = df2[['source','target','weight']].copy()
    df['height'] = height
    return df
    

def map_edges(transaction_ids,token_id):
    total = len(transaction_ids)
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504, 520, 525], raise_on_redirect=True,
                    raise_on_status=True)
    s.mount('http://', HTTPAdapter(max_retries=retries))
    s.mount('https://', HTTPAdapter(max_retries=retries))
    transactions = []
    responses = []
    urls = []
    for transaction_id in transaction_ids:
        urls.append(f'https://api.ergoplatform.com/api/v1/transactions/{transaction_id}')

    for i in range(0,total,20):
        progress(i,total)
        rs = (grequests.get(u, session=s) for u in urls[i:i+20])
        rl = grequests.map(rs)
        responses = responses + rl

    for r in responses:
        if r.status_code != 200:
            print(r.status_code)
        data = r.json()
        try:
            transactions.append(map_transaction_edges(data,token_id))
        except:
            print(data['id'])
    df = pd.concat(transactions)
    df.reset_index(inplace=True,drop=True)    
    progress(total,total)
    return df 

def map_nodes(df):
    addresses = list(set(pd.concat([df['source'],df['target']]).to_list()))
    #edges = json.loads(df.to_json(orient="records"))
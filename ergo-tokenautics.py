import sys
import argparse
import os
from dotenv import load_dotenv
import warnings

import psycopg2 as pg
from datetime import datetime
import pandas as pd

import grequests
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# load environment variables from .env
load_dotenv()
EXPLORER_API_BASE = os.environ.get('EXPLORER_API_BASE')
if EXPLORER_API_BASE[-1:] != '/':
    EXPLORER_API_BASE += '/'
EXPLORER_DB_HOST = os.environ.get('EXPLORER_DB_HOST')
EXPLORER_DB_USER = os.environ.get('EXPLORER_DB_USER')
EXPLORER_DB_PW = os.environ.get('EXPLORER_DB_PW')

# combines spectrum list with token_list.csv
def update_tokens():
    try:
        current_tokens = pd.read_csv('token_list.csv',names = ['ticker','address','decimals'])
        spectrum_url = 'https://raw.githubusercontent.com/spectrum-finance/ergo-token-list/main/src/tokens.json'
        r = requests.get(spectrum_url)
        tokens = []
        tok_dict = r.json()
        for key in tok_dict:
            value = tok_dict[key]
            value['address'] = key
            tokens.append(value)

        s_tokens = pd.DataFrame(tokens)
        s_tokens = s_tokens[['ticker','address','decimals']]
        u_df = pd.concat([current_tokens,s_tokens], ignore_index=True).drop_duplicates()
        u_df.to_csv('token_list.csv',index=False,header=False,encoding='utf-8')
        return True
    except:
        return False

# Connects to explorer db using psycopg2 and executes sql query from explorerquery.sql
def connect_db():
    conn = pg.connect(dbname="explorer", user=EXPLORER_DB_USER, password=EXPLORER_DB_PW,host=EXPLORER_DB_HOST)
    with open('token_list.csv','r',encoding='utf-8') as f:
        token_file=f.readlines()
    with open('sql/token_holders.sql','r') as f:
        query_template = f.read()

    token_parse = lambda x : x.replace('\n','').split(',')[0:2]

    tokens = dict(map(token_parse,token_file))
    warnings.filterwarnings('ignore')
    for token in tokens:
        token_id = tokens[token]
        start = datetime.now()
        query = query_template % token_id
        try:
            df = pd.read_sql(query,conn)
            conn.commit()
        except Exception as e:
            print(f'{token} skipped')
            continue
        df['amount'] = df['amount'].astype('int64')
        df['percentage'] = round((df['amount'] / df.sum()['amount']),6)
        df.sort_values(by=['amount'],ascending=False).to_csv(f'data/{token}.csv',index=False)
        print(f'{token} complete')
    
    return True

# Helper functiosn for connect_api()

# Progress bar for looping through requests (not necessary for )
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
    r = requests.get(url=f'https://{EXPLORER_API_BASE}api/v1/boxes/unspent/byTokenId/{token_id}?limit=1')
    total = r.json()['total']

    retries = Retry(total=5, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504, 520, 525], raise_on_redirect=True,
                    raise_on_status=True)

    boxes = []
    responses = []
    while offset <= total:
        progress(offset,total)
        urls = []
        while offset <= total and len(urls) < 5:
            url = f'https://{EXPLORER_API_BASE}api/v1/boxes/unspent/byTokenId/{token_id}?limit=100&offset={offset}'
            urls.append(url)
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
        boxes= boxes + get_box_amounts(data['items'],token_id)
    df = pd.DataFrame(list(boxes)).drop_duplicates()
    df2 = df.groupby('address').sum()
    df2['percentage'] = round((df2['amount'] / df.sum()['amount']),6)
    progress(total,total)
    return df2

# main function to connect to explorer api 
def connect_api():
    with open('token_list.csv','r',encoding='utf-8') as f:
        token_file=f.readlines()
    token_parse = lambda x : x.replace('\n','').split(',')[0:2]
    tokens = dict(map(token_parse,token_file))
    for token in tokens:
        token_id = tokens[token]
        start = datetime.now()
        try:
            df = get_holders(token_id)
        except Exception as e:
            print(f'{token} skipped')
            print(e)
            continue
        df.sort_values(by=['amount'],ascending=False).to_csv(f'data/{token}.csv',index=True)
        print('complete')
    return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-db","--database",help="Whether to attempt a database connection to explorer.  Defaults to Y if blank")
    parser.add_argument("-u","--update",help="Update token_list.csv with latest from Spectrum finance. Defaults to Y if blank",action="store_true")
    args =  parser.parse_args()

    # flag to attempt db connection or skip and go directly to api connection
    connect_db_flag = args.database or "Y"
    connect_db_flag = connect_db_flag.upper()


    if connect_db_flag not in ('Y','N'):
        print('-db flag must be Y or N')
        return False

    # update token_list
    if args.update:
        update_tokens()
        print("token_list.csv updated")

    complete = False
    try:
        if connect_db_flag == 'Y':
            complete = connect_db()
        else:
            print('DB connection bypassed')
    except Exception as e:
        print('Database connection failed')
        print(e)

    try:
        if not complete:
            complete = connect_api()
    except Exception as e:
        print(e)




if __name__ == "__main__":
    main()
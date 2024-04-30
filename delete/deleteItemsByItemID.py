import argparse
import secret
import time
import pandas as pd
from authenticateToDSpace import authenticate_to_dspace

startTime = time.time()

secretsVersion = input('To edit production server, enter secrets file: ')
if secretsVersion != '':
    try:
        secret = __import__(secretsVersion)
        print('Editing Production')
    except ImportError:
        print('Editing Stage')
else:
    print('Editing Stage')

base_url = secret.base_url
user = secret.user
password = secret.password

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
args = parser.parse_args()

if args.file:
    filename = args.file
else:
    filename = input('Enter filename (including \'.csv\'): ')

# Authenticate to DSpace site, get token
session = authenticate_to_dspace(base_url, user, password)


df = pd.read_csv(filename)
total_items = len(df)

all_items = []
for count, row in df.iterrows():
    print('{} of {} items.'.format(str(count+1), str(total_items)))
    row = row.copy()
    item_id = row['item_uuid']
    item_url = base_url+'/core/items/'+item_id
    delete_response = session.delete(item_url)
    print(delete_response)


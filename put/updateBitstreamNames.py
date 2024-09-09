import pandas as pd
import argparse
import secret
import time
from datetime import datetime
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

bitstream_endpoint = '/core/bitstreams'

if args.file:
    filename = args.file
else:
    filename = input('Enter filename (including \'.csv\'): ')

df = pd.read_csv(filename)

s = authenticate_to_dspace(base_url, user, password)
headers = s.headers


all_items = []
for count, row in df.iterrows():
    count = count + 1
    row = row.copy()
    dt = datetime.now().strftime('%Y-%m-%d')
    bit_uuid = row['bitstream_uuid']
    print(count, bit_uuid)
    old_value = row['old_filename']
    new_value = row['new_filename']
    keys = ['dc.title', 'dc.source']
    bitstream_url = base_url+bitstream_endpoint+'/'+bit_uuid
    get_bitstream = s.get(bitstream_url).json()
    get_name = get_bitstream.get('name')
    if get_name != old_value:
        print('Updating bitstream')
        metadata = get_bitstream['metadata']
        patches = [{'op': 'add',
                    'path': '/metadata/dc.title.alternative',
                    'value': [{'value': new_value}]}
                   ]
        for key_count, key in enumerate(keys):
            full_key = '/metadata/'+key
            patch = {'op': 'replace', 'path': full_key, 'value': [{'value': new_value}]}
            patches.append(patch)
            prov_note = '{}: {} was replaced by {}: {} through an ' \
                        'API batch process on {}.'.format(key, old_value, key, new_value, dt)
            prov_patch = {'op': 'add', 'path': '/metadata/dc.description.provenance/'+str(key_count),
                          'value': prov_note}
            patches.append(prov_patch)
        print(patches)
        updated = s.patch(bitstream_url, json=patches, headers=headers).json()

        print(updated)
        updated_name = updated.get('name')
        row['update_name'] = updated_name
    elif get_name == new_value:
        error_message = 'Filename already updated. No patch.'
        print(error_message)
        row['error'] = error_message
    else:
        error_message = 'Filename does not match old_value. No patch.'
        print(error_message)
        row['get_name'] = get_name
        row['error'] = error_message
    all_items.append(row)


dt = datetime.now().strftime('%Y-%m-%d')
log = pd.DataFrame.from_records(all_items)
log.to_csv('logOfFilenameChanges_'+dt+'.csv', index=False)

elapsedTime = time.time() - startTime
m, s = divmod(elapsedTime, 60)
h, m = divmod(m, 60)
print('Total script run time: ', '%d:%02d:%02d' % (h, m, s))

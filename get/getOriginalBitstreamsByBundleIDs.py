import requests
import pandas as pd


headers = {'content-type': 'application/json'}
baseURL = 'https://jscholarship.library.jhu.edu'

df = pd.read_csv('originalBundlesByItemIDs.csv')

all_items = []
for count, row in df.iterrows():
    row = row
    bundle_uuid = row['bundle_uuid']
    print(count, bundle_uuid)
    more_links = True
    next_list = []
    while more_links:
        if not next_list:
            full_bitstream_link = baseURL+'/server/api/core/bundles/'+bundle_uuid+'/bitstreams'
            print(full_bitstream_link)
            try:
                r = requests.get(full_bitstream_link, headers=headers, timeout=5).json()
            except requests.exceptions.Timeout:
                print('error!')
                continue
        else:
            next_link = next_list[0]
            print(next_link)
            r = requests.get(next_link, headers=headers, timeout=5).json()
        bitstreams = r['_embedded']['bitstreams']
        for bitstream in bitstreams:
            bitstream_uuid = bitstream['uuid']
            bitstream_name = bitstream['name']
            row['bitstream_uuid'] = bitstream_uuid
            row['bitstream_name'] = bitstream_name
            all_items.append(row)
        next_list.clear()
        search_results = r['_links']
        next_page = search_results.get('next')
        if next_page:
            next_link = next_page.get('href')
            next_list.append(next_link)
        else:
            break


all_items = pd.DataFrame.from_dict(all_items)
all_items.to_csv('originalBitstreamsByBundleIDs.csv', index=False)
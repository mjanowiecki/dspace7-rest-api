import requests
import pandas as pd

headers = {'content-type': 'application/json'}
baseURL = 'https://jscholarship.library.jhu.edu'


df = pd.read_csv('testing.csv')

all_items = []
for count, row in df.iterrows():
    row = row
    item_uuid = row['item_uuid']
    print(count, item_uuid)
    more_links = True
    next_list = []
    while more_links:
        if not next_list:
            full_bundle_link = baseURL+'/server/api/core/items/'+item_uuid+'/bundles'
            r = requests.get(full_bundle_link, headers=headers, timeout=5).json()
        else:
            next_link = next_list[0]
            r = requests.get(next_link, headers=headers, timeout=5).json()
        bundles = r['_embedded']['bundles']
        for bundle_count, bundle in enumerate(bundles):
            bundle_uuid = bundle['uuid']
            bundle_name = bundle['name']
            if bundle_name == 'ORIGINAL':
                bundle_uuid = bundle['uuid']
                row['bundle_name'] = bundle_name
                row['bundle_uuid'] = bundle_uuid
                all_items.append(row)
            else:
                pass
        next_list.clear()
        search_results = r['_links']
        next_page = search_results.get('next')
        if next_page:
            next_link = next_page.get('href')
            next_list.append(next_link)
        else:
            break

all_items = pd.DataFrame.from_dict(all_items)
all_items.to_csv('originalBundlesByItemIDs.csv', index=False)
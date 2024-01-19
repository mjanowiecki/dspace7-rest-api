import requests
import pandas as pd

header = {'content-type': 'application/json'}
baseURL = 'https://j10p-stage.library.jhu.edu'
endpoint = '/server/api/core/collections'
pagination = '?size=100'


all_items = []
more_links = True
next_list = []
while more_links:
    if not next_list:
        full_link = baseURL+endpoint+pagination
        r = requests.get(full_link, headers=header).json()
    else:
        next_link = next_list[0]
        r = requests.get(next_link).json()
    collections = r['_embedded']['collections']
    for collection in collections:
        coll_id = collection['id']
        uuid = collection['uuid']
        name = collection['name']
        handle = collection['handle']
        coll_dict = {'id': coll_id, 'uuid': uuid,
                     'name': name, 'handle': handle}
        all_items.append(coll_dict)
    results = r['page']
    print(results)
    totalElements = results['totalElements']
    print(totalElements)
    next_list.clear()
    links = r.get('_links')
    nextDict = links.get('next')
    if nextDict:
        next_link = nextDict.get('href')
        next_list.append(next_link)
    else:
        break

all_items = pd.DataFrame.from_dict(all_items)
all_items.to_csv('allCollectionItems.csv', index=False)
import requests
import pandas as pd

header = {'content-type': 'application/json'}
baseURL = 'https://j10p-stage.library.jhu.edu'
endpoint = '/server/api/core/communities'
pagination = '?size=100'


all_items = []
more_links = True
next_list = []
while more_links:
    if not next_list:
        full_link = baseURL+endpoint+pagination
        r = requests.get(full_link, headers=header).json()
        results = r['page']
        totalElements = results['totalElements']
        print('Total elements: {}.'.format(totalElements))
    else:
        next_link = next_list[0]
        r = requests.get(next_link).json()
    communities = r['_embedded']['communities']
    for community in communities:
        comm_id = community['id']
        uuid = community['uuid']
        name = community['name']
        handle = community['handle']
        comm_dict = {'id': comm_id, 'uuid': uuid,
                     'name': name, 'handle': handle}
        all_items.append(comm_dict)
    results = r['page']
    totalPage = results['totalPages']
    number = results['number']
    number = number + 1
    print('Page {} of {}.'.format(number, totalPage))
    next_list.clear()
    links = r.get('_links')
    nextDict = links.get('next')
    if nextDict:
        next_link = nextDict.get('href')
        next_list.append(next_link)
    else:
        break

all_items = pd.DataFrame.from_dict(all_items)
all_items.to_csv('allCommunityItems.csv', index=False)
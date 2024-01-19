""" provide UUID for a DSpace 7 collection or community
    output a CSV file with metadata for all items in that collection or community
"""

import requests
import pandas as pd


def main():
    # it looks like the way to retrieve items by collection in DSpace 7 is via the search endpoint
    # specify the collection or community UUID in the scope parameter
    # see https://groups.google.com/g/dspace-tech/c/jZ5eSMl1c1U

    # this example collection is the Baltimore City Sheet Maps collection
    # expecting 427 items
    collection_uuids = ["7e692697-d553-4887-8404-61a0821e42b8"]

    base_url = "https://j10p-stage.library.jhu.edu"
    timeout = 10
    size = 100

    all_items = []
    for uuid in collection_uuids:
        endpoint = f"{base_url}/server/api/discover/search/objects?scope={uuid}"
        for page in get_paginated_data(endpoint, size, timeout):
            results = page["_embedded"]["searchResult"]["_embedded"]["objects"]
            for result in results:
                item = result["_embedded"]["indexableObject"]
                item_dict = json_to_flat_dict(item)
                all_items.append(item_dict)

    all_items = pd.DataFrame.from_dict(all_items)
    all_items.to_csv("allCollectionItems.csv", index=False)


def get_paginated_data(endpoint, size, timeout):
    """generator function for paginated search results
    uses the "next" links within the returned data
    """
    while endpoint:
        r = requests.get(endpoint, timeout=timeout, params={"size": size})
        if r.status_code == 200:
            data = r.json()

            # print "Page X of Y"
            page_data = data["_embedded"]["searchResult"]["page"]
            num = page_data["number"] + 1
            total = page_data["totalPages"]
            print(f"Page {num} of {total}")
            # on the last iteration, print total element count
            if num == total:
                print(f"Total elements: {page_data['totalElements']}")

            yield data

            # get next page
            next_link = data["_embedded"]["searchResult"]["_links"].get("next")
            endpoint = next_link["href"] if next_link else None


def json_to_flat_dict(item):
    """take JSON item with nested/multi-value fields returned from DSpace 7 API

    return a dict with a flat structure. where a field has multiple values,
    these values are concatenated separated by a pipe character.
    """
    item_dict = {"uuid": item["uuid"]}
    for fieldname, field_values in item["metadata"].items():
        str_value = "|".join([i["value"] for i in field_values])
        item_dict[fieldname] = str_value
    return item_dict


if __name__ == "__main__":
    main()

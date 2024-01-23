""" Output a CSV with data for all communities in the DSpace instance.
    CSV headers: id, uuid, name, handle
"""
import pandas as pd
from get_utils import get_paginated_data


def main():
    base_url = "https://j10p-stage.library.jhu.edu"
    endpoint = f"{base_url}/server/api/core/communities"
    timeout = 10
    size = 100

    all_items = []
    for page in get_paginated_data(endpoint, size, timeout):
        for community in page["_embedded"]["communities"]:
            community_dict = {
                "id": community["id"],
                "uuid": community["uuid"],
                "name": community["name"],
                "handle": community["handle"],
            }
            all_items.append(community_dict)

    all_items = pd.DataFrame.from_dict(all_items)
    all_items.to_csv("allCommunities.csv", index=False)


if __name__ == "__main__":
    main()

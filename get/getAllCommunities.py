import requests
import pandas as pd


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
                "handle": community["handle"]
            }
            all_items.append(community_dict)

    all_items = pd.DataFrame.from_dict(all_items)
    all_items.to_csv("allCommunities.csv", index=False)


def get_paginated_data(endpoint, size, timeout):
    """generator function for paginated data
    uses the "next" links within the returned data
    """
    while endpoint:
        r = requests.get(endpoint, timeout=timeout, params={"size": size})
        if r.status_code == 200:
            data = r.json()

            # print "Page X of Y"
            page_data = data["page"]
            num = page_data["number"] + 1
            total = page_data["totalPages"]
            print(f"Page {num} of {total}")
            # on the last iteration, print total element count
            if num == total:
                print(f"Total elements: {page_data['totalElements']}")

            yield data

            # get next page
            next_link = data["_links"].get("next")
            endpoint = next_link["href"] if next_link else None



if __name__ == "__main__":
    main()

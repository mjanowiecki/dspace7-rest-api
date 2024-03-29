""" Functions useful for making GET requests to the DSpace 7 REST API,
    designed to be imported into other scripts.

    * Get paginated objects and search results
    * Process returned metadata: join the values of multi-value fields into a
      "|"-separated string for CSV output

"""

import requests


def get_paginated_data(endpoint, size, timeout):
    """generator function for paginated data
    uses the "next" links within the returned data

    For use with endpoints such as /api/core/communities and
    /api/core/collections that return a list of objects.

    For endpoints that return search results, such as
    /api/discover/search/, use get_paginated_search_results

    """
    # open session
    with requests.Session() as session:
        session.timeout = timeout
        session.params = {"size": size}

        # make paginated requests
        while endpoint:
            r = session.get(endpoint)
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

                # return data from the page
                yield data

                # get next page
                next_link = data["_links"].get("next")
                endpoint = next_link["href"] if next_link else None


def get_paginated_search_results(endpoint, size, timeout):
    """generator function for paginated search results
    uses the "next" links within the returned data

    For use with endpoints that return search results,
    /api/discover/search/

    """
    # open session
    with requests.Session() as session:
        session.timeout = timeout
        session.params = {"size": size}

        # make paginated requests
        while endpoint:
            r = session.get(endpoint)
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

                # return data from the page
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

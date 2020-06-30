
# ---
# name: pipedrive-products
# deployed: true
# config: index
# title: Pipedrive Products
# description: Returns a list of products from Pipedrive
# params:
#   - name: properties
#     type: array
#     description: The properties to return (defaults to all properties). See "Returns" for a listing of the available properties.
#     required: false
#   - name: filter
#     type: string
#     description: Filter to apply with key/values specified as a URL query string where the keys correspond to the properties to filter.
#     required: false
# returns:
#   - name: id
#     type: integer
#     description: The id of the product
#   - name: name
#     type: string
#     description: The name of the product
#   - name: code
#     type: string
#     description: The code of the product
#   - name: description
#     type: string
#     description: A description of the product
#   - name: unit
#     type: string
#     description: The unit in which the product is sold
#   - name: category
#     type: string
#     description: The category of the product
#   - name: tax
#     type: number
#     description: The tax percentage of the product
#   - name: price_id
#     type: integer
#     description: The id of the price associated with the product
#   - name: price
#     type: number
#     description: The price of the product in a particular currency
#   - name: cost
#     type: number
#     description: The cost of the product in a particular currency
#   - name: overhead_cost
#     type: number
#     description: The overhead cost of the product in a particular currency
#   - name: currency
#     type: string
#     description: The currency unit for the price and cost of the product
#   - name: active_flag
#     type: string
#     description: Whether or not the product is active
#   - name: owner_id
#     type: integer
#     description: The id of the owner associated with the deal
#   - name: owner_name
#     type: string
#     description: The name of the owner associated with the deal
#   - name: owner_email
#     type: string
#     description: The email of the owner associated with the deal
#   - name: followers_count
#     type: integer
#     description: The number of followers associated with the product
#   - name: add_time
#     type: string
#     description: The date the product was added
#   - name: update_time
#     type: string
#     description: The date the product was last updated
# examples:
#   - '""'
# ---

import json
import urllib
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import *
from decimal import *
from collections import OrderedDict

# main function entry point
def flexio_handler(flex):

    flex.output.content_type = 'application/x-ndjson'
    for data in get_data(flex.vars):
        flex.output.write(data)

def get_data(params):

    # get the api key and company domain from the variable input
    auth_token = dict(params).get('pipedrive_connection',{}).get('access_token')
    api_base_uri = dict(params).get('pipedrive_connection',{}).get('api_base_uri')

    # see here for more info:
    # https://developers.pipedrive.com/docs/api/v1/#!/Products/get_products
    # https://pipedrive.readme.io/docs/core-api-concepts-pagination

    headers = {
        'Authorization': 'Bearer ' + auth_token
    }
    url = api_base_uri + '/v1/products'

    page_size = 500
    page_cursor_id = None
    while True:

        url_query_params = {
            'limit': page_size
        }

        if page_cursor_id is not None:
            url_query_params['start'] = page_cursor_id
        url_query_str = urllib.parse.urlencode(url_query_params)
        page_url = url + '?' + url_query_str

        response = requests_retry_session().get(page_url, headers=headers)
        response.raise_for_status()
        content = response.json()
        data = content.get('data',[])

        if len(data) == 0: # sanity check in case there's an issue with cursor
            break

        buffer = ''
        for header_item in data:
            detail_items_all =  header_item.get('prices',[])
            if len(detail_items_all) == 0:
                item = get_item_info(header_item, {}) # if we don't have any prices, make sure to return item header info
                buffer = buffer + json.dumps(item, default=to_string) + "\n"
            else:
                for detail_item in detail_items_all:
                    item = get_item_info(header_item, detail_item)
                    buffer = buffer + json.dumps(item, default=to_string) + "\n"
        yield buffer

        has_more = content.get('additional_data',{}).get('pagination',{}).get('more_items_in_collection', False)
        if has_more == False:
            break

        page_cursor_id = content.get('additional_data',{}).get('pagination',{}).get('next_start')
        if page_cursor_id is None:
            break

def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def to_date(value):
    # TODO: convert if needed
    return value

def to_string(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (int)):
        return str(value)
    if isinstance(value, (Decimal)):
        return str(value)
    return value

def get_item_info(header_item, detail_item):

    # map this function's property names to the API's property names
    info = OrderedDict()

    info['id'] = header_item.get('id')
    info['name'] = header_item.get('name')
    info['code'] = header_item.get('code')
    info['description'] = header_item.get('description')
    info['unit'] = header_item.get('unit')
    info['category'] = header_item.get('category')
    info['tax'] = header_item.get('tax')
    info['price_id'] = detail_item.get('id')
    info['price'] = detail_item.get('price')
    info['cost'] = detail_item.get('cost')
    info['overhead_cost'] = detail_item.get('overhead_cost')
    info['currency'] = detail_item.get('currency')
    info['active_flag'] = header_item.get('active_flag')
    info['owner_id'] = (header_item.get('owner_id') or {}).get('id')
    info['owner_name'] = (header_item.get('owner_id') or {}).get('name')
    info['owner_email'] = (header_item.get('owner_id') or {}).get('email')
    info['followers_count'] = header_item.get('followers_count')
    info['add_time'] = to_date(header_item.get('add_time'))
    info['update_time'] = to_date(header_item.get('update_time'))

    return info


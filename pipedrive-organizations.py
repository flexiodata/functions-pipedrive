
# ---
# name: pipedrive-organizations
# deployed: true
# config: index
# title: Pipedrive Organizations
# description: Returns a list of organizations from Pipedrive
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
#     description: The id associated with the organization
#   - name: name
#     type: string
#     description: The name of the organization
#   - name: label
#     type: string
#     description: The label for the organization
#   - name: active_flag
#     type: boolean
#     description: Whether or not the organization is active
#   - name: add_time
#     type: string
#     description: The date the information for the organization was first added
#   - name: update_time
#     type: string
#     description: The last date the information for the organization was updated
#   - name: address
#     type: string
#     description: The address for the organization
#   - name: address_subpremise
#     type: string
#     description: The address subpremise for the organization
#   - name: address_street_number
#     type: string
#     description: The address street number for the organization
#   - name: address_route
#     type: string
#     description: The address route for the organization
#   - name: address_sublocality
#     type: string
#     description: The address sublocality for the organization
#   - name: address_locality
#     type: string
#     description: The address locality for the organization
#   - name: address_admin_area_level_1
#     type: string
#     description: The address admin area level 1 for the organization
#   - name: address_admin_area_level_2
#     type: string
#     description: The address admin area level 2 for the organization
#   - name: address_country
#     type: string
#     description: The address country for the organization
#   - name: address_postal_code
#     type: string
#     description: The address postal code for the organization
#   - name: last_activity_id
#     type: integer
#     description: The last activity id associated with the organization
#   - name: last_activity_date
#     type: string
#     description: The last activity date associated with the organization
#   - name: next_activity_id
#     type: integer
#     description: The next activity id associated with the organization
#   - name: next_activity_date
#     type: string
#     description: The next activity date associated with the organization
#   - name: activities_count
#     type: integer
#     description: A count of the activity for the organization
#   - name: done_activities_count
#     type: integer
#     description: A count of the activity done for the organization
#   - name: undone_activities_count
#     type: integer
#     description: A count of the activity not done for the organization
#   - name: reference_activities_count
#     type: integer
#     description: A count of the reference activities for the organization
#   - name: open_deals_count
#     type: integer
#     description: A count of the open deals associated with the organization
#   - name: closed_deals_count
#     type: integer
#     description: A count of the closed deals associated with the organization
#   - name: won_deals_count
#     type: integer
#     description: A count of the won deals associated with the organization
#   - name: lost_deals_count
#     type: integer
#     description: A count of the lost deals associated with the organization
#   - name: related_won_deals_count
#     type: integer
#     description: A count of the related won deals associated with the organization
#   - name: related_lost_deals_count
#     type: integer
#     description: A count of the related lost deals associated with the organization
#   - name: related_open_deals_count
#     type: integer
#     description: A count of the related open deals associated with the organization
#   - name: related_closed_deals_count
#     type: integer
#     description:  A count of the related closed deals associated with the organization
#   - name: files_count
#     type: integer
#     description: A count of the files associated with the organization
#   - name: notes_count
#     type: integer
#     description: A count of the notes associated with the organization
#   - name: followers_count
#     type: integer
#     description: A count of the followers associated with the organization
#   - name: email_messages_count
#     type: integer
#     description: A count of the email messages associated with the organization
#   - name: people_count
#     type: integer
#     description: A count of the people associated with the organization
# examples:
#   - '""'
#   - '"name, address"'
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
    for item in get_data(flex.vars):
        result = json.dumps(item, default=to_string) + "\n"
        flex.output.write(result)

def get_data(params):

    # get the api key and company domain from the variable input
    auth_token = dict(params).get('pipedrive_connection',{}).get('access_token')
    api_base_uri = dict(params).get('pipedrive_connection',{}).get('api_base_uri')

    # see here for more info:
    # https://developers.pipedrive.com/docs/api/v1/#!/Organizations/get_organizations
    # https://pipedrive.readme.io/docs/core-api-concepts-pagination

    headers = {
        'Authorization': 'Bearer ' + auth_token
    }
    url = api_base_uri + '/v1/organizations'

    page_size = 500
    page_cursor_id = None
    while True:

        url_query_params = {'limit': page_size}
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

        for item in data:
            yield get_item_info(item)

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

def get_item_info(item):

    # map this function's property names to the API's property names
    info = OrderedDict()

    info['id'] = item.get('id')
    info['name'] = item.get('name')
    info['label'] = item.get('label')
    info['active_flag'] = item.get('active_flag')
    info['add_time'] = to_date(item.get('add_time'))
    info['update_time'] = to_date(item.get('update_time'))
    info['address'] = item.get('address')
    info['address_subpremise'] = item.get('address_subpremise')
    info['address_street_number'] = item.get('address_street_number')
    info['address_route'] = item.get('address_route')
    info['address_sublocality'] = item.get('address_sublocality')
    info['address_locality'] = item.get('address_locality')
    info['address_admin_area_level_1'] = item.get('address_admin_area_level_1')
    info['address_admin_area_level_2'] = item.get('address_admin_area_level_2')
    info['address_country'] = item.get('address_country')
    info['address_postal_code'] = item.get('address_postal_code')
    info['last_activity_id'] = item.get('last_activity_id')
    info['last_activity_date'] = to_date(item.get('last_activity_date'))
    info['next_activity_id'] = item.get('next_activity_id')
    info['next_activity_date'] = to_date(item.get('next_activity_date'))
    info['activities_count'] = item.get('activities_count')
    info['done_activities_count'] = item.get('done_activities_count')
    info['undone_activities_count'] = item.get('undone_activities_count')
    info['reference_activities_count'] = item.get('reference_activities_count')
    info['open_deals_count'] = item.get('open_deals_count')
    info['closed_deals_count'] = item.get('closed_deals_count')
    info['won_deals_count'] = item.get('won_deals_count')
    info['lost_deals_count'] = item.get('lost_deals_count')
    info['related_won_deals_count'] = item.get('related_won_deals_count')
    info['related_lost_deals_count'] = item.get('related_lost_deals_count')
    info['related_open_deals_count'] = item.get('related_open_deals_count')
    info['related_closed_deals_count'] = item.get('related_closed_deals_count')
    info['files_count'] = item.get('files_count')
    info['notes_count'] = item.get('notes_count')
    info['followers_count'] = item.get('followers_count')
    info['email_messages_count'] = item.get('email_messages_count')
    info['people_count'] = item.get('people_count')

    return info

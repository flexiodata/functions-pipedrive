
# ---
# name: pipedrive-deals
# deployed: true
# config: index
# title: Pipedrive Deals
# description: Returns a list of deals from Pipedrive
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
#     description: The id of the deal
#   - name: title
#     type: string
#     description: The title of the deal
#   - name: label
#     type: string
#     description: The label of the deal
#   - name: value
#     type: number
#     description: The value of the deal
#   - name: currency
#     type: string
#     description: The currency type of the value of the deal
#   - name: add_time
#     type: string
#     description: The date the deal was added
#   - name: update_time
#     type: string
#     description: The date the deal was last updated
#   - name: active
#     type: boolean
#     description: Whether or not the deal is active
#   - name: deleted
#     type: boolean
#     description: Whether or not the deal is deleted
#   - name: status
#     type: string
#     description: The status of the deal
#   - name: probability
#     type: string
#     description: The probability associated with the deal
#   - name: creator_user_id
#     type: integer
#     description: The id of the creator of the deal
#   - name: creator_user_name
#     type: string
#     description: The name of the creator of the deal
#   - name: creator_user_email
#     type: string
#     description: The email of the creator of the deal
#   - name: user_id
#     type: integer
#     description: The id of the user associated with the deal
#   - name: user_name
#     type: string
#     description: The name of the user associated with the deal
#   - name: user_email
#     type: string
#     description: The email of the user associated with the deal
#   - name: person_name
#     type: string
#     description: The name of the person in the organization associated with the deal
#   - name: org_name
#     type: string
#     description: The name of the organization associated with the deal
#   - name: org_address
#     type: string
#     description: The address of the organization associated with the deal
#   - name: pipeline_id
#     type: integer
#     description: The pipeline id currently associated with the deal
#   - name: stage_id
#     type: integer
#     description: The stage id of the deal
#   - name: stage_change_time
#     type: string
#     description: The date the deal stage was last changed
#   - name: last_activity_id
#     type: integer
#     description: The id of the last activity event associated with the deal
#   - name: last_activity_date
#     type: string
#     description: The date of the last activity event associated with the deal
#   - name: next_activity_id
#     type: integer
#     description: The id of the next activity event scheduled for the deal
#   - name: next_activity_date
#     type: string
#     description: The date of the next activity event scheduled for the deal
#   - name: next_activity_subject
#     type: string
#     description: The subject of the next activity
#   - name: next_activity_type
#     type: string
#     description: The type of the next activity
#   - name: next_activity_duration
#     type: integer
#     description: The duration of the next activity
#   - name: next_activity_note
#     type: string
#     description: Notes associated with the next activity
#   - name: expected_close_date
#     type: string
#     description: The expected date the deal will close
#   - name: close_time
#     type: string
#     description: The date the deal was closed
#   - name: won_time
#     type: string
#     description: The date the deal was won
#   - name: lost_time
#     type: string
#     description: The date the deal was lost
#   - name: lost_reason
#     type: string
#     description: The reason the deal was lost
#   - name: products_count
#     type: integer
#     description: The number of products associated with the deal
#   - name: files_count
#     type: integer
#     description: The number of files associated with the deal
#   - name: notes_count
#     type: integer
#     description: The number of notes associated with the deal
#   - name: email_messages_count
#     type: integer
#     description: The number of emails associated with the deal
#   - name: activities_count
#     type: integer
#     description: The number of activities associated with the deal
#   - name: done_activities_count
#     type: integer
#     description: The number of done activities associated with the deal
#   - name: undone_activities_count
#     type: integer
#     description: The number of undone activities associated with the deal
#   - name: reference_activities_count
#     type: integer
#     description: The number of reference activities associated with the deal
#   - name: participants_count
#     type: integer
#     description: The number of participants associated with the deal
#   - name: followers_count
#     type: integer
#     description: The number of followers associated with the deal
# examples:
#   - '""'
#   - '"title, value, status, add_time"'
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
    # https://developers.pipedrive.com/docs/api/v1/#!/Deals/get_deals
    # https://pipedrive.readme.io/docs/core-api-concepts-pagination

    headers = {
        'Authorization': 'Bearer ' + auth_token
    }
    url = api_base_uri + '/v1/deals'

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
    info['title'] = item.get('title')
    info['label'] = item.get('label')
    info['value'] = item.get('value')
    info['currency'] = item.get('currency')
    info['add_time'] = to_date(item.get('add_time'))
    info['update_time'] = to_date(item.get('update_time'))
    info['active'] = item.get('active')
    info['deleted'] = item.get('deleted')
    info['status'] = item.get('status')
    info['probability'] = item.get('probability')
    info['creator_user_id'] = item.get('creator_user_id',{}).get('id')
    info['creator_user_name'] = item.get('creator_user_id',{}).get('name')
    info['creator_user_email'] = item.get('creator_user_id',{}).get('email')
    info['user_id'] = item.get('user_id',{}).get('id')
    info['user_name'] = item.get('user_id',{}).get('name')
    info['user_email'] = item.get('user_id',{}).get('email')
    info['person_name'] = item.get('person_id',{}).get('name')
    info['org_name'] = item.get('org_id',{}).get('name')
    info['org_address'] = item.get('org_id',{}).get('address')
    info['pipeline_id'] = item.get('pipeline_id')
    info['stage_id'] = item.get('stage_id')
    info['stage_change_time'] = to_date(item.get('stage_change_time'))
    info['last_activity_id'] = item.get('last_activity_id')
    info['last_activity_date'] = to_date(item.get('last_activity_date'))
    info['next_activity_id'] = item.get('next_activity_id')
    info['next_activity_date'] = to_date(item.get('next_activity_date'))
    info['next_activity_subject'] = item.get('next_activity_subject')
    info['next_activity_type'] = item.get('next_activity_type')
    info['next_activity_duration'] = item.get('next_activity_duration')
    info['next_activity_note'] = item.get('next_activity_note')
    info['expected_close_date'] = to_date(item.get('expected_close_date'))
    info['close_time'] = to_date(item.get('close_time'))
    info['won_time'] = to_date(item.get('won_time'))
    info['lost_time'] = to_date(item.get('lost_time'))
    info['lost_reason'] = item.get('lost_reason')
    info['products_count'] = item.get('products_count')
    info['files_count'] = item.get('files_count')
    info['notes_count'] = item.get('notes_count')
    info['email_messages_count'] = item.get('email_messages_count')
    info['activities_count'] = item.get('activities_count')
    info['done_activities_count'] = item.get('done_activities_count')
    info['undone_activities_count'] = item.get('undone_activities_count')
    info['reference_activities_count'] = item.get('reference_activities_count')
    info['participants_count'] = item.get('participants_count')
    info['followers_count'] = item.get('followers_count')

    return info

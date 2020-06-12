
# ---
# name: pipedrive-activity
# deployed: true
# config: index
# title: Pipedrive Activity
# description: Returns a list of activity from Pipedrive
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
#     description: The id of activity
#   - name: user_id
#     type: integer
#     description: The id of the user the activity is assigned to
#   - name: created_by_user_id
#     type: integer
#     description: The id of the user that created the activity
#   - name: org_id
#     type: integer
#     description: The id of the organization associated with the activity
#   - name: org_name
#     type: string
#     description: The name of the organization associated with the activity
#   - name: person_id
#     type: integer
#     description: The id of the contact person associated with the activity
#   - name: person_name
#     type: string
#     description: The name of the contact person associated with the activity
#   - name: lead_id
#     type: integer
#     description: The id of the lead associated with the activity
#   - name: lead_title
#     type: string
#     description: The title of the lead associated with the activity
#   - name: deal_id
#     type: integer
#     description: The id of the deal associated with the activity
#   - name: deal_title
#     type: string
#     description: The title of the deal associated with the activity
#   - name: subject
#     type: string
#     description: The subject of the activity
#   - name: type
#     type: string
#     description: The type of activity
#   - name: done
#     type: boolean
#     description: Whether or not the activity is done
#   - name: marked_as_done_time
#     type: string
#     description: Marked as done time
#   - name: due_date
#     type: string
#     description: The date the activity is due
#   - name: due_time
#     type: string
#     description: The time the activity is due
#   - name: duration
#     type: string
#     description: The duration of the activity
#   - name: add_time
#     type: string
#     description: The time the activity was added
#   - name: update_time
#     type: string
#     description: The time of the last update
#   - name: last_notification_time
#     type: string
#     description: The time of the last notification
#   - name: busy_flag
#     type: string
#     description: A flag indicating the busy status
#   - name: public_description
#     type: string
#     description: A public description of the activity
#   - name: note
#     type: string
#     description: A note for the activity
#   - name: location_subpremise
#     type: string
#     description: The apartment or suite number of the location address
#   - name: location_street_number
#     type: string
#     description: The street number of the location address
#   - name: location_route
#     type: string
#     description: The street name of the location address
#   - name: location_sublocality
#     type: string
#     description: The district or sublocality of the location address
#   - name: location_locality
#     type: string
#     description: The city, town, village, or locality of the location address
#   - name: location_admin_area_level_1
#     type: string
#     description: The state or county of the location address
#   - name: location_admin_area_level_2
#     type: string
#     description: The region of the location address
#   - name: location_country
#     type: string
#     description: The country of the location address
#   - name: location_postal_code
#     type: string
#     description: The postal code of the location address
#   - name: location_formatted_address
#     type: string
#     description: The combined location address
#   - name: conference_meeting_client
#     type: string
#     description: The conference meeting client
#   - name: conference_meeting_url
#     type: string
#     description: The conference meeting link
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
    # https://developers.pipedrive.com/docs/api/v1/#!/Activities/get_activities
    # https://pipedrive.readme.io/docs/core-api-concepts-pagination

    headers = {
        'Authorization': 'Bearer ' + auth_token
    }
    url = api_base_uri + '/v1/activities'

    page_size = 500
    page_cursor_id = None
    while True:

        url_query_params = {
            'limit': page_size,
            'user_id': 0 # return all activities that the user has access to, not just activity for a specific user
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
        for item in data:
            item = get_item_info(item)
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

def get_item_info(item):

    # map this function's property names to the API's property names
    info = OrderedDict()

    info['id'] = item.get('id')
    info['user_id'] = item.get('user_id')
    info['created_by_user_id'] = item.get('created_by_user_id')
    info['org_id'] = item.get('org_id')
    info['org_name'] = item.get('org_name')
    info['person_id'] = item.get('person_id')
    info['person_name'] = item.get('person_name')
    info['lead_id'] = item.get('lead_id')
    info['lead_title'] = item.get('lead_title')
    info['deal_id'] = item.get('deal_id')
    info['deal_title'] = item.get('deal_title')
    info['subject'] = item.get('subject')
    info['type'] = item.get('type')
    info['done'] = item.get('done')
    info['marked_as_done_time'] = to_date(item.get('marked_as_done_time'))
    info['due_date'] = to_date(item.get('due_date'))
    info['due_time'] = to_date(item.get('due_time'))
    info['duration'] = to_date(item.get('duration'))
    info['add_time'] = to_date(item.get('add_time'))
    info['update_time'] = to_date(item.get('update_time'))
    info['last_notification_time'] = to_date(item.get('last_notification_time'))
    info['busy_flag'] = item.get('busy_flag')
    info['public_description'] = item.get('public_description')
    info['note'] = item.get('note')
    info['location_subpremise'] = item.get('location_subpremise')
    info['location_street_number'] = item.get('location_street_number')
    info['location_route'] = item.get('location_route')
    info['location_sublocality'] = item.get('location_sublocality')
    info['location_locality'] = item.get('location_locality')
    info['location_admin_area_level_1'] = item.get('location_admin_area_level_1')
    info['location_admin_area_level_2'] = item.get('location_admin_area_level_2')
    info['location_country'] = item.get('location_country')
    info['location_postal_code'] = item.get('location_postal_code')
    info['location_formatted_address'] = item.get('location_formatted_address')
    info['conference_meeting_client'] = item.get('conference_meeting_client')
    info['conference_meeting_url'] = item.get('conference_meeting_url')

    return info
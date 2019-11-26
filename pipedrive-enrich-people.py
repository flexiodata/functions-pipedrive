
# ---
# name: pipedrive-enrich-people
# deployed: true
# title: Pipedrive People Enrichment
# description: Returns profile information of a person in Pipedrive based on the search term
# params:
#   - name: search_term
#     type: string
#     description: The search term to look for; minimum of 2 characters.
#     required: true
#   - name: properties
#     type: array
#     description: The properties to return (defaults to `name`). See "Notes" for a listing of the available properties.
#     required: false
# examples:
# notes: |
#   The following properties are available:
#     * `id`: The person's id
#     * `name`: The person's name (default)
#     * `label`: The person's label
#     * `phone`: Phone number
#     * `email`: Email address
#     * `add_date`: Created date
#     * `update_date`: Updated date
#     * `open_deals_cnt`: Number of open deals
#     * `next_activity_date`: Next activity date
#     * `last_activity_date`: Last activity date
#     * `won_deals_cnt`: Number of won deals
#     * `lost_deals_cnt`: Number of lost deals
#     * `closed_deals_cnt`: Number of closed deals
#     * `activities_cnt`: Number of total activities
#     * `done_activities_cnt`: Number of done activities
#     * `undone_activities_cnt`: Number of activities to do
#     * `email_messages_cnt`: Number of email messages
#     * `picture_id`: Profile picture
#     * `last_incoming_mail_date`: Date of the last email received
#     * `last_outgoing_mail_date`: Date of the last email sent
# ---

import json
import requests
import urllib
import itertools
from datetime import *
from cerberus import Validator
from collections import OrderedDict

# main function entry point
def flexio_handler(flex):

    # get the api key from the variable input
    auth_token = dict(flex.vars).get('pipedrive_connection',{}).get('access_token')
    if auth_token is None:
        flex.output.content_type = "application/json"
        flex.output.write([[""]])
        return

    # get the company domain from the variable input
    api_base_uri = dict(flex.vars).get('pipedrive_connection',{}).get('api_base_uri')
    if api_base_uri is None:
        flex.output.content_type = "application/json"
        flex.output.write([[""]])
        return

    # get the input
    input = flex.input.read()
    try:
        input = json.loads(input)
        if not isinstance(input, list): raise ValueError
    except ValueError:
        raise ValueError

    # define the expected parameters and map the values to the parameter names
    # based on the positions of the keys/values
    params = OrderedDict()
    params['search_term'] = {'required': True, 'type': 'string'}
    params['properties'] = {'required': False, 'validator': validator_list, 'coerce': to_list, 'default': 'name'}
    input = dict(zip(params.keys(), input))

    # validate the mapped input against the validator
    # if the input is valid return an error
    v = Validator(params, allow_unknown = True)
    input = v.validated(input)
    if input is None:
        raise ValueError

    # map this function's property names to the API's property names
    property_map = OrderedDict()
    property_map['id'] = 'id'
    property_map['name'] = 'name'
    property_map['label'] = 'label'
    property_map['phone'] = 'phone'
    property_map['email'] = 'email'
    property_map['add_date'] = 'add_time'
    property_map['update_date'] = 'update_time'
    property_map['open_deals_cnt'] = 'open_deals_count'
    property_map['next_activity_date'] = 'next_activity_date'
    property_map['last_activity_date'] = 'last_activity_date'
    property_map['won_deals_cnt'] = 'won_deals_count'
    property_map['lost_deals_cnt'] = 'lost_deals_count'
    property_map['closed_deals_cnt'] = 'closed_deals_count'
    property_map['activities_cnt'] = 'activities_count'
    property_map['done_activities_cnt'] = 'done_activities_count'
    property_map['undone_activities_cnt'] = 'undone_activities_count'
    property_map['email_messages_cnt'] = 'email_messages_count'
    property_map['picture_id'] = 'picture_id'
    property_map['last_incoming_mail_date'] = 'last_incoming_mail_time'
    property_map['last_outgoing_mail_date'] = 'last_outgoing_mail_time'

    try:

        #### DO THE SEARCH ####

        # see here for more info:
        # https://developers.pipedrive.com/docs/api/v1/#!/SearchResults/get_searchResults
        url_query_params = {
            'item_type': 'person',
            'term': input['search_term']
        }
        url_query_str = urllib.parse.urlencode(url_query_params)
        url = api_base_uri + '/v1/searchResults?' + url_query_str
        headers = {
            'Authorization': 'Bearer ' + auth_token
        }

        # get the response data as a JSON object
        response = requests.get(url, headers=headers)
        content = response.json()

        # get the first element of the search results
        search_results = content.get('data',[]) or []

        # bail out; no results were returned
        if len(search_results) == 0:
            flex.output.content_type = "application/json"
            flex.output.write([['No people match the search term']])
            return

        # get the organization id from the search results
        person_id = search_results[0]['id']
        person_id = to_string(person_id)

        #### QUERY THE ORGANIZATION ####

        # list of this function's properties we'd like to query
        properties = [p.lower().strip() for p in input['properties']]

        # if we have a wildcard, get all the properties
        if len(properties) == 1 and properties[0] == '*':
            properties = list(property_map.keys())

        # list of the Pipedrive properties we'd like to query
        pipedrive_properties = [property_map[p] for p in properties]

        # see here for more info:
        # https://developers.pipedrive.com/docs/api/v1/#!/Organizations/get_persons
        url = api_base_uri + '/v1/persons/' + person_id
        headers = {
            'Authorization': 'Bearer ' + auth_token
        }

        # get the response data as a JSON object
        response = requests.get(url, headers=headers)
        content = response.json()

        # get the response data as a JSON object
        response = requests.get(url)
        content = response.json()

        # limit the results to the requested properties
        result = []
        for p in pipedrive_properties:
            # map scalar values
            val = content.get('data',{}).get(p,'') or ''

            # map values that are nested in an array
            if isinstance(val, list) and len(val) > 0:
                if p is 'phone' or p is 'email':
                    val = val[0]
                    val = val.get('value','') or ''

            result.append(val)
        result = [result]

        # return the results
        result = json.dumps(result, default=to_string)
        flex.output.content_type = "application/json"
        flex.output.write(result)

    except:
        raise RuntimeError

def validator_list(field, value, error):
    if isinstance(value, str):
        return
    if isinstance(value, list):
        for item in value:
            if not isinstance(item, str):
                error(field, 'Must be a list with only string values')
        return
    error(field, 'Must be a string or a list of strings')

def to_string(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (int)):
        return str(value)
    if isinstance(value, (Decimal)):
        return str(value)
    return value

def to_list(value):
    # if we have a list of strings, create a list from them; if we have
    # a list of lists, flatten it into a single list of strings
    if isinstance(value, str):
        return value.split(",")
    if isinstance(value, list):
        return list(itertools.chain.from_iterable(value))
    return None


# ---
# name: pipedrive-enrich-org
# deployed: true
# title: Pipedrive Organization Enrichment
# description: Returns profile information of a company in Pipedrive based on the search term
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
#     * `id`: Organization id
#     * `name`: Organization name (default)
#     * `label`: Organization label
#     * `people_cnt`: Number of people in the organization
#     * `open_deals_cnt`: Number of open deals
#     * `add_date`: Created date
#     * `update_date`: Updated date
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
#     * `address`: Address
#     * `address_suite`: Apartment or suite number
#     * `address_number`: Street number
#     * `address_street`: Street name
#     * `address_district`: District/sublocality
#     * `address_city`: City/town/village/locality
#     * `address_state`: State/county
#     * `address_region`: Region
#     * `address_country`: Country
#     * `address_postal_code`: Zip/postal code
#     * `address_combined`: Combined address
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
    auth_token = dict(flex.vars).get('pipedrive_connection').get('access_token')
    if auth_token is None:
        flex.output.content_type = "application/json"
        flex.output.write([[""]])
        return

    # get the company domain from the variable input
    api_base_uri = dict(flex.vars).get('pipedrive_connection').get('api_base_uri')
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
    property_map['people_cnt'] = 'people_count'
    property_map['open_deals_cnt'] = 'open_deals_count'
    property_map['add_date'] = 'add_time'
    property_map['update_date'] = 'update_time'
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
    property_map['address'] = 'address'
    property_map['address_suite'] = 'address_subpremise'
    property_map['address_number'] = 'address_street_number'
    property_map['address_street'] = 'address_route'
    property_map['address_district'] = 'address_sublocality'
    property_map['address_city'] = 'address_locality'
    property_map['address_state'] = 'address_admin_area_level_1'
    property_map['address_region'] = 'address_admin_area_level_2'
    property_map['address_country'] = 'address_country'
    property_map['address_postal_code'] = 'address_postal_code'
    property_map['address_combined'] = 'address_formatted_address'

    try:

        #### DO THE SEARCH ####

        # see here for more info:
        # https://developers.pipedrive.com/docs/api/v1/#!/SearchResults/get_searchResults
        url_query_params = {
            'api_token': auth_token,
            'item_type': 'organization',
            'term': input['search_term']
        }
        url_query_str = urllib.parse.urlencode(url_query_params)
        url = api_base_uri + '/v1/searchResults?' + url_query_str

        # get the response data as a JSON object
        response = requests.get(url)
        content = response.json()

        # get the first element of the search results
        search_results = content.get('data',[]) or []

        # bail out; no results were returned
        if len(search_results) == 0:
            flex.output.content_type = "application/json"
            flex.output.write([['No organizations match the search term']])
            return

        # get the organization id from the search results
        org_id = search_results[0]['id']
        org_id = to_string(org_id)

        #### QUERY THE ORGANIZATION ####

        # list of this function's properties we'd like to query
        properties = [p.lower().strip() for p in input['properties']]

        # if we have a wildcard, get all the properties
        if len(properties) == 1 and properties[0] == '*':
            properties = list(property_map.keys())

        # list of the Pipedrive properties we'd like to query
        pipedrive_properties = [property_map[p] for p in properties]

        # see here for more info:
        # https://developers.pipedrive.com/docs/api/v1/#!/Organizations/get_organizations
        url_query_params = {
            'api_token': auth_token
        }
        url_query_str = urllib.parse.urlencode(url_query_params)
        url = api_base_uri + '/v1/organizations/' + org_id + '?' + url_query_str

        # get the response data as a JSON object
        response = requests.get(url)
        content = response.json()

        # get the response data as a JSON object
        response = requests.get(url)
        content = response.json()

        # limit the results to the requested properties
        result = []
        for p in pipedrive_properties:
            result.append(content.get('data',{}).get(p,'') or '')
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

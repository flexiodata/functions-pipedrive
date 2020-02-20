
# ---
# name: pipedrive-list-orgs
# deployed: true
# title: Pipedrive Organizations List
# description: Returns a list of organizations from Pipedrive
# params:
#   - name: properties
#     type: array
#     description: The properties to return (defaults to all properties). See "Returns" for a listing of the available properties.
#     required: false
# returns:
#   - name: id
#     type: string
#     description: Organization id
#   - name: name
#     type: string
#     description: Organization name
#   - name: label
#     type: string
#     description: Organization label
#   - name: people_cnt
#     type: string
#     description: Number of people in the organization
#   - name: open_deals_cnt
#     type: string
#     description: Number of open deals
#   - name: add_date
#     type: string
#     description: Created date
#   - name: update_date
#     type: string
#     description: Updated date
#   - name: next_activity_date
#     type: string
#     description: Next activity date
#   - name: last_activity_date
#     type: string
#     description: Last activity date
#   - name: won_deals_cnt
#     type: string
#     description: Number of won deals
#   - name: lost_deals_cnt
#     type: string
#     description: Number of lost deals
#   - name: closed_deals_cnt
#     type: string
#     description: Number of closed deals
#   - name: activities_cnt
#     type: string
#     description: Number of total activities
#   - name: done_activities_cnt
#     type: string
#     description: Number of done activities
#   - name: undone_activities_cnt
#     type: string
#     description: Number of activities to do
#   - name: email_messages_cnt
#     type: string
#     description: Number of email messages
#   - name: picture_id
#     type: string
#     description: Profile picture
#   - name: address
#     type: string
#     description: Address
#   - name: address_suite
#     type: string
#     description: Apartment or suite number
#   - name: address_number
#     type: string
#     description: Street number
#   - name: address_street
#     type: string
#     description: Street name
#   - name: address_district
#     type: string
#     description: District/sublocality
#   - name: address_city
#     type: string
#     description: City/town/village/locality
#   - name: address_state
#     type: string
#     description: State/county
#   - name: address_region
#     type: string
#     description: Region
#   - name: address_country
#     type: string
#     description: Country
#   - name: address_postal_code
#     type: string
#     description: Zip/postal code
#   - name: address_combined
#     type: string
#     description: Combined address
# examples:
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
    params['properties'] = {'required': False, 'validator': validator_list, 'coerce': to_list, 'default': '*'}
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

        # list of this function's properties we'd like to query
        properties = [p.lower().strip() for p in input['properties']]

        # if we have a wildcard, get all the properties
        if len(properties) == 1 and properties[0] == '*':
            properties = list(property_map.keys())

        # list of the Pipedrive properties we'd like to query
        pipedrive_properties = [property_map[p] for p in properties]

        # see here for more info:
        # https://developers.pipedrive.com/docs/api/v1/#!/Organizations/get_organizations
        url = api_base_uri + '/v1/organizations'
        headers = {
            'Authorization': 'Bearer ' + auth_token
        }

        # get the response data as a JSON object
        response = requests.get(url, headers=headers)
        content = response.json()

        # return the info
        result = []
        result.append(properties)

        # build up each row and append it to the result
        orgs = content.get('data',[])
        for org in orgs:
            row = []
            for p in pipedrive_properties:
                row.append(org.get(p,'') or '')
            result.append(row)

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

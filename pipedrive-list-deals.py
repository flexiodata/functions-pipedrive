
# ---
# name: pipedrive-list-deals
# deployed: true
# title: Pipedrive Deals List
# description: Returns a list of deals from Pipedrive
# params:
#   - name: properties
#     type: array
#     description: The properties to return (defaults to all properties). See "Returns" for a listing of the available properties.
#     required: false
# returns:
#   - name: deal_name
#     type: string
#     description: Deal name
#   - name: deal_owner
#     type: string
#     description: Deal owner
#   - name: deal_status
#     type: string
#     description: Deal status
#   - name: amt
#     type: string
#     description: Amount
#   - name: amt_home
#     type: string
#     description: Amount in home currency
#   - name: lost_reason
#     type: string
#     description: Lost reason
#   - name: close_date
#     type: string
#     description: Close date
#   - name: pipeline_id
#     type: string
#     description: Pipeline ID
#   - name: participants_cnt
#     type: string
#     description: Number of contacts
#   - name: activities_cnt
#     type: string
#     description: Number of activities
#   - name: last_activity_date
#     type: string
#     description: Last activity date
#   - name: next_activity_date
#     type: string
#     description: Next activity date
#   - name: created_date
#     type: string
#     description: Created date
#   - name: updated_date
#     type: string
#     description: Last activity date
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
    property_map['deal_name'] = 'title'
    property_map['deal_owner'] = 'owner_name'
    property_map['deal_status'] = 'status'
    property_map['amt'] = 'value'
    property_map['amt_home'] = 'weighted_value'
    property_map['lost_reason'] = 'lost_reason'
    property_map['close_date'] = 'close_time'
    property_map['pipeline_id'] = 'pipeline_id'
    property_map['participants_cnt'] = 'participants_count'
    property_map['activities_cnt'] = 'activities_count'
    property_map['last_activity_date'] = 'last_activity_date'
    property_map['next_activity_date'] = 'next_activity_date'
    property_map['created_date'] = 'add_time'
    property_map['updated_date'] = 'update_time'

    try:

        # list of this function's properties we'd like to query
        properties = [p.lower().strip() for p in input['properties']]

        # if we have a wildcard, get all the properties
        if len(properties) == 1 and properties[0] == '*':
            properties = list(property_map.keys())

        # list of the Pipedrive properties we'd like to query
        pipedrive_properties = [property_map[p] for p in properties]

        # see here for more info:
        # https://developers.pipedrive.com/docs/api/v1/#!/Deals/get_deals
        url = api_base_uri + '/v1/deals'
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
        deals = content.get('data',[])
        for deal in deals:
            row = []
            for p in pipedrive_properties:
                row.append(deal.get(p,'') or '')
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

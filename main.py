#!/usr/bin/python3
import json
import logging
import logging.config
import sys
import time
from datetime import datetime
from typing import Dict, List

from pydantic import BaseSettings
from requests_ratelimiter import LimiterSession


class Settings(BaseSettings):
    internal_api_url: str = 'http://127.0.0.1:8000'
    internal_api_username: str = 'admin'
    internal_api_password: str = 'password'
    coach_sequence_api_url: str = 'https://bahn.expert'
    hafas_api_url: str = 'https://v5.db.transport.rest'
    query_when: str = 'now'
    query_duration: int = 60
    query_language: str = 'de'
    query_bus: bool = 'false'
    query_ferry: bool = 'false'
    query_subway: bool = 'false'
    query_tram: bool = 'false'
    query_taxi: bool = 'false'
    query_suburban: bool = 'false'
    query_regional: bool = 'false'
    query_regionalexp: bool = 'false'
    query_national: bool = 'true'
    query_nationalexpress: bool = 'true'
    query_stopovers: bool = 'true'
    query_pretty: bool = 'false'
    query_remarks: bool = 'true'
    query_polyline: bool = 'false'
    external_rate_limit: int = 90
    internal_rate_limit: int = 550
    debug: bool = 'true'


def setup_logging():

    streamHandler = logging.StreamHandler(sys.stdout)
    streamHandler.setFormatter(logging.Formatter(
        '%(levelname)s - %(message)s'))

    logging.getLogger().addHandler(streamHandler)
    logging.getLogger().setLevel(logging.INFO)


def get_departure_board(eva_number: int):
    """
  Get the arrival board for a given EVA number.

  This function sends a GET request to the HAFAS API using the provided EVA number and the settings specified in the Settings class. If the response is successful, it returns the JSON data from the response. Otherwise, it returns an empty list.

  Args:
    eva_number: The EVA number to get the arrival board for.

  Returns:
    A list containing the arrival board data, or an empty list if there was an error.
  """
    # Create the URL using the provided EVA number
    url = '{}/stops/{}/departures'.format(
        Settings().hafas_api_url, eva_number)

    # Create the parameters dictionary with all the settings values
    params = {
        'duration': Settings().query_duration,
        'language': Settings().query_language,
        'bus': Settings().query_bus,
        'ferry': Settings().query_ferry,
        'subway': Settings().query_subway,
        'tram': Settings().query_tram,
        'taxi': Settings().query_taxi,
        'suburban': Settings().query_suburban,
        'regional': Settings().query_regional,
        'regionalExp': Settings().query_regionalexp,
        'national': Settings().query_national,
        'nationalExpress': Settings().query_nationalexpress,
        'stopovers': Settings().query_stopovers,
        'pretty': Settings().query_pretty,
        'remarks': Settings().query_remarks,
        'when': Settings().query_when
    }

    result = []

    try:
        # Send the GET request and get the response
        response = internal_session.get(url=url, params=params)

        # Log the request and response
        logging.debug('%s %s %s', response.request.method,
                      response.status_code, response.request.url)

        # If the response is successful, return the JSON data
        if response.ok:
            response = response.json()
            result = response
    except Exception as e:
        # Handle any errors that occurred while sending the request
        print('Unable to get url {} due to {}.'.format(url, e.__class__))

    return result


def get_arrival_board(eva_number: int):
    """
  Get the arrival board for a given EVA number.

  This function sends a GET request to the HAFAS API using the provided EVA number and the settings specified in the Settings class. If the response is successful, it returns the JSON data from the response. Otherwise, it returns an empty list.

  Args:
    eva_number: The EVA number to get the arrival board for.

  Returns:
    A list containing the arrival board data, or an empty list if there was an error.
  """

    # Create the URL using the provided EVA number
    url = '{}/stops/{}/arrivals'.format(
        Settings().hafas_api_url, eva_number)

    # Create the parameters dictionary with all the settings values
    params = {
        'duration': Settings().query_duration,
        'language': Settings().query_language,
        'bus': Settings().query_bus,
        'ferry': Settings().query_ferry,
        'subway': Settings().query_subway,
        'tram': Settings().query_tram,
        'taxi': Settings().query_taxi,
        'suburban': Settings().query_suburban,
        'regional': Settings().query_regional,
        'regionalExp': Settings().query_regionalexp,
        'national': Settings().query_national,
        'nationalExpress': Settings().query_nationalexpress,
        'stopovers': Settings().query_stopovers,
        'pretty': Settings().query_pretty,
        'remarks': Settings().query_remarks,
        'when': Settings().query_when
    }

    result = []

    try:
        # Send the GET request and get the response
        response = internal_session.get(url=url, params=params)

        # Log the request and response
        logging.debug('%s %s %s', response.request.method,
                      response.status_code, response.request.url)

        # If the response is successful, return the JSON data
        if response.ok:
            response = response.json()
            result = response
    except Exception as e:
        # Handle any errors that occurred while sending the request
        print('Unable to get url {} due to {}.'.format(url, e.__class__))

    return result


def get_composition(train_number: str, eva_number: int, departure: datetime):
    # .strftime('%Y-%m-%dT%H:%M%:00.000Z')
    formatted_departure_time = departure

    url = '{}/api/reihung/v4/wagen/{}'.format(
        Settings().coach_sequence_api_url, train_number)
    params = {
        'departure': formatted_departure_time,
        'evaNumber': eva_number
    }
    try:
        with external_session.get(url=url, params=params) as response:
            logging.debug('%s %s %s', response.request.method,
                          response.status_code, response.request.url)
            if response.ok:
                resp = response.json()
                if resp['sequence']:
                    if resp['sequence']['groups']:
                        return resp['sequence']['groups']
    except Exception as e:
        print('Unable to get url {} due to {}.'.format(url, e.__class__))


def get_train_trip(line_name: str, trip_id: str, stopovers: bool = True,
                   remarks: bool = True, polyline: bool = False, language: str = 'de') -> Dict:
    """
    Retrieves information about a train trip from the HAFAS API.

    This function makes a GET request to the HAFAS API with the specified line name and trip ID,
    and returns the API response as a JSON object. The request includes several query parameters
    to request additional information about the trip, such as stopovers, remarks, and the polyline.

    Args:
        line_name (str): The name of the train line for the trip.
        trip_id (str): The ID of the trip to retrieve information for.
        stopovers (bool): Include stopover information in the response.
        remarks (bool): Include remarks in the response.
        polyline (bool): Include the polyline in the response.
        language (str): The language for the response.

    Returns:
        dict: The JSON response from the HAFAS API, containing information about the trip.

    Raises:
        ConnectionError: If the request to the HAFAS API fails due to a connection error.
        TimeoutError: If the request to the HAFAS API times out.
    """

    # Set the URL for the HAFAS API with the specified trip ID
    url = '{}/trips/{}'.format(Settings().hafas_api_url, trip_id)

    # Set the query parameters for the request
    params = {
        'lineName': line_name,
        'stopovers': stopovers,
        'remarks': remarks,
        'polyline': polyline,
        'language': language
    }

    # Make the GET request to the HAFAS API
    try:
        with internal_session.get(url=url, params=params) as response:
            # Log the request method, response status code, and URL
            logging.debug('%s %s %s', response.request.method,
                          response.status_code, response.request.url)

            # If the response is successful, return the JSON response
            if response.ok:
                response = response.json()
                return response
    # Handle specific types of exceptions
    except ConnectionError as e:
        raise e
    except TimeoutError as e:
        raise e
    # Handle any other type of exception
    except Exception as e:
        raise e


def get_time_table(eva_number: str) -> List[Dict]:
    """
    Retrieves the departure and arrival boards for a given EVA number.

    This function retrieves the departure and arrival boards for a given EVA number,
    and combines them into a single list of dictionaries. It uses the `get_departure_board`
    and `get_arrival_board` functions to retrieve the departure and arrival boards,
    respectively.

    Args:
        eva_number (str): The EVA number of the stop to retrieve the time table for.

    Returns:
        list: A list of dictionaries containing the combined departure and arrival boards.
    """

    # Retrieve the departure and arrival boards for the given EVA number
    departure_board = get_departure_board(eva_number)
    arrival_board = get_arrival_board(eva_number)

    # Combine the departure and arrival boards into a single list
    time_table = [*departure_board, *arrival_board]

    return time_table


def get_station_list(usage: str) -> List[Dict]:
    """
    Retrieves a list of stations from the internal API.

    This function makes a GET request to the internal API with the specified `usage`
    parameter, and returns the list of stations from the API response. If the API
    response includes a "next" link, the function follows the link and retrieves
    additional pages of data until all pages have been retrieved.

    Args:
        usage (str): The usage parameter for the request to the internal API.

    Returns:
        list: A list of dictionaries containing information about the stations.
    """

    url = '{}/stations'.format(Settings().internal_api_url)
    params = {
        'usage': usage
    }

    try:
        with internal_session.get(url=url, params=params) as response:

            # If the response is successful, process the data
            if response.ok:

                # Create an empty list to store the data
                data = []

                # Get the "results" key from the response
                response = response.json()

                # Add the items from the "result" key to the list of data
                data.extend(response['results'])

                # Check if there is a "next" link in the response
                while response['next'] is not None:
                    # If there is, follow the "next" link to get the next page of data
                    url = response['next']
                    response = external_session.get(url)

                    logging.info('%s', url)

                    # If the response is successful, add the data to the list of data
                    if response.ok:
                        # Get the "results" key from the response
                        response = response.json()

                        # Add the items from the "result" key to the list of data
                        data.extend(response['results'])

            # If there is data
            if data:

                # Return the data
                return data
    except Exception as e:
        print('Unable to get url {} due to {}.'.format(url, e.__class__))


def get_or_create_operator(name: str):
    result = None
    url = '{}/operators/'.format(Settings().internal_api_url)
    params = {
        'name': name
    }
    with internal_session.get(url=url, params=params, auth=(Settings().internal_api_username, Settings().internal_api_password)) as response:
        logging.debug('%s %s %s', response.request.method,
                      response.status_code, response.request.url)
        if response.ok:
            response = response.json()
            if response['count'] > 0:
                result = response['results'][0]
                logging.debug('Operator %s was found with id %s.',
                              name, result['id'])
            else:
                data = {
                    'name': name
                }
                with internal_session.post(url=url, data=data, auth=(Settings().internal_api_username, Settings().internal_api_password)) as response:
                    logging.debug('%s %s %s', response.request.method,
                                  response.status_code, response.request.url)
                    if response.ok:
                        result = response.json()
                        logging.info(
                            'Operator %s was created with id %s.', name, result['id'])
                    else:
                        logging.error('%s', response.text)
        else:
            logging.error('%s', response.text)
    return result


def get_or_create_line(operator: dict, product: str, number: int, name: str):
    result = None
    url = '{}/lines/'.format(Settings().internal_api_url)
    params = {
        'operator': operator['id'],
        'product': product,
        'number': number,
    }
    with internal_session.get(url=url, params=params, auth=(Settings().internal_api_username, Settings().internal_api_password)) as response:
        logging.debug('%s %s %s', response.request.method,
                      response.status_code, response.request.url)
        if response.ok:
            response = response.json()
            if response['count'] > 0:
                result = response['results'][0]
                logging.debug('Line %s was found with id %s.',
                              name, result['id'])
            else:
                data = {
                    'number': number,
                    'name': name,
                    'product': product,
                    'operator': operator['url']
                }
                with internal_session.post(url=url, data=data, auth=(Settings().internal_api_username, Settings().internal_api_password)) as response:
                    logging.debug('%s %s %s', response.request.method,
                                  response.status_code, response.request.url)
                    if response.ok:
                        result = response.json()
                        logging.info(
                            'Line %s was created with id %s.', name, result['id'])
                    else:
                        logging.error('%s', response.text)
        else:
            logging.error('%s', response.text)
    return result


def get_or_create_station(eva_number: int, name: str, lng: float, lat: float):
    result = None
    url = '{}/stations/'.format(Settings().internal_api_url, eva_number)
    params = {
        'eva_number': eva_number,
    }
    with internal_session.get(url=url, params=params, auth=(Settings().internal_api_username, Settings().internal_api_password)) as response:
        logging.debug('%s %s %s', response.request.method,
                      response.status_code, response.request.url)
        if response.ok:
            response = response.json()
            if response['count'] > 0:
                result = response['results'][0]
                logging.debug('Station %s was found with id %s.',
                              name, result['id'])
            else:
                data = {
                    'eva_number': eva_number,
                    'name': name,
                    'lng': lng,
                    'lat': lat
                }
                with internal_session.post(url=url, data=data, auth=(Settings().internal_api_username, Settings().internal_api_password)) as response:
                    logging.debug('%s %s %s', response.request.method,
                                  response.status_code, response.request.url)
                    if response.ok:
                        result = response.json()
                        logging.info(
                            'Station %s was created with id %s.', name, result['id'])
                    else:
                        logging.error('%s', response.text)
        else:
            logging.error('%s', response.text)
    return result


def create_or_update_train(line: dict, trip_id: str, origin: dict, destination: dict, cancelled: bool = False, date: datetime = datetime.today()):
    result = None
    url = '{}/trains/'.format(Settings().internal_api_url)
    params = {
        'line': line['id'],
        'date': date.strftime('%Y-%m-%d'),
        'tripID': trip_id
    }
    with internal_session.get(url=url, params=params, auth=(Settings().internal_api_username, Settings().internal_api_password)) as response:
        logging.debug('%s %s %s', response.request.method,
                      response.status_code, response.request.url)
        if response.ok:
            response = response.json()
            if response['count'] > 0:
                result = response['results'][0]
                logging.debug('Train %s was found with id %s.',
                              line['name'], result['id'])
                if result['cancelled'] is not cancelled:
                    url = result['url']
                    data = {
                        'cancelled': cancelled,
                    }
                    with internal_session.patch(url=url, data=data, auth=(Settings().internal_api_username, Settings().internal_api_password)) as response:
                        logging.debug('%s %s %s', response.request.method,
                                      response.status_code, response.request.url)
                        if response.ok:
                            result = response.json()
                            logging.info(
                                'Train %s was updated with id %s.', line['name'], result['id'])
                        else:
                            logging.error('%s', response.text)
            else:
                data = {
                    'line': line['url'],
                    'name': '{} {}'.format(line['product'], line['number']),
                    'date': date.strftime('%Y-%m-%d'),
                    'trip_id': trip_id,
                    'cancelled': cancelled,
                    'origin': origin['url'],
                    'destination': destination['url']
                }
                with internal_session.post(url=url, data=data, auth=(Settings().internal_api_username, Settings().internal_api_password)) as response:
                    logging.debug('%s %s %s', response.request.method,
                                  response.status_code, response.request.url)
                    if response.ok:
                        result = response.json()
                        logging.info('Train %s was created with id %s.',
                                     line['name'], result['id'])
                    else:
                        logging.error('%s', response.text)
        else:
            logging.error('%s', response.text)
    return result


def create_or_update_stopover(idx: int, stopover: dict, train: dict):
    station = get_or_create_station(eva_number=stopover['stop']['id'], name=stopover['stop']['name'],
                                    lng=stopover['stop']['location']['longitude'], lat=stopover['stop']['location']['latitude'])
    result = None
    url = '{}/stopovers/'.format(Settings().internal_api_url)
    params = {
        'train': train['id'],
        'stop_index': idx,
    }
    with internal_session.get(url=url, params=params, auth=(Settings().internal_api_username, Settings().internal_api_password)) as response:
        logging.debug('%s %s %s', response.request.method,
                      response.status_code, response.request.url)
        if response.ok:
            response = response.json()
            if response['count'] > 0:
                result = response['results'][0]
                logging.debug('Stopover %s was found with id %s.',
                              idx, result['id'])

                # # Check if the 'arrival' or 'departure' key exists in the dictionary.
                # if 'arrival' in stopover or 'departure' in stopover:
                #     # If the 'arrival' key exists, check if its value is later than the current time.
                #     if 'arrival' in stopover:
                #         url = result['url']
                #         data = {
                #             'arrival_actual_time': (stopover['arrival'] if stopover['arrival'] is not None else None)
                #         }
                #         with internal_session.patch(url=url, data=data, auth=(Settings().internal_api_username, Settings().internal_api_password)) as response:
                #             logging.debug('%s %s %s', response.request.method,
                #                         response.status_code, response.request.url)
                #             if response.ok:
                #                 result = response.json()
                #                 logging.info(
                #                     'Stopopver %s was updated with id %s.', idx, result['id'])
                #             else:
                #                 logging.error('%s', response.text)
                    
                #     # If the 'departure' key exists, check if its value is later than the current time.
                #     if 'departure' in stopover:
                #         # The 'departure' time is later than the current time, so we can run the code.
                #         # (Replace this with the code you want to run.)
                #         url = result['url']
                #         data = {
                #             'departure_actual_time': (stopover['departure'] if stopover['departure'] is not None else None),
                #         }
                #         with internal_session.patch(url=url, data=data, auth=(Settings().internal_api_username, Settings().internal_api_password)) as response:
                #             logging.debug('%s %s %s', response.request.method,
                #                         response.status_code, response.request.url)
                #             if response.ok:
                #                 result = response.json()
                #                 logging.info(
                #                     'Stopopver %s was updated with id %s.', idx, result['id'])
                #             else:
                #                 logging.error('%s', response.text)
            else:
                data = {
                    'station': station['url'],
                    'stop_index': idx,
                    'train': train['url'],
                    'platform': (stopover['plannedDeparturePlatform'] if stopover['plannedDeparturePlatform'] is not None else None),
                    'departure_planned_time': (stopover['plannedDeparture'] if stopover['plannedDeparture'] is not None else None),
                    'departure_actual_time': (stopover['departure'] if stopover['departure'] is not None else None),
                    'arrival_planned_time': (stopover['plannedArrival'] if stopover['plannedArrival'] is not None else None),
                    'arrival_actual_time': (stopover['arrival'] if stopover['arrival'] is not None else None)
                }
                with internal_session.post(url=url, data=data, auth=(Settings().internal_api_username, Settings().internal_api_password)) as response:
                    logging.debug('%s %s %s', response.request.method,
                                  response.status_code, response.request.url)
                    if response.ok:
                        result = response.json()
                        logging.info('Stopopver %s was created with id %s.',
                                     idx, result['id'])
                    else:
                        logging.error('%s', response.text)
        else:
            logging.error('%s', response.text)
    return result


def get_or_create_remark(train: dict, message: str):
    result = None
    url = '{}/remarks/'.format(Settings().internal_api_url)
    params = {
        'train': train['id'],
        'message': message,
    }
    with internal_session.get(url=url, params=params, auth=(Settings().internal_api_username, Settings().internal_api_password)) as response:
        logging.debug('%s %s %s', response.request.method,
                      response.status_code, response.request.url)
        if response.ok:
            response = response.json()
            if response['count'] > 0:
                result = response['results'][0]
                logging.debug('Remark %s was found with id %s.',
                              message, result['id'])
            else:
                data = {
                    'train': train['url'],
                    'message': message,
                }
                with internal_session.post(url=url, data=data, auth=(Settings().internal_api_username, Settings().internal_api_password)) as response:
                    logging.debug('%s %s %s', response.request.method,
                                  response.status_code, response.request.url)
                    if response.ok:
                        result = response.json()
                        logging.info(
                            'Remark %s was created with id %s.', message, result['id'])
                    else:
                        logging.error('%s', response.text)
        else:
            logging.error('%s', response.text)
    return result


def get_or_create_composition(train: dict, composition: dict):
    result = None
    url = '{}/compositions/'.format(Settings().internal_api_url)
    params = {
        'train': train['id'],
    }
    with internal_session.get(url=url, params=params, auth=(Settings().internal_api_username, Settings().internal_api_password)) as response:
        logging.debug('%s %s %s', response.request.method,
                      response.status_code, response.request.url)
        if response.ok:
            response = response.json()
            if response['count'] > 0:
                result = response['results'][0]
                logging.debug('Composition for train %s was found with id %s.',
                              train['name'], result['id'])
            else:
                data = {
                    'train': train['url'],
                    'coach_sequence': json.dumps(composition),
                }
                with internal_session.post(url=url, data=data, auth=(Settings().internal_api_username, Settings().internal_api_password)) as response:
                    logging.debug('%s %s %s', response.request.method,
                                  response.status_code, response.request.url)
                    if response.ok:
                        result = response.json()
                        logging.info(
                            'Composition for train %ss was created with id %s.', train['name'], result['id'])
                    else:
                        logging.error('%s', response.text)
        else:
            logging.error('%s', response.text)
    return result


def main():
    start_time = datetime.now()
    logging.info('started execution')

    try:

        station_list = get_station_list(usage='FV')
        station_list_len = len(station_list)

        train_list = []

        for idx, station in enumerate(station_list, start=1):

            logging.info('%s / %s (%s)', idx,
                         station_list_len, station['name'])

            train_list.extend(get_time_table(eva_number=station['eva_number']))

        filtered_train_list = []

        filtered_train_list = [item for item in train_list if item.get(
            'tripID') not in filtered_train_list]

        filtered_train_list_len = len(filtered_train_list)

        for idx, item in enumerate(filtered_train_list, start=1):
            logging.info('%s / %s (%s)', idx,
                         filtered_train_list_len, item['line']['name'])

            train_details = get_train_trip(
                line_name=item['line']['name'], trip_id=item['tripId'])

            # Check if the operator of the train exists
            operator = get_or_create_operator(
                name=item['line']['operator']['name'])

            # Check if the line of the train exists
            line = get_or_create_line(
                operator=operator, product=item['line']['productName'], number=item['line']['fahrtNr'], name=item['line']['name'])
            pass

            # Check if the origin of the train exists
            origin = get_or_create_station(eva_number=train_details['origin']['id'], name=train_details['origin']['name'],
                                           lng=train_details['origin']['location']['longitude'], lat=train_details['origin']['location']['latitude'])

            # Check if the destitnation of the train exists
            destination = get_or_create_station(eva_number=train_details['destination']['id'], name=train_details['destination']['name'],
                                                lng=train_details['destination']['location']['longitude'], lat=train_details['destination']['location']['latitude'])

            # Create or update the train
            train = create_or_update_train(
                line=line, trip_id=item['tripId'], origin=origin, destination=destination)

            # Create all stopovers

            for idx, stopover in enumerate(train_details['stopovers']):
                if not 'cancelled' in stopover:
                    create_or_update_stopover(
                        idx=idx, stopover=stopover, train=train)

            # Create all remarks

            for idx, remark in enumerate(train_details['remarks']):
                get_or_create_remark(message=remark['text'], train=train)

            # Create the composition

            if not train['cancelled']:

                composition_data = get_composition(
                    train_number=line['number'], eva_number=train_details['origin']['id'], departure=train_details['stopovers'][0]['plannedDeparture'])
                if composition_data:
                    composition = get_or_create_composition(
                        train=train, composition=composition_data)
    except Exception as e:
        logging.error('%s', e)

    end_time = datetime.now()
    logging.info('finished execution. Duration %s', (end_time - start_time))


if __name__ == '__main__':

    setup_logging()
    logging.info('the script was started with the following configuration')
    for key, value in Settings().dict().items():
        if not 'password' in key:
            logging.info('%s : %s', key, value)
        else:
            logging.info('%s : NO_LOG', key)

    external_session = LimiterSession(
        per_minute=Settings().external_rate_limit)
    internal_session = LimiterSession(
        per_minute=Settings().internal_rate_limit)

    while True:
        main()
        time.sleep(60)
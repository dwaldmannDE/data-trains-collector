#!/usr/bin/python3
import json
import logging
import logging.config
import re
import sys
import time
from typing import List, Dict
from datetime import datetime, timedelta

import dateutil.parser
import schedule
from pydantic import BaseSettings
from pyrate_limiter.bucket import MemoryQueueBucket
from requests import Session as RSession
from requests_cache import CacheMixin
from requests_cache.backends import SQLiteCache
from requests_ratelimiter import LimiterMixin


class CachedLimiterSession(CacheMixin, LimiterMixin, RSession):
    """Session class with caching and rate-limiting behavior. Accepts arguments for both
         LimiterSession and CachedSession.  """


class Settings(BaseSettings):
    internal_base_url: str = 'https://django.kube-test.itdw.io'
    coach_sequence_base_url: str = 'https://bahn.expert'
    hafas_base_url: str = 'https://v5.db.transport.rest'
    query_when: str = 'now'
    query_duration: int = 480
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
    rate_limit: int = 90
    debug: bool = 'true'


def retry_function(func, *args, **kwargs):
    # Set the number of retries to 0
    retries = 0

    # Set the maximum number of retries
    max_retries = 2

    # Run the function indefinitely
    while True:
        try:
            # Call the function with the provided arguments
            return func(*args, **kwargs)
        except Error as e:
            # Increment the number of retries
            retries += 1

            # If the maximum number of retries has been reached, handle the error
            if retries >= max_retries:
                # Handle the error
                raise e
            else:
                # Continue to the next iteration of the loop and try the function again
                continue


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
        Settings().hafas_base_url, eva_number)

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
        response = http_session.get(url=url, params=params)

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
        Settings().hafas_base_url, eva_number)

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
        response = http_session.get(url=url, params=params)

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


def get_composition(train_number: str, eva_number: int, initial_departure: datetime, departure_time: datetime):
    formatted_initial_departure = initial_departure
    # .strftime('%Y-%m-%dT%H:%M%:00.000Z')
    formatted_departure_time = departure_time

    url = '{}/api/reihung/v4/wagen/{}'.format(Settings().api_url, train_number)
    params = {
        'initialDeparture': formatted_initial_departure,
        'departure': formatted_departure_time,
        'evaNumber': eva_number
    }
    try:
        with http_session.get(url=url, params=params) as response:
            logging.debug('%s %s %s', response.request.method,
                          response.status_code, response.request.url)
            if response.status_code == 200:
                resp = response.json()
                if resp['sequence']:
                    if resp['sequence']['groups']:
                        return resp['sequence']['groups']
    except Exception as e:
        print('Unable to get url {} due to {}.'.format(url, e.__class__))


def get_train_trip(line_name: str, trip_id: str, stopovers: bool = True,
                   remarks: bool = True, polyline: bool = True, language: str = 'de') -> Dict:
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
    url = '{}/trips/{}'.format(Settings().hafas_base_url, trip_id)

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
        with http_session.get(url=url, params=params) as response:
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
    departure_board = retry_function(get_departure_board, eva_number)
    arrival_board = retry_function(get_arrival_board, eva_number)

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

    url = '{}/stations'.format(Settings().internal_base_url)
    params = {
        'usage': usage
    }

    try:
        with http_session.get(url=url, params=params) as response:

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
                    response = http_session.get(url)

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


def create_or_update_train(train_details: dict):
    pass

    url = '{}/trains'.format(Settings().internal_base_url)
    params = {
        'journey_id': trip_id
    }

    try:
        with http_session.get(url=url, params=params) as response:
            if response.ok:
                response = response.json()
                pass
            else:
                pass

    except:
        pass


def main():
    start_time = datetime.now()
    logging.info('started execution')

    station_list = get_station_list(usage='FV')
    station_list_len = len(station_list)

    train_list = []

    for idx, station in enumerate(station_list, start=1):

        logging.info('%s / %s (%s)', idx, station_list_len, station['name'])

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

        create_or_update_train(train_details)

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

    http_session = CachedLimiterSession(
        per_minute=Settings().rate_limit,
        bucket_class=MemoryQueueBucket,
        backend=SQLiteCache(name='http_cache'),
        expire_after=timedelta(seconds=3600),
        cache_control=False,
        allowable_codes=[200]
    )

    schedule.every().minute.do(main)

    # schedule.every(10).minutes.do(job)
    # schedule.every().hour.do(job)
    # schedule.every().day.at("10:30").do(job)
    # schedule.every().monday.do(job)
    # schedule.every().wednesday.at("13:15").do(job)
    # schedule.every().minute.at(":17").do(job)

    main()

    while True:
        schedule.run_pending()
        time.sleep(60)

    # main()

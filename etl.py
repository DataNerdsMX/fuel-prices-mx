#!/usr/bin/python3
import os
import sys
import time
import pickle
import datetime
import logging
from collections import namedtuple
import requests


logger = logging.getLogger(__name__)
logging_format = '%(asctime)s|%(levelname)s|%(funcName)s: %(message)s'
logging_level = logging.DEBUG if os.environ.get('DEBUG', '').lower() == 'true' else logging.INFO
logging.basicConfig(stream=sys.stdout, level=logging_level, format=logging_format)


###############################################################################
# Globals

LOCATIONS_URL = 'http://api-catalogo.cre.gob.mx/api/utiles/municipios'
PRICES_URL = 'http://api-reportediario.cre.gob.mx/api/EstacionServicio/Petroliferos?entidadId={state_id}&municipioId={location_id}&_={timestamp}'
USER_AGENT = 'DataNerdsMX Gasolinazo 1.0'
HTTP_REQUESTS_SLEEP_TIME = 0.3


# New Relic
NR_ACCOUNT_ID = os.environ['NR_ACCOUNT_ID']
NR_INSIGHTS_INSERT_KEY = os.environ['NR_INSIGHTS_INSERT_KEY']
NR_INSIGHTS_API_URL = f'https://insights-collector.newrelic.com/v1/accounts/{NR_ACCOUNT_ID}/events'
NR_SLEEP_TIME_BETWEEN_POSTS = 1
NR_EVENTS_PER_POST = 1000
NR_EVENT_NAME = 'FuelPriceSample'


###############################################################################
# Code

Location = namedtuple('Location', 'location_id state_id state location')


FuelPriceRecord = namedtuple('FuelPriceRecord', 'location brand station type product price applied_at')


def get_rotated_file_path(file_path):
    components = file_path.split('.')
    components.insert(len(components) - 1, datetime.date.today().isoformat())
    return '.'.join(components)


def save_data(data, name):
    file_path = get_rotated_file_path(f'data/{name}.pickle')
    logger.info('Saving data to %s', file_path)
    with open(file_path, 'w+b') as file:
        pickle.dump(data, file)


def read_data(name):
    file_path = get_rotated_file_path(f'data/{name}.pickle')
    if not os.path.exists(file_path):
        return None
    logger.info('Reading data from %s', file_path)
    with open(file_path, 'rb') as file:
        return pickle.load(file)


def request(url):
    headers = {'User-Agent': USER_AGENT}
    response = requests.get(url, headers=headers)
    logger.debug(response)
    logger.debug('Content: %s...', response.content[:100])
    return response


def get_location_key(r):
    location_id = r['MunicipioId']
    state_id = r['EntidadFederativaId']
    return (f'{state_id:0>2}', f'{location_id:0>3}')


def batch(elements, length=1):
    l = len(elements)
    for ndx in range(0, l, length):
        yield elements[ndx:min(ndx + length, l)]


def import_locations():
    logger.info('Getting the locations catalog...')
    locations = {}
    response = request(LOCATIONS_URL)
    for r in response.json():
        state_id, location_id = get_location_key(r)
        location = Location(
            location_id=location_id,
            state_id=state_id,
            state=r['EntidadFederativa']['Nombre'],
            location=r['Nombre'])
        locations[(state_id, location_id)] = location
    logger.debug(locations)
    save_data(locations, 'locations')
    logger.info('...Done')
    return locations


def import_prices():
    locations = read_data('locations') or import_locations()
    logger.info('Getting the prices for each location...')
    records = []
    totals = {'locations_missing_data': 0, 'locations_processed': 0, 'stations_processed': 0}
    for location in locations.values():
        logger.debug(location)
        timestamp = int(datetime.datetime.now().timestamp())
        params = dict(state_id=location.state_id, location_id=location.location_id, timestamp=timestamp)
        response = request(PRICES_URL.format(**params))
        logger.debug(response.content)
        json_records = response.json()
        if not json_records:
            logger.warn('No data for location %s', location)
            totals['locations_missing_data'] += 1
            continue
        for r in json_records:
            applied_at = datetime.datetime.strptime(r['FechaAplicacion'], '%Y-%m-%dT%H:%M:%S')
            fuel_price_record = FuelPriceRecord(
                location=locations[get_location_key(r)],
                brand=r['Marca'],
                station=r['Nombre'],
                type='gasoline' if r['Producto'] == 'Gasolinas' else 'diesel',
                product=r['SubProducto'].split()[0],
                price=float(r['PrecioVigente']),
                applied_at=int(applied_at.timestamp()))
            totals['stations_processed'] += 1
            logger.debug(fuel_price_record)
            records.append(fuel_price_record)
        totals['locations_processed'] += 1
        time.sleep(HTTP_REQUESTS_SLEEP_TIME)
    save_data(records, 'prices')
    logger.info('...Done')
    logger.info(totals)
    return records


def post_events_to_insights(events):
    headers = {
        'Content-Type': 'application/json',
        'X-Insert-Key': NR_INSIGHTS_INSERT_KEY}
    response = requests.post(NR_INSIGHTS_API_URL, json=events, headers=headers)
    logger.debug(response)
    time.sleep(NR_SLEEP_TIME_BETWEEN_POSTS)
    success = response.status_code == 200
    if not success:
        logger.warn(response.content)
    return success


def export_prices():
    all_prices = read_data('prices') or import_prices()
    logger.info('Exporting the prices for each location in batches...')
    counter = 0
    errors = 0
    for prices in batch(all_prices, length=NR_EVENTS_PER_POST):
        events = []
        for price in prices:
            logger.debug(price)
            event = {
                'eventType': NR_EVENT_NAME,
                'location': price.location.location,
                'state': price.location.state,
                'brand': price.brand,
                'station': price.station,
                'type': price.type,
                'product': price.product,
                'price': price.price,
                'applied_at': price.applied_at}
            events.append(event)
        events_in_this_batch = len(events)
        success = post_events_to_insights(events)
        if success:
            counter += events_in_this_batch
        else:
            errors += events_in_this_batch
    logger.info('...Done')
    logger.info('Events inserted: %s', counter)
    logger.info('Events NOT inserted: %s', errors)


def main():
    if not NR_ACCOUNT_ID or not NR_INSIGHTS_INSERT_KEY:
        raise EnvironmentError('Missing keys in .env')
    export_prices()


###############################################################################
# Unit tests

def test_get_location_key():
    test = [
        (('02', '012'), {'MunicipioId':  '12', 'EntidadFederativaId':  '2'}),
        (('02', '012'), {'MunicipioId': '012', 'EntidadFederativaId': '02'}),
        (('40', '103'), {'MunicipioId': '103', 'EntidadFederativaId': '40'})]
    for expected, given in test:
        assert expected == get_location_key(given)

def test_get_rotated_file_path():
    today = datetime.date.today()
    test = [
        (f'foo.bar/baz.{today}.pickle', 'foo.bar/baz.pickle'),
        (f'foo.bar/foo.bar.{today}.pickle', 'foo.bar/foo.bar.pickle')]
    for expected, given in test:
        assert expected == get_rotated_file_path(given)

def test_batch():
    test = [
        ([[1, 2, 3], [4, 5, 6]],          [1, 2, 3, 4, 5, 6],        3),
        ([[1], [2], [3], [4], [5], [6]],  [1, 2, 3, 4, 5, 6],        1),
        ([['a', 'b'], ['c', 'd'], ['e']], ['a', 'b', 'c', 'd', 'e'], 2)]
    for expected, given, length in test:
        assert expected == list(batch(given, length=length))


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.error(f'\n\n******* {e} *******\n')
        exit(1)
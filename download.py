"""
Download images from planet.com API
"""

import configparser
import json
import queue
import time
import multiprocessing as mp
import os
import socket
import re
import requests
from requests.auth import HTTPBasicAuth

CONFIG = configparser.ConfigParser()
CONFIG.read('config.ini')

ITEM_TYPE = "PSScene4Band"


def download_file(url, file_name):
    """
    Download final file
    """
    with requests.get(url, stream=True) as result:
        result.raise_for_status()
        with open(file_name, 'wb') as file_write:
            for chunk in file_write.iter_content(chunk_size=8192):
                if chunk:
                    file_write.write(chunk)


def download(queue_active_assets):
    """
    Download assets from active assets queue
    """
    while True:
        try:
            item_id, section, asset_type, link = queue_active_assets.get(False)
        except queue.Empty:
            time.sleep(1)
            continue
        print("Got active asset: {} {}".format(item_id, asset_type))
        result = requests.head(
            link,
            auth=HTTPBasicAuth(CONFIG['DEFAULT']['API_KEY'], ''))
        dispo = result.headers['content-disposition']
        fname = re.findall("filename=(.+)", dispo)
        download_file(link, os.path.join(CONFIG[section]['download'], fname))


def is_active(queue_inactive_assets, queue_active_assets):
    """
    Check if inactive assets are activated, then download
    """
    print("Start active checking process.")
    while True:
        try:
            item_id, section, asset_type, timestamp = (
                queue_inactive_assets.get(False))
        except queue.Empty:
            time.sleep(1)
            continue
        if timestamp < time.time() + 180:
            # back to queue
            queue_inactive_assets.put((item_id, section,
                                       asset_type, timestamp))
        active, link = check_active_asset(item_id, asset_type)
        if active:
            queue_active_assets.put((item_id, section, asset_type, link))
        queue_inactive_assets.put((item_id, section, asset_type, timestamp))


def check_active_asset(item_id, asset_type):
    """
    Check if asset is active
    """
    url = ('https://api.planet.com/data/v1/item-types/{}/items/{}/assets'
           .format(ITEM_TYPE, item_id))
    try:
        result = requests.get(
            url, timeout=10,
            auth=HTTPBasicAuth(CONFIG['DEFAULT']['API_KEY'], ''))
    except socket.timeout:
        print("URL {} timed out.".format(url))
        return (None, None)
    assets_result_json = result.json()
    if not assets_result_json:
        return (None, None)
    if assets_result_json[asset_type]['status'] == "active":
        return (True, assets_result_json[asset_type]['location'])
    return (False, assets_result_json[asset_type]['_links']['activate'])


def activate(queue_item_ids, queue_inactive_assets, queue_active_assets):
    """
    If asset is inactive, activate and put on queue_inactive_assets.
    Otherweise put on queue_active_assets.
    """
    print("Starting activation process.")
    while True:
        try:
            item_id, section = queue_item_ids.get(False)
        except queue.Empty:
            time.sleep(1)
            continue
        for asset_type in ["analytic", "analytic_xml"]:
            print("Checking asset {} in {}".format(asset_type, item_id))
            active, link = check_active_asset(item_id, asset_type)
            if active is None:
                print("{} contains no valid asset.".format(item_id))
                continue
            elif not active:
                print("activating {}".format(link))
                activation_result = requests.get(
                    link,
                    auth=HTTPBasicAuth(CONFIG['DEFAULT']['API_KEY'], ''))
                print("Activating {}: {}".format(item_id,
                                                 activation_result.text))
                queue_inactive_assets.put((item_id, section,
                                           asset_type, time.time()))
            else:
                print("Queuing active asset: {}".format(item_id))
                queue_active_assets.put((item_id, section, asset_type, link))


def search_query(geojson_geometry, section):
    """
    Renders the data on the planet.com server
    """
    # get images that overlap with our area of interest
    geometry_filter = {
        "type": "GeometryFilter",
        "field_name": "geometry",
        "config": geojson_geometry
    }

    # get images acquired within a date range
    date_range_filter = {
        "type": "DateRangeFilter",
        "field_name": "acquired",
        "config": {
            "gte": CONFIG[section]['from'] + "T00:00:00.000Z",
            "lte": CONFIG[section]['to'] + "T00:00:00.000Z"
        }
    }

    # only get images which have <50% cloud coverage
    cloud_cover_filter = {
        "type": "RangeFilter",
        "field_name": "cloud_cover",
        "config": {
            "lte": float(CONFIG[section]['cloud_limit'])
        }
    }

    # combine our geo, date, cloud filters
    combined_filter = {
        "type": "AndFilter",
        "config": [geometry_filter, date_range_filter, cloud_cover_filter]
    }
    search_request = {
        "interval": "day",
        "item_types": [ITEM_TYPE],
        "filter": combined_filter
    }
    return search_request


def search(queue_item_ids, section, search_request=None, next_link=None):
    """
    Get search results from API
    """
    if next_link:
        print("Next link {}".format(next_link))
        search_result = requests.get(
            next_link,
            auth=HTTPBasicAuth(CONFIG['DEFAULT']['API_KEY'], ''))
        return  # Todo: remove
    else:
        print("Search query")
        search_result = requests.post(
            'https://api.planet.com/data/v1/quick-search',
            auth=HTTPBasicAuth(CONFIG['DEFAULT']['API_KEY'], ''),
            json=search_request)
    search_result_json = search_result.json()
    for feature in search_result_json['features']:
        queue_item_ids.put((feature['id'], section))
        print("Adding feature id: {}".format(feature['id']))
    if search_result_json['_links']['_next']:
        search(queue_item_ids, section,
               next_link=search_result_json['_links']['_next'])


def load_ids(queue_item_ids):
    """
    Wrap API search
    """
    for section in CONFIG:
        if section == "DEFAULT":
            continue
        with open(CONFIG[section]['geojson']) as json_file:
            geojson_geometry = json.load(json_file)
        search_request = search_query(geojson_geometry, section)
        search(queue_item_ids, section, search_request=search_request)


def main():
    """
    Spawn worker processes
    """
    mp.set_start_method('spawn')
    queue_item_ids = mp.Queue()  # item_id
    queue_inactive_assets = mp.Queue()  # (item_id, asset_type, timestamp)
    queue_active_assets = mp.Queue()  # (item_id, asset_type)
    p_load_ids = mp.Process(target=load_ids, args=(queue_item_ids,))
    p_load_ids.start()
    p_activate = mp.Process(target=activate, args=(queue_item_ids,
                                                   queue_inactive_assets,
                                                   queue_active_assets,))
    p_activate.start()
    p_is_active = mp.Process(target=is_active, args=(queue_inactive_assets,
                                                     queue_active_assets,))
    p_is_active.start()
    p_download = mp.Process(target=download, args=(queue_active_assets,))
    p_download.start()
    p_load_ids.join()
    p_activate.join()
    p_is_active.join()
    p_download.join()


if __name__ == '__main__':
    main()

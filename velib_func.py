import requests
import pandas as pd
from datetime import datetime
import re


def load_data(url):
    """
    Function to query API velib
    :param url: url to query
    :return: API response, and others informations
    """
    assert isinstance(url, str), 'Argument of wrong type!'
    try:
        result_query = requests.get(url)
        result_query = result_query.json()
        update_time = result_query['lastUpdatedOther']
        return pd.DataFrame.from_dict(result_query['data']['stations']), datetime.fromtimestamp(update_time)
    except:
        print('Wrong url')


def del_bracket(x):
    """
    Function to delete bracket
    :param x: character with bracket
    :return: character without bracket
    """
    x = re.sub('[^a-zA-Z]', '', str(x))
    x = re.sub("^\[|\]$", "", x)
    return x


def find_num_bikes(x):
    """
    Function to find num bike.
    :param x: A character to find numver
    :return:
    """
    numbers = re.findall('[0-9]+', x)
    value = f"{numbers[0]},{numbers[1]}"
    assert isinstance(value, str)
    return value

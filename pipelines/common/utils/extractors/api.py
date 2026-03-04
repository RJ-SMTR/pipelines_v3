# -*- coding: utf-8 -*-
"""Module to get data from APIs"""

import time
from typing import Union

import requests

from pipelines.common import constants
from pipelines.common.utils.fs import save_local_file


def get_api_data(
    url: str,
    headers: Union[None, dict] = None,
    params: Union[None, dict] = None,
    raw_filetype: str = "json",
) -> Union[str, dict, list[dict]]:
    """
    Get data from a single API endpoint.

    Args:
        url (str): API endpoint URL
        headers (Union[None, dict]): Request headers
        params (Union[None, dict]): Request parameters
        raw_filetype (str): File type for response (json, csv, etc.)

    Returns:
        Union[str, dict, list[dict]]: API response data
    """

    for retry in range(constants.MAX_RETRIES):
        response = requests.get(
            url,
            headers=headers,
            timeout=constants.MAX_TIMEOUT_SECONDS,
            params=params,
        )

        if response.ok:
            break
        if response.status_code >= constants.HTTP_SERVER_ERROR_STATUS:
            print(f"Server error {response.status_code}")
            if retry == constants.MAX_RETRIES - 1:
                response.raise_for_status()
            time.sleep(60)
        else:
            response.raise_for_status()

    if raw_filetype == "json":
        data = response.json()
    else:
        data = response.text

    return data


def get_raw_api(
    url: str,
    raw_filepath: str,
    headers: Union[None, dict] = None,
    params: Union[None, dict] = None,
    raw_filetype: str = "json",
) -> list[str]:
    """
    Get data from a single API endpoint and save to a local file.

    Args:
        url (str): API endpoint URL
        raw_filepath (str): File path template with {page} placeholder
        headers (Union[None, dict]): Request headers
        params (Union[None, dict]): Request parameters
        raw_filetype (str): File type for response (json, csv, etc.)

    Returns:
        list[str]: List with the path where data was saved
    """
    data = get_api_data(url=url, headers=headers, params=params, raw_filetype=raw_filetype)
    filepath = raw_filepath.format(page=0)
    save_local_file(filepath=filepath, filetype=raw_filetype, data=data)
    return [filepath]


def get_raw_api_list(
    url: Union[str, list[str]],
    raw_filepath: str,
    params_list: Union[None, list[dict]] = None,
    headers: Union[None, dict] = None,
) -> list[str]:
    """
    Get data from API by aggregating multiple calls and save to a local file.

    Args:
        url (str or list[str]): API endpoint URL(s)
        raw_filepath (str): File path template with {page} placeholder
        params_list (list[dict]): List of parameter dicts for multiple requests
        headers (Union[None, dict]): Request headers

    Returns:
        list[str]: List with the path where data was saved
    """
    data = []
    if isinstance(url, list):
        for single_url in url:
            page_data = get_api_data(url=single_url, headers=headers, raw_filetype="json")
            data += page_data
    else:
        if params_list is None:
            raise ValueError(
                "When 'url' is a string, 'params_list' must be provided. "
                "For a single API call without parameters, use 'get_raw_api'."
            )

        for params in params_list:
            page_data = get_api_data(url=url, headers=headers, params=params, raw_filetype="json")
            data += page_data

    filepath = raw_filepath.format(page=0)
    save_local_file(filepath=filepath, filetype="json", data=data)
    return [filepath]

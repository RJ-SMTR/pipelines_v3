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
    timeout: Union[None, int] = constants.MAX_TIMEOUT_SECONDS,
) -> Union[str, dict, list[dict]]:
    """
    Get data from a single API endpoint.

    Args:
        url (str): API endpoint URL
        headers (Union[None, dict]): Request headers
        params (Union[None, dict]): Request parameters
        raw_filetype (str): File type for response (json, csv, etc.)
        timeout (Union[None, int]): Request timeout in seconds. Defaults to MAX_TIMEOUT_SECONDS.

    Returns:
        Union[str, dict, list[dict]]: API response data
    """

    for retry in range(constants.MAX_RETRIES):
        response = requests.get(
            url,
            headers=headers,
            timeout=timeout,
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


def get_raw_api(  # noqa: PLR0913
    url: str,
    raw_filepath: str,
    headers: Union[None, dict] = None,
    params: Union[None, dict] = None,
    raw_filetype: str = "json",
    response_key: Union[None, str] = None,
) -> list[str]:
    """
    Get data from a single API endpoint and save to a local file.

    Args:
        url (str): API endpoint URL
        raw_filepath (str): File path template with {page} placeholder
        headers (Union[None, dict]): Request headers
        params (Union[None, dict]): Request parameters
        raw_filetype (str): File type for response (json, csv, etc.)
        response_key (Union[None, str]): If set, extracts data[response_key] before saving

    Returns:
        list[str]: List with the path where data was saved
    """
    data = get_api_data(url=url, headers=headers, params=params, raw_filetype=raw_filetype)
    if response_key is not None:
        data = data[response_key]
    filepath = raw_filepath.format(page=0)
    save_local_file(filepath=filepath, filetype=raw_filetype, data=data)
    return [filepath]


def get_raw_api_paginated(  # noqa: PLR0913
    url: str,
    raw_filepath: str,
    page_param_name: str,
    page_size_param_name: str,
    page_size: int,
    params: dict,
    headers: Union[None, dict] = None,
    response_key: Union[None, str] = None,
    first_page: int = 0,
) -> list[str]:
    """Get data from a page-number paginated API and save each page locally.

    Args:
        url (str): API endpoint URL.
        raw_filepath (str): File path template with a ``{page}`` placeholder.
        page_param_name (str): Name of the page-number query parameter.
        page_size_param_name (str): Name of the page-size query parameter.
        page_size (int): Maximum number of records requested per page.
        headers (Union[None, dict]): Request headers.
        params (dict): Additional request parameters.
        response_key (Union[None, str]): Key containing the records when the response is an object.
        first_page (int): First page number accepted by the API. Defaults to 0.

    Returns:
        list[str]: Paths of the saved page files.
    """
    if page_size <= 0:
        raise ValueError("page_size must be greater than zero")

    filepaths = []
    page_index = 0
    page_data_len = page_size

    while page_data_len == page_size:
        current_page = first_page + page_index
        page_params = {
            **params,
            page_param_name: current_page,
            page_size_param_name: page_size,
        }
        response_data = get_api_data(
            url=url,
            headers=headers,
            params=page_params,
            raw_filetype="json",
        )
        page_data = response_data[response_key] if response_key is not None else response_data
        if not isinstance(page_data, list):
            raise ValueError("Paginated API response must contain a list of records")

        page_data_len = len(page_data)
        print(
            f"Page size: {page_size}\n"
            f"Current page: {current_page}\n"
            f"Current page returned {page_data_len} rows"
        )

        filepath = raw_filepath.format(page=page_index)
        save_local_file(filepath=filepath, filetype="json", data=page_data)
        filepaths.append(filepath)

        if page_data_len < page_size:
            print("Last page, ending extraction")
        page_index += 1

    return filepaths


def get_raw_api_list(
    url: Union[str, list[str]],
    raw_filepath: str,
    params_list: Union[None, list[dict]] = None,
    headers: Union[None, dict] = None,
    timeout: Union[None, int] = constants.MAX_TIMEOUT_SECONDS,
) -> list[str]:
    """
    Get data from API by aggregating multiple calls and save to a local file.

    Args:
        url (str or list[str]): API endpoint URL(s)
        raw_filepath (str): File path template with {page} placeholder
        params_list (list[dict]): List of parameter dicts for multiple requests
        headers (Union[None, dict]): Request headers
        timeout (Union[None, int]): Request timeout in seconds. Defaults to MAX_TIMEOUT_SECONDS.

    Returns:
        list[str]: List with the path where data was saved
    """
    data = []
    if isinstance(url, list):
        for single_url in url:
            page_data = get_api_data(
                url=single_url, headers=headers, raw_filetype="json", timeout=timeout
            )
            data += page_data
    else:
        if params_list is None:
            raise ValueError(
                "When 'url' is a string, 'params_list' must be provided. "
                "For a single API call without parameters, use 'get_raw_api'."
            )

        for params in params_list:
            page_data = get_api_data(
                url=url, headers=headers, params=params, raw_filetype="json", timeout=timeout
            )
            data += page_data

    filepath = raw_filepath.format(page=0)
    save_local_file(filepath=filepath, filetype="json", data=data)
    return [filepath]

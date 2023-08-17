import os, sys
import requests
from time import sleep
from datetime import date
import logging
from typing import Dict, Optional, Tuple, Literal, Union
from enum import Enum
import xxhash
from bs4 import BeautifulSoup
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.cosmos import CosmosClient


def initialize_session() -> requests.Session :
    # initialize session
    session = requests.Session()
    # allow bad ssl and turn off warnings
    session.verify = False
    requests.packages.urllib3.disable_warnings(
        requests.packages.urllib3.exceptions.InsecureRequestWarning
    )
    return session


def initialize_blob_container_client(container_name) -> ContainerClient:
    blob_connection_str = os.getenv("ScrapeDataStorage")
    blob_service_client: BlobServiceClient = BlobServiceClient.from_connection_string(
        blob_connection_str
    )
    container_client = blob_service_client.get_container_client(container_name)
    return container_client


def initialize_cosmos_db_client(container_name):
    cosmos_connection_str = os.getenv("AzureCosmosStorage")
    cosmos_service_client: CosmosClient = CosmosClient.from_connection_string(
        cosmos_connection_str
    )
    cosmos_db_client = cosmos_service_client.get_database_client("cases-json-db")
    container_client = cosmos_db_client.get_container_client(container_name)
    return container_client


def write_debug_and_raise(
    page_text: str, verification_text: Optional[str] = None
) -> None:
    error_msg: str = (
        (
            f"{verification_text} could not be found in page."
            if verification_text
            else "Failed to load page."
        )
        + f" Aborting. Writing /data/debug.html with response. May not be HTML."
    )

    with open(os.path.join("data", "debug.html"), "w") as file_handle:
        file_handle.write(page_text)
    
    logging.error(error_msg)
    raise Exception(error_msg)


# helper function to make form data
def create_search_form_data(
    date: str, JO_id: str, hidden_values: Dict[str, str], odyssey_version: int
) -> Dict[str, str]:
    form_data = {}
    form_data.update(hidden_values)
    if odyssey_version < 2017:
        form_data.update(
            {
                "SearchBy": "3",
                "cboJudOffc": JO_id,
                "DateSettingOnAfter": date,
                "DateSettingOnBefore": date,
                "SearchType": "JUDOFFC",  # Search by Judicial Officer
                "SearchMode": "JUDOFFC",
                "CaseCategories": "CR",  # "CR,CV,FAM,PR" criminal, civil, family, probate and mental health - these are the options
            }
        )
    else:
        form_data.update(
            {
                "SearchCriteria.SelectedHearingType": "Criminal Hearing Types",
                "SearchCriteria.SearchByType": "JudicialOfficer",
                "SearchCriteria.SelectedJudicialOfficer": JO_id,
                "SearchCriteria.DateFrom": date,
                "SearchCriteria.DateTo": date,
            }
        )
    return form_data


class HTTPMethod(Enum):
    POST = 1
    GET = 2


def request_page_with_retry(
    session: requests.Session,
    url: str,
    verification_text: Optional[str] = None,
    http_method: HTTPMethod = HTTPMethod.POST,
    params: Dict[str, str] = {},
    data: Optional[Dict[str, str]] = None,
    max_retries: int = 5,
    ms_wait: int = 200,
) -> str:
    response: Union[requests.Response, None] = None
    i: int = 0

    while True:
        logging.info(f"Making {http_method.name} request to url {url}...")
        response = session.request(
            method=http_method.name,
            url=url,
            params=params,
            data=data,
        )
        if response.ok:
            break

        logging.exception(f"Try {i} - failed to {http_method.name} url {url} with code {response.status_code}.")
        sleep(ms_wait / 1000 * (i + 1))
        i += 1

        if i > max_retries:
            write_debug_and_raise(
                verification_text=verification_text,
                page_text=response.text,
            )

    if verification_text and verification_text not in response.text:
        write_debug_and_raise(
            verification_text=verification_text,
            page_text=response.text,
        )

    return response.text

def create_single_case_search_form_data(hidden_values: Dict[str, str], case_number: str):
    form_data = {}
    form_data.update(hidden_values)
    os_specific_time_format = "%#m/%#d/%Y" if os.name == 'nt' else "%-m/%-d/%Y"
    form_data.update(
        {
            "__EVENTTARGET":"",
            "SearchBy": "0",
            "DateSettingOnAfter": "1/1/1970",
            "DateSettingOnBefore": date.today().strftime(os_specific_time_format),
            "SearchType": "CASE",  # Search by case id
            "SearchMode": "CASENUMBER",
            "CourtCaseSearchValue": case_number,
            "CaseCategories": "",
            "cboJudOffc":"38501",
        }
    )
    return form_data

def write_string_to_blob(
    file_contents: str, blob_name: str, container_client, container_name: str, overwrite: bool = False
) -> bool:
    """Write a string to a blob file. If

    Args:
        file_contents (str): String to be written as body of the file
        blob_name (str): name of the file
        overwrite (bool, optional): If False, checks if file exists first. Defaults to False.
    Returns:
        bool: True if file written, False if not written
    """
    blob_client = container_client.get_blob_client(blob_name)
    if blob_client.exists() and not overwrite:
        logging.info(msg=f"{blob_name} already exists in {container_name}, skipping.")
        return False
    blob_client.upload_blob(data=file_contents)
    return True


def hash_case_html(file_contents: str) -> dict:
    """Return the xxhash of a given string of the html of one case, cleaned to only relevant parts

    Args:
        file_contents (str): String of the html file to be processed

    Returns:
        dict: dict with keys 'hash' and 'case_no'
    """
    soup = BeautifulSoup(file_contents, "html.parser", from_encoding="UTF-8")
    # Extract county case number
    case_no = soup.select('div[class="ssCaseDetailCaseNbr"] > span')[0].text
    body = soup.find("body")
    balance_table = body.find_all("table")[-1]
    if "Balance Due" in balance_table.text:
        balance_table.decompose()
    relevant_file_str = str(body)
    filehash = xxhash.xxh64(relevant_file_str).hexdigest()
    return {"file_hash": filehash, "case_no": case_no}

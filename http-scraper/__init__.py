import logging, os, csv, urllib.parse, json, requests
from typing import List, Any, Optional
from datetime import datetime, timedelta, date
from time import time

from bs4 import BeautifulSoup
import azure.functions as func
from azure.storage.blob import ContainerClient


from shared.helpers import *

# Environment variables that MUST be set when running the function.
# If you forget to set them, function will fail loudly (crash).
CONTAINER_NAME_HTML: str = os.environ["blob_container_name_html"]
CASE_BATCH_SIZE: int = int(os.environ["cases_batch_size"])

# Cache expensive computation for potential re-use
# https://learn.microsoft.com/en-us/azure/azure-functions/functions-reference-python#global-variables
SESSION : Optional[requests.Session] = None
CONTAINER_CLIENT_HTML : Optional[ContainerClient] = None

def main(req: func.HttpRequest, msg: func.Out[List[str]]) -> func.HttpResponse:
    logging.info("Python HTTP trigger function received a request.")

    req_body: Any = req.get_json()
    # Get parameters from request payload
    # TODO - seeing as how this will be running in a context where we want to keep time < 2-15 min,
    # and run many in parallel,
    # do we still need the 'back-up' start/end times (based off today) and 'backup' county ("hays")?
    # May be better to simple require certain parameters?
    # Are we worried about Odyssey noticing a ton of parallel requests?

    start_date: date = date.fromisoformat(
        req_body.get("start_date", (date.today() - timedelta(days=1)).isoformat())
    )
    end_date: date = date.fromisoformat(req_body.get("end_date", date.today().isoformat()))
    county: str = req_body.get("county", "hays")
    judicial_officers: List[str] = req_body.get("judicial_officers", [])
    ms_wait = int(req_body.get("ms_wait", "200"))
    log_level: str = req_body.get("log_level", "INFO")
    court_calendar_link_text: str = req_body.get("court_calendar_link_text", "Court Calendar")
    location: Optional[str] = req_body.get("location", None)
    test: Optional[str] = req_body.get("test", None)
    is_test = bool(test)

    # Get/initialize blob container client for sending html files to
    global CONTAINER_CLIENT_HTML
    if CONTAINER_CLIENT_HTML == None:
        CONTAINER_CLIENT_HTML = initialize_blob_container_client(CONTAINER_NAME_HTML)

    # Get/initialize session
    global SESSION
    if SESSION == None:
        SESSION = initialize_session()
    
    # initialize logger
    logger: logging.Logger = logging.getLogger(name="pid: " + str(os.getpid()))
    logging.root.setLevel(level=log_level)

    # make cache directories if not present
    case_html_path: str = os.path.join(
        os.path.dirname(__file__), "..", "data", county, "case_html"
    )
    os.makedirs(case_html_path, exist_ok=True)

    # get county portal and version year information from csv file
    base_url, odyssey_version, notes = parse_csv(
        county_to_find=county,
        logger=logger,
    )

    # determine the court calendar URL
    search_url: str = find_search_url(
        county=county, 
        base_url=base_url, 
        odyssey_version=odyssey_version, 
        notes=notes, 
        court_calendar_link_text=court_calendar_link_text, 
        session=SESSION, 
        ms_wait=ms_wait,
        logger=logger,
    )
    search_verification_text: str = "Court Calendar" if odyssey_version < 2017 else "SearchCriteria.SelectedCourt"

    # hit the search page to gather initial data
    search_page_html: str = request_page_with_retry(
        session=SESSION,
        url=search_url,
        verification_text=search_verification_text,
        http_method=HTTPMethod.GET,
        ms_wait=ms_wait,
    )
    search_soup = BeautifulSoup(search_page_html, "html.parser")

    # we need these hidden values to POST a search
    hidden_values = {
        hidden["name"]: hidden["value"]
        for hidden in search_soup.select('input[type="hidden"]')
        if hidden.has_attr("name")
    }
    # get nodedesc and nodeid information from main page location select box
    if odyssey_version < 2017:
        location_option = main_soup.findAll("option")[0]
        logger.info(f"location: {location_option.text}")
        hidden_values.update({"NodeDesc": location, "NodeID": location_option["value"]})
    else:
        hidden_values["SearchCriteria.SelectedCourt"] = hidden_values[
            "Settings.DefaultLocation"
        ]  # TODO: Search in default court. Might need to add further logic later to loop through courts.

    # get a list of JOs to their IDs from the search page
    judicial_officer_to_ID = {
        option.text: option["value"]
        for option in search_soup.select(
            'select[labelname="Judicial Officer:"] > option'
            if odyssey_version < 2017
            else 'select[id="selHSJudicialOfficer"] > option'
        )
        if option.text
    }
    # if judicial_officers param is not specified, use all of them
    if not judicial_officers:
        judicial_officers = list(judicial_officer_to_ID.keys())

    # initialize variables to time script and build a list of already scraped cases
    START_TIME = time()

    # loop through each day
    for day in (
        start_date + timedelta(n) for n in range((end_date - start_date).days + 1)
    ):
        date_string = datetime.strftime(day, "%m/%d/%Y")
        # Need underscore since azure treats slashes as new files
        date_string_underscore = datetime.strftime(day, "%m_%d_%Y")

        # loop through each judicial officer
        for JO_name in judicial_officers:
            if JO_name not in judicial_officer_to_ID:
                logger.error(
                    f"judicial officer {JO_name} not found on search page. Continuing."
                )
                continue
            JO_id = judicial_officer_to_ID[JO_name]
            logger.info(f"Searching cases on {date_string} for {JO_name}")
            # POST a request for search results
            results_page_html = request_page_with_retry(
                session=SESSION,
                url=search_url
                if odyssey_version < 2017
                else urllib.parse.urljoin(
                    base_url, "Hearing/SearchHearings/HearingSearch"
                ),
                verification_text="Record Count"
                if odyssey_version < 2017
                else "Search Results",
                data=create_search_form_data(
                    date_string, JO_id, hidden_values, odyssey_version
                ),
                ms_wait=ms_wait,
            )
            results_soup = BeautifulSoup(results_page_html, "html.parser")

            # different process for getting case data for pre and post 2017 Odyssey versions
            if odyssey_version < 2017:
                case_urls = [
                    base_url + anchor["href"]
                    for anchor in results_soup.select('a[href^="CaseDetail"]')
                ]

                logger.info(f"{len(case_urls)} cases found")

                # if there are 10 or less cases, or it's a test run, just scrape now
                if len(case_urls) <= 10 or is_test:
                    for case_url in case_urls:
                        case_id = case_url.split("=")[1]
                        logger.info(f"{case_id} - scraping case")
                        # make request for the case
                        case_html = request_page_with_retry(
                            session=SESSION,
                            url=case_url,
                            verification_text="Date Filed",
                            ms_wait=ms_wait,
                        )
                        # write html case data
                        logger.info(f"{len(case_html)} response string length")
                        file_hash_dict = hash_case_html(case_html)
                        blob_name = f"{file_hash_dict['case_no']}:{county}:{date_string_underscore}:{file_hash_dict['file_hash']}.html"
                        logger.info(f"Sending {blob_name} to {CONTAINER_NAME_HTML} container...")
                        write_string_to_blob(file_contents=case_html, blob_name=blob_name, container_client=CONTAINER_CLIENT_HTML, container_name=CONTAINER_NAME_HTML)
                        if is_test:
                            logger.info("Testing, stopping after first case")
                            # bail
                            return
                
                # else if more than 10 cases, put them on message queue in batches to avoid function timeout
                # 1 batch of cases = 1 message on queue
                else:
                    messages = []
                    for i in range(0, len(case_urls), cases_batch_size):
                        message_dict = {
                            "case-urls": case_urls[i:i+cases_batch_size],
                            "scrape-params": {
                                'search-url': search_url,
                                'base-url': base_url,
                                'county': county,
                                'odyssey-version': odyssey_version,
                                'notes': notes,
                                'date-string': date_string,
                                'date-string-underscore': date_string_underscore,
                                'JO-id': JO_id,
                                'hidden-values': hidden_values,
                                'ms-wait': ms_wait,
                                'location': location  
                            }
                        }
                        message = json.dumps(message_dict)
                        messages.append(message)
                    logger.info(f"Writing {len(messages)} batches to message queue")
                    # put array of messages on queue - expects array of strings 
                    msg.set(messages)

            # else if odyssey version > 2017 
            else:
                # Need to POST this page to get a JSON of the search results after the initial POST
                case_list_json = request_page_with_retry(
                    session=SESSION,
                    url=urllib.parse.urljoin(base_url, "Hearing/HearingResults/Read"),
                    verification_text="AggregateResults",
                )
                case_list_json = json.loads(case_list_json)
                logger.info(f"{case_list_json['Total']} cases found")
                for case_json in case_list_json["Data"]:
                    case_id = str(case_json["CaseId"])
                    logger.info(f"{case_id} scraping case")
                    # make request for the case
                    case_html = request_page_with_retry(
                        session=SESSION,
                        url=urllib.parse.urljoin(base_url, "Case/CaseDetail"),
                        verification_text="Case Information",
                        ms_wait=ms_wait,
                        params={
                            "eid": case_json["EncryptedCaseId"],
                            "CaseNumber": case_json["CaseNumber"],
                        },
                    )
                    # make request for financial info
                    case_html += request_page_with_retry(
                        session=SESSION,
                        url=urllib.parse.urljoin(
                            base_url, "Case/CaseDetail/LoadFinancialInformation"
                        ),
                        verification_text="Financial",
                        ms_wait=ms_wait,
                        params={
                            "caseId": case_json["CaseId"],
                        },
                    )
                    # write case html data
                    logger.info(f"{len(case_html)} response string length")
                    # write to blob
                    file_hash_dict = hash_case_html(case_html)
                    blob_name = f"{file_hash_dict['case_no']}:{county}:{date_string_underscore}:{file_hash_dict['file_hash']}.html"
                    logger.info(f"Sending {blob_name} to blob...")
                    write_string_to_blob(file_contents=case_html, blob_name=blob_name, container_client=CONTAINER_CLIENT_HTML, container_name=CONTAINER_NAME_HTML)
                    if is_test:
                        logger.info("Testing, stopping after first case")
                        return

    logger.info(f"\nTime to run script: {round(time() - START_TIME, 2)} seconds")

    print("Returning response...")
    return func.HttpResponse(
        f"Finished scraping cases for {judicial_officers} in {county} from {start_date} to {end_date}",
        status_code=200,
    )

def parse_csv(county_to_find: str, logger: logging.Logger) -> tuple[str, int, str]:
    """
    Parses portal metadata from the static CSV for the county we are parsing.

    :returns: 
        - base_url - the base url of the portal for the county
        - odyssey_version - the version that the portal for the county uses
        - notes - any additional encoded text for the portal
    """
    with open(
        os.path.join(
            os.path.dirname(__file__), "..", "resources", "texas_county_data.csv"
        ),
        mode="r",
    ) as file_handle:
        csv_file = csv.DictReader(file_handle)
        for row in csv_file:
            if row["county"].lower() == county_to_find.lower():
                base_url: str = row["portal"]
                # add trailing slash if not present, otherwise urljoin breaks
                if base_url[-1] != "/":
                    base_url += "/"

                odyssey_version: int = int(row["version"].split(".")[0])
                notes: str = row["notes"]
                logger.info(f"{base_url} - scraping this url")

                return base_url, odyssey_version, notes
            
    raise Exception(
        f"The required data to scrape this county is not in ./resources/texas_county_data.csv, could not find county = {county_to_find}"
    )

def maybe_login(county: str, base_url: str, notes: str, session: requests.Session, ms_wait: int) -> None:
    """
    Tries to login to the portal with the username and password provided in CSV notes.
    """
    if "PUBLICLOGIN#" not in notes:
        return
    
    notes_split: list[str] = notes.split("#")
    if len(notes_split) < 3:
        raise Exception(
            f"For {county}, the NOTES section is malformed. Expected at least 2 `#` signs in {notes}."
        )

    userpass_split: list[str] = notes_split[1].split("/")
    if len(userpass_split) != 2:
        raise Exception(
            f"For {county}, the NOTES section is malformed. Expected exactly 1 `/` sign in {notes}."
        )

    data: dict[str, str] = {
        "UserName": userpass_split[0],
        "Password": userpass_split[1],
        "ValidateUser": "1",
        "dbKeyAuth": "Justice",
        "SignOn": "Sign On",
    }

    request_page_with_retry(
        session=session,
        url=urllib.parse.urljoin(base_url, "login.aspx"),
        http_method=HTTPMethod.GET,
        ms_wait=ms_wait,
        data=data,
    )

def find_search_url(county: str, base_url: str, odyssey_version: int, notes: str,
                    court_calendar_link_text: str, session: requests.Session, 
                    ms_wait: int, logger: logging.Logger) -> str:
    """
    Finds the URL to search the court calendar.
    Depending on the odyssey version, we may need to make a few requests
    to arrive at the search page.

    :returns: search_url: The URL to the court calendar search page.
    """
    if odyssey_version >= 2017:
        # Newer portal versions have a static search URL, no need to find it.
        return urllib.parse.urljoin(base_url, "Home/Dashboard/26")
    
    # Scrape main page first to get necessary data.
    # Some sites have a public guest login that must be used.
    maybe_login(county=county, base_url=base_url, notes=notes, session=session, ms_wait=ms_wait)

    # After login, get main page
    main_page_html: str = request_page_with_retry(
        session=session,
        url=base_url,
        verification_text="ssSearchHyperlink",
        http_method=HTTPMethod.GET,
        ms_wait=ms_wait,
    )
        
    main_soup = BeautifulSoup(main_page_html, "html.parser")
    for link in main_soup.select("a.ssSearchHyperlink"):
        if court_calendar_link_text in link.text:
            # build url for court calendar
            logger.info(f"Looking for court calendar URL in link: {link}")
            search_page_id: str = link["href"].split("?ID=")[1].split("'")[0]
            search_url: str = base_url + "Search.aspx?ID=" + search_page_id
            return search_url
        
    write_debug_and_raise(
        verification_text="Court Calendar link",
        page_text=main_page_html,
    )
    return '' # doesn't matter
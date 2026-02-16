#!/usr/bin/python

# Copyright: (c) 2026, Conrad Mercer <momercers@gmail.com>
#                      David Villafaña <david.villafana@capcu.org>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
from __future__ import absolute_import, division, print_function
from dataclasses import dataclass
from dataclasses import astuple
from ansible.module_utils.basic import AnsibleModule
from typing import Callable, Any
import requests
import logging
import os
import base64
from datetime import datetime
import fitz
import csv

__metaclass__ = type

DOCUMENTATION = r"""
---
module: get_portfolio_report

short_description: Calls the FICS Mortgage Servicer special services API to generate a document containing all the Portfolio Report.

# If this is part of a collection, you need to use semantic versioning,
# i.e. the version is of the form "2.5.0" and not "2.4".
version_added: "3.7.0"

description:
    - Calls the FICS Mortgage Servicer special services API to create the New Loans Entered Report file at the specified destination. 
    - Disclaimer: this module has only been tested for our exact use case

author:
    - Conrad Mercer

requirements: [ ]

options:
    dest:
        description: This is the full path to where the file will be created, it creates parent directories if they do not exist
        required: true
        type: str
    fics_api_url:
        description: This is the URL of the special service API
        required: true
        type: str
    api_token:
        description: this is the api token used for authentication to the API
        required: true
        type: str
    api_update_database:
        description: this is a bool which either Updates the system or not. We set it to false when testing
        required: true
        type: bool
    api_log_directory:
        description: this is the directory that the API logs will be created in
        required: false
        type: str
"""

EXAMPLES = r"""
- name: create file to send
  get_portfolio_report:
    dest: /mnt/fics_deliq/IT/Backups/fics/portfolio_report_2026-02-07
    fics_api_url: http://mortgageservicer.fics/MortgageServicerService.svc/REST/
    api_token: ASDFASDFJSDFSHFJJSDGFSJGQWEUI123123SDFSDFJ12312801C15034264BC98B33619F4A547AECBDD412D46A24D2560D5EFDD8DEDFE74325DC2E7B156C60B942
    api_log_directory: /tmp/api_logs/
"""

RETURN = r"""
msg:
    description: The result message of the download operation
    type: str
    returned: always
    sample: '"Wrote files to /mnt/fics_deliq/IT/Backups/fics/new_loans_entered_report-2026-02-07"'
changed:
    description: Whether any local files were changed
    type: bool
    returned: always
    sample: true
api_response:
    description: Document{Base64encoded document}
    type: str
    returned: always
"""

@dataclass(frozen=True)
class bbox:
    x0: float
    y0: float
    x: float
    y: float
    word: str
    block: int
    line: int
    word_no: int

@dataclass
class PdfLine:
    linenum: int
    list[bbox]


@dataclass
class page_representation:
    lines: list[PdfLine]

def log_function_call(log_path: str, func: Callable[..., Any], *args, **kwargs) -> Any:
    # Ensure the directory for the log file exists
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    # Set up logging
    logger = logging.getLogger(func.__name__)
    logger.setLevel(logging.INFO)

    # Create a file handler
    handler = logging.FileHandler(f"{log_path}/api_calls.log")
    handler.setLevel(logging.INFO)

    # Create a logging format
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)

    try:
        # Log the function call and its arguments
        logger.info(f"Calling {func.__name__}")
        logger.info(f"Args: {args}")
        logger.info(f"Kwargs: {kwargs}")

        # Call the function and get the result
        result = func(*args, **kwargs)

        # Log the function's return value
        logger.info(f"Result: {result}")

        return result

    except Exception as e:
        logger.exception(f"Exception occurred: {str(e)}")
        raise

    finally:
        # Remove the handler to avoid duplicate logs in future calls
        logger.removeHandler(handler)


def call_api(base_url: str, method: str, endpoint: str, parameters: dict):
    # Define the headers (if required)
    headers = {
        "Content-Type": "application/json",  # Adjust the content type as needed
    }

    # Send the POST request

    http: dict = {
        "post": requests.post,
        "get": requests.get,
        "put": requests.put,
        "delete": requests.delete,
    }
    response = http[method](base_url + endpoint, json=parameters, headers=headers)

    # Capture the response
    if response.status_code == 200:
        return response.json()
    else:
        print(
            f"Error response code ({response.status_code}) from api call: {response.text}"
        )
        return None


def get_new_loans_entered_report(
    api_url: str, 
    api_token: str,
    api_update_database: bool,
    api_log_directory: str,
) -> dict:
    params: dict = {
        "Message":{
            "PrintCoupons": False,
            "UpdateDatabase": api_update_database,
            "SystemDate": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "Token": api_token,
        }
    }
    return log_function_call(
        api_log_directory,
        call_api,
        base_url=api_url,
        method="post",
        endpoint="CreateNewLoansEnteredReport",
        parameters=params,
    )

def sort_bboxes_into_lines():
    print("do stuff")


def pdf_to_csv(pdf_path: str, csv_path:str):
    doc = fitz.open(pdf_path)
    all_rows = []

    for page_num in range(len(doc)):
        page = doc[page_num]
        text = page.get_text("words")
        bboxes = [bbox(*x) for x in text]
        for b in bboxes:
            all_rows.append(list(astuple(b)))

    doc.close()
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(all_rows)


def run_module():
    module_args = dict(
        pdf_dest=dict(type="str", required=True, no_log=False),
        csv_dest=dict(type="str", required=True, no_log=False),
        fics_api_url=dict(type="str", required=True, no_log=False),
        api_token=dict(type="str", required=True, no_log=True),
        api_update_database=dict(type="bool", required=True, no_log=False),
        api_log_directory=dict(type="str", required=False, no_log=False),
    )    

    # seed the result dict in the object
    # we primarily care about changed and state
    # changed is if this module effectively modified the target
    # state will include any data that you want your module to pass back
    # for consumption, for example, in a subsequent task
    result = dict(changed=False, msg="", failed=False, api_response={})

    # the AnsibleModule object will be our abstraction working with Ansible
    # this includes instantiation, a couple of common attr would be the
    # args/params passed to the execution, as well as if the module
    # supports check mode
    module = AnsibleModule(argument_spec=module_args, supports_check_mode=False)

    pdf_dest: str = module.params["pdf_dest"]
    csv_dest: str = module.params["csv_dest"]
    api_url: str = module.params["fics_api_url"]
    api_token: str = module.params["api_token"]
    api_update_database: bool = module.params["api_update_database"]
    api_log_directory: str = module.params["api_log_directory"]

    # if the user is working with this module in only check mode we do not
    # want to make any changes to the environment, just return the current
    # state with no modifications
    if module.check_mode:
        module.exit_json(**result)

    trial_resp: dict = get_new_loans_entered_report(
        api_url=api_url, 
        api_token=api_token,
        api_update_database=api_update_database,
        api_log_directory=api_log_directory,
    )

    if trial_resp is None:
        module.fail_json(
            msg="API call returned no response (check HTTP status code in logs)",
            changed=False,
            failed=True,
    )

    try:
        if trial_resp.get("ApiCallSuccessful", None):
            try:
                os.makedirs(name=str(os.path.dirname(pdf_dest)), exist_ok=True)
            except Exception as e:
                module.fail_json(
                    msg=f"failed to create parent directories: {e}",
                    changed=False,
                    failed=True,
                )
            base64_file = None
            doc_collection = trial_resp.get("DocumentCollection", [])
            for doc in doc_collection:
                if doc.get("Name") == "NewLoansEnteredReport":
                    base64_file = doc.get("DocumentBase64")

            if base64_file:
                new_loans_entered = base64.b64decode(base64_file)
                with open(module.params["pdf_dest"], "wb") as new_loans_entered_file:
                    new_loans_entered_file.write(new_loans_entered)

                try:
                    pdf_to_csv(pdf_dest, csv_dest)
                    result["changed"] = True
                    result["failed"] = False
                    result["msg"] = f"Wrote file at {module.params['dest']}"
                    result["api_response"] = trial_resp

                except Exception as e:
                    module.fail_json(
                        msg=f"Wrote PDF at {module.params['pdf_dest']} but failed to write CSV: {type(e).__name__}: {str(e)}",
                        changed=False,
                        failed=True,
                        api_response=trial_resp,
                    )
                    
            else:
                result["failed"] = True
                result["msg"] = "no report file found in api response!"
                result["api_response"] = trial_resp

        else:
            module.fail_json(
                msg="API call unsuccessful",
                changed=False,
                failed=True,
                api_response=trial_resp,
            )

    except Exception as e:
        module.fail_json(msg=f"failed to create file: {e}", changed=False, failed=True)

    # in the event of a successful module execution, you will want to
    # simple AnsibleModule.exit_json(), passing the key/value results
    module.exit_json(**result)


if __name__ == "__main__":
    run_module()

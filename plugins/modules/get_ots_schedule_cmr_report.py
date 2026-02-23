#!/usr/bin/python

# Copyright: (c) 2026, Conrad Mercer <momercers@gmail.com>
#                      David Villafaña <david.villafana@capcu.org>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
from __future__ import absolute_import, division, print_function
from sqlite3 import Row
from ansible.module_utils.basic import AnsibleModule
from typing import Callable, Any
import requests
import logging
import os
import base64
from datetime import datetime
import io
import csv
import re
import fitz

__metaclass__ = type

DOCUMENTATION = r"""
---
module: get_ots_schedule_cmr_report

short_description: Calls the FICS Mortgage Servicer special services API to generate a document containing all the OTS Schedule CMRs.

# If this is part of a collection, you need to use semantic versioning,
# i.e. the version is of the form "2.5.0" and not "2.4".
version_added: "3.5.0"

description:
    - Calls the FICS Mortgage Servicer special services API to create the OTS Schedule CMR file at the specified destination. 
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
    api_due_date:
        description: this is the date the application is due
        required: true
        type: str
    api_log_directory:
        description: this is the directory that the API logs will be created in
        required: false
        type: str

"""

EXAMPLES = r"""
- name: create file to send
  get_ots_schedule_cmr_report:
    dest: /mnt/fics_deliq/IT/Backups/fics/ots_schedule_CMRs_reports_2026-02-07
    fics_api_url: http://mortgageservicer.fics/MortageServicerService.svc/REST/
    api_token: ASDFASDFJSDFSHFJJSDGFSJGQWEUI123123SDFSDFJ12312801C15034264BC98B33619F4A547AECBDD412D46A24D2560D5EFDD8DEDFE74325DC2E7B156C60B942
    api_log_directory: /tmp/api_logs/
"""

RETURN = r"""
msg:
    description: The result message of the download operation
    type: str
    returned: always
    sample: '"Wrote files to /mnt/fics_deliq/IT/Backups/fics/ots_schedule_CMRs_reports_2026-02-07"'
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


def call_api(
    base_url: str, 
    method: str, 
    endpoint: str, 
    parameters: dict,
    api_token: str
):
    headers = {
        "Content-Type": "application/json",  # Adjust the content type as needed
        "X-API-KEY": api_token
    }

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

#
# def convert_pdf_to_csv(pdf_bytes: bytes) -> list:
#     """Extract table rows from PDF"""
#     try:
#         with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
#             all_rows = []
#             
#             for page in pdf.pages:
#                 text = page.extract_text()
#                 if not text:
#                     continue
#
#                 lines = text.split('\n')
#
#                 for line in lines:
#                     if not line.strip():
#                         continue
#
#                     row = re.split(r'  +', line.strip())
#                     row = [cell.strip() for cell in row if cell.strip()]
#
#                     if row:
#                         all_rows.append(row)
#                         
#             return all_rows
#             
#     except Exception as e:
#         raise Exception(f"PDF conversion error: {str(e)}")


def get_ots_schedule_cmr_report(
    api_url: str, 
    api_token: str, 
    api_log_directory: str,
) -> dict:
    # params: dict = {
    #     "Message":{
    #         "SystemDate": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    #         "Token": api_token,
    #     }
    # }
    params: dict = {
        "user_id": "api_user"
    }

    return log_function_call(
        api_log_directory,
        call_api,
        base_url=api_url,
        method="post",
        endpoint="/api/get_ots_report/",
        parameters=params,
        api_token=api_token
    )


def generate_csv(response, dest):
    results = sorted(response["results"], key=lambda x: x['inv_group_cd'])
    all_rows = []

    title_rows = [''] * 10
    title_rows[0] = "Num Loans"
    title_rows[1] = "Investor Name"
    title_rows[2] = "Bk-Inv-Grp"
    title_rows[3] = "Total Balances"
    title_rows[4] = "Avg Rem Term"
    title_rows[5] = "Years"
    title_rows[6] = "Avg Int Rate"
    title_rows[8] = "Combined Avg Years"
    title_rows[9] = "Combined Avg Int Rate"
    all_rows.append(title_rows)

    built_rows = {}
    for entry in results:
        rows = [''] * 10
        rows[0] = entry['num_loans']
        rows[1] = entry['inv_name']
        rows[2] = f"{entry['inv_bank_cd']}-{entry['inv_cd']}-{entry['inv_group_cd']}"
        rows[3] = entry['balances']
        rows[4] = entry['rem_term']
        rows[5] = round(entry['years'], 3)
        rows[6] = round(entry['int_rate'], 3)
        built_rows[rows[2]] = rows 

    year10_100: str = '1-100-10'
    year10_200: str = '1-200-10'

    if year10_100 in built_rows and year10_200 in built_rows:
        avg_years = round ((built_rows[year10_100][5] + built_rows[year10_200][5])/2,3)
        built_rows[year10_200][8] = avg_years

    year15_100: str = '1-100-15'
    year15_200: str = '1-200-15'

    if year15_100 in built_rows and year15_200 in built_rows:
        avg_years = round ((built_rows[year15_100][5] + built_rows[year15_200][5])/2,3)
        built_rows[year15_200][8] = avg_years


    with open(dest, 'w', newline='', encoding='utf-8') as dest:
        writer = csv.writer(dest)
        writer.writerows(all_rows)


def run_module():
    module_args = dict(
        dest=dict(type="str", required=True, no_log=False),
        custom_api_url=dict(type="str", required=True, no_log=False),
        api_token=dict(type="str", required=True, no_log=True),
        api_log_directory=dict(type="str", required=False, no_log=False)
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

    api_url: str = module.params["custom_api_url"]
    api_token: str = module.params["api_token"]
    api_log_directory: str = module.params["api_log_directory"]
    dest: str = module.params["dest"]

    # if the user is working with this module in only check mode we do not
    # want to make any changes to the environment, just return the current
    # state with no modifications
    if module.check_mode:
        module.exit_json(**result)

    trial_resp: dict = get_ots_schedule_cmr_report(
        api_url=api_url, 
        api_token=api_token, 
        api_log_directory=api_log_directory,
    )

    if trial_resp is None:
        module.fail_json(
            msg="API call returned no response (check HTTP status code in logs)",
            changed=False,
            failed=True,
    )

    try:
        if trial_resp:
            try:
                os.makedirs(name=str(os.path.dirname(dest)), exist_ok=True)
            except Exception as e:
                module.fail_json(
                    msg=f"failed to create parent directories: {e}",
                    changed=False,
                    failed=True,
                )

            try:
                generate_csv( trial_resp, dest)
                result["changed"] = True
                result["failed"] = False
                result["msg"] = f"Wrote CSV at {module.params['dest']}"
                result["api_response"] = trial_resp

            except Exception as e:
                module.fail_json(
                    msg=f"Failed to write CSV: {type(e).__name__}: {str(e)}",
                    changed=False,
                    failed=True,
                    api_response=trial_resp,
                )

        else:
            module.fail_json(
                msg="API call unsuccessful",
                changed=False,
                failed=True,
                api_response=trial_resp,
            )
            # base64_file = trial_resp.get("Document", {}).get("DocumentBase64", None)
            # if base64_file:
            #     ots_schedule_cmr_report = base64.b64decode(base64_file)
            #     
            #     try:
            #         csv_rows = convert_pdf_to_csv(ots_schedule_cmr_report)
            #
            #         if not csv_rows:
            #             module.fail_json(
            #             msg="No tables found in PDF",
            #             changed=False,
            #             failed=True,
            #             )
            #
            #         with open(module.params["dest"], "w", newline='', encoding='utf-8') as f:
            #             writer = csv.writer(f)
            #             writer.writerows(csv_rows)
            #             
            #         result["changed"] = True
            #         result["failed"] = False
            #         result["msg"] = f"Wrote CSV file with {len(csv_rows)} rows at {module.params['dest']}"
            #         result["api_response"] = trial_resp
            #
            #     except Exception as e:
            #         module.fail_json(
            #         msg=f"Failed to convert PDF to CSV: {str(e)}",
            #         changed=False,
            #         failed=True,
            #     )

    except Exception as e:
        module.fail_json(msg=f"failed to create file: {e}", changed=False, failed=True)

    module.exit_json(**result)


if __name__ == "__main__":
    run_module()

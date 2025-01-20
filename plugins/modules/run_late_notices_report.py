#!/usr/bin/python

# Copyright: (c) 2024, David Villafaña <david.villafana@capcu.org>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
from __future__ import absolute_import, division, print_function
from ansible.module_utils.basic import AnsibleModule
from typing import Callable, Any
from datetime import datetime, timedelta
import requests
import logging
import os
import base64

__metaclass__ = type

DOCUMENTATION = r"""
---
module: run_late_notices_report

short_description: Calls the FICS Mortgage Servicer batch service API to create the late notices report and summary report PDFs at the desired locations

# If this is part of a collection, you need to use semantic versioning,
# i.e. the version is of the form "2.5.0" and not "2.4".
version_added: "1.0.7"

description:
    - Calls the FICS Mortgage Servicer special services API to create the trial balance report pdf at the desired location
    - Disclaimer, this module has only been tested for our exact use case

author:
    - David Villafaña IV

requirements:
     - logging >= 0.4.9.6
     - requests >= 2.32.3

options:
    dest:
        description: This is the full path to where the late notices file will be created, it creates parent directories if they do not exist
        required: true
        type: str
    summary_dest:
        description: This is the full path to where the late notices summary file will be created, it creates parent directories if they do not exist
        required: true
        type: str
    batch_service_api_url:
        description: This is the URL of the desired API
        required: true
        type: str
    api_token:
        description: this is the api token used for authentication to the API
        required: true
        type: str
    api_log_directory:
        description: this is the directory that the API logs will be created in
        required: false
        type: str
"""

EXAMPLES = r"""
- name: Run the Late Notices Report via FICS API
  run_late_notices_report:
    dest: /mnt/fics/Mortgage Services/etc/late_notices.pdf
    summary_dest: /mnt/fics/Mortgage Services/etc/late_notices_summary.pdf
    batch_service_api_url: http://mortgageservicer.fics/BatchService.svc/REST/
    api_token: ASDFASDFJSDFSHFJJSDGFSJGQWEUI123123SDFSDFJ12312801C15034264BC98B33619F4A547AECBDD412D46A24D2560D5EFDD8DEDFE74325DC2E7B156C60B942
    api_log_directory: /mnt/fics/etc/api_logs/
"""

RETURN = r"""
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


def run_late_notices_report(api_log_directory: str, api_url: str, api_token: str, beginning_date: datetime, ending_date: datetime):
    params: dict = {
        "Message": {
            "BeginningDate": beginning_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "EndingDate": ending_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "PrintLateNoticesLetter": True,
            "UseLogo": True,
            "IncludeReturnedCheckChargeFees": True,
            "IncludeUnappliedBalance": True,
            "IncludeUnpaidLateCharges": True,
            "SelectedSortByType": 1,
            "IncludeFACTAct": True,
            "Token": api_token,
        }
    }
    return log_function_call(
        api_log_directory,
        call_api,
        base_url=api_url,
        method="post",
        endpoint="RunLateNoticesReport",
        parameters=params,
    )


def run_module():
    module_args = dict(
        dest=dict(type="str", required=True, no_log=False),
        summary_dest=dict(type="str", required=True, no_log=False),
        batch_service_api_url=dict(type="str", required=True, no_log=False),
        api_token=dict(type="str", required=True, no_log=True),
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

    api_url: str = module.params["batch_service_api_url"]
    api_token: str = module.params["api_token"]
    api_log_directory: str = module.params["api_log_directory"]
    dest: str = module.params["dest"]

    # if the user is working with this module in only check mode we do not
    # want to make any changes to the environment, just return the current
    # state with no modifications
    if module.check_mode:
        module.exit_json(**result)

    ending_date: datetime = datetime.now()
    beginning_date: datetime = ending_date - timedelta(days=365)
    late_notice_resp: dict = run_late_notices_report(
        api_url=api_url, api_token=api_token, api_log_directory=api_log_directory, beginning_date=beginning_date, ending_date=ending_date
    )

    try:
        if late_notice_resp.get("ApiCallSuccessful", None):
            try:
                os.makedirs(name=str(os.path.dirname(dest)), exist_ok=True)
            except Exception as e:
                module.fail_json(
                    msg=f"failed to create parent directories: {e}",
                    changed=False,
                    failed=True,
                )
            base64_late_notices_file = late_notice_resp.get("LateNotice", {}).get("Document", {}).get("DocumentBase64", None)
            base64_late_notice_summary_file = late_notice_resp.get("LateNoticeSummaryReport", {}).get("Document", {}).get("DocumentBase64", None)
            if base64_late_notices_file and base64_late_notice_summary_file:
                txt_data = base64.b64decode(base64_late_notices_file)
                with open(module.params["dest"], "wb") as txt_file:
                    txt_file.write(txt_data)
                txt_data = base64.b64decode(base64_late_notice_summary_file)
                with open(module.params["summary_dest"], "wb") as txt_file:
                    txt_file.write(txt_data)
                result["changed"] = True
                result["failed"] = False
                result["msg"] = f"Wrote file at {module.params['dest']}"
                result["api_response"] = late_notice_resp
            else:
                result["failed"] = True
                result["msg"] = "One or more files missing from api response!"
                result["api_response"] = late_notice_resp

        else:
            module.fail_json(
                msg="API call unsuccessful",
                changed=False,
                failed=True,
                api_response=late_notice_resp,
            )

    except Exception as e:
        module.fail_json(msg=f"failed to create file: {e}", changed=False, failed=True)

    # in the event of a successful module execution, you will want to
    # simple AnsibleModule.exit_json(), passing the key/value results
    module.exit_json(**result)


if __name__ == "__main__":
    run_module()

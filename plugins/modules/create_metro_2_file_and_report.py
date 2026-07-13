# Copyright: (c) 2024, David Villafaña <david.villafana@capcu.org>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
from __future__ import absolute_import, division, print_function
from ansible.module_utils.basic import AnsibleModule
from typing import Optional, Callable, Any
import requests
from datetime import datetime
import logging
import os
from urllib.parse import urljoin as join

__metaclass__ = type

DOCUMENTATION = r"""
---
module: create_metro_2_file_and_report

short_description: Calls the FICS Mortgage Servicer special services API to create the credit bureau files at the specified location

# If this is part of a collection, you need to use semantic versioning,
# i.e. the version is of the form "2.5.0" and not "2.4".
version_added: "1.0.0"

description:
    - Calls the FICS Mortgage Servicer special services API to create the credit burea files at the specified location
    - Disclaimer: this module has only been tested for our exact use case

author:
    - David Villafaña IV

requirements:
     - functools >= 0.5
     - logging >= 0.4.9.6
     - requests >= 2.32.3
     - datetime >= 5.5

options:
    api_url:
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
- name: create file to send
  create_metro_2_file_and_report:
    special_service_api_url: http://mortgageservicer.fics
    api_token: ASDFASDFJSDFSHFJJSDGFSJGQWEUI123123SDFSDFJ12312801C15034264BC98B33619F4A547AECBDD412D46A24D2560D5EFDD8DEDFE74325DC2E7B156C60B942
    api_log_directory: /mnt/fics/etc/archive/
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


def call_api(
    base_url: str, method: str, endpoint: str, parameters: dict, module: dict
) -> Optional[dict]:
    headers = {
        "Content-Type": "application/json",
    }

    if method == "post":
        response = requests.post(
            join(base_url, endpoint), json=parameters, headers=headers
        )
    elif method == "get":
        response = requests.get(
            join(base_url, endpoint), json=parameters, headers=headers
        )
    elif method == "put":
        response = requests.put(
            join(base_url, endpoint), json=parameters, headers=headers
        )
    elif method == "delete":
        response = requests.delete(
            join(base_url, endpoint), json=parameters, headers=headers
        )
    else:
        module.fail_json(
            msg=f"Invalid API method '{method}'", changed=False, failed=True
        )

    if response.status_code == 200:
        return response.json()
    else:
        module.fail_json(
            msg=f"Error response code ({response.status_code}) from api call: {response.text}",
            changed=False,
            failed=True,
        )


def create_metro_2_file_and_report(
    api_url: str,
    file_path: str,
    api_token: str,
    system_time: str,
    api_log_directory: str,
    module,
) -> dict:
    # TODO: yes I know that module should not be passed in as it makes the function more impure but I'll rewrite this to use exception handling or error return types in the future... maybe
    params = {
        "Message": {
            "IsEquifax": True,
            "IsExperian": True,
            "IsInnovis": False,
            "IsTransUnion": True,
            "IsCreateFileForConnect": True,
            "IsUpdate": True,
            "FilePath": file_path,
            "ApiParameters": "sample string",
            "SystemDate": system_time,
            "Token": api_token,
        },
        "SaveToRadstar": True,
    }
    # TODO: this call will save three files to the folder specified in desktop options, those files are
    return log_function_call(
        api_log_directory,
        call_api,
        base_url=api_url,
        method="post",
        endpoint="/BatchService.svc/REST/CreateMetro2FileAndReport",
        parameters=params,
        module=module,
    )


def get_ms_company_information(
    api_url: str, api_token: str, api_log_directory: str, module
) -> dict:
    # TODO: yes I know that module should not be passed in as it makes the function more impure but I'll rewrite this to use exception handling or error return types in the future... maybe
    params: dict = {"Message": {"Token": api_token}}
    return log_function_call(
        api_log_directory,
        call_api,
        base_url=api_url,
        method="post",
        endpoint="/MortgageServicerService.svc/REST/GetMsCompanyInformation",
        parameters=params,
        module=module,
    )


def run_module():
    module_args = dict(
        api_url=dict(type="str", required=True, no_log=False),
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

    api_url: str = module.params["api_url"]
    api_token: str = module.params["api_token"]
    system_time: str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    api_log_directory: str = module.params["api_log_directory"]

    # if the user is working with this module in only check mode we do not
    # want to make any changes to the environment, just return the current
    # state with no modifications
    if module.check_mode:
        module.exit_json(**result)

    credit_bureau_file_path: str = get_ms_company_information(
        api_url=api_url,
        api_token=api_token,
        api_log_directory=api_log_directory,
        module=module,
    )["FilePath"]
    bureau_response: dict = create_metro_2_file_and_report(
        file_path=credit_bureau_file_path,
        api_token=api_token,
        api_url=api_url,
        api_log_directory=api_log_directory,
        system_time=system_time,
        module=module,
    )

    try:
        if not bureau_response.get("ApiCallSuccessful", None):
            module.fail_json(
                msg="API call unsuccessful",
                changed=False,
                failed=True,
                api_response=bureau_response,
            )
    except Exception as e:
        module.fail_json(msg=f"failed to create file: {e}", changed=False, failed=True)

    # here we will include only the data we need
    bureau_response["Document"][
        "DocumentBase64"
    ] = "[REDACTED] - base64 encoded data that we will not include"
    bureau_response["Data"][
        "RecapReportItems"
    ] = "[REDACTED] - list of the loans and some of their metadata"
    bureau_response["Data"][
        "CreditBureauLoans"
    ] = "[REDACTED] - list of customer account numbers"
    bureau_response["Data"]["FileTotals"] = "[REDACTED] - list of file sizes"
    bureau_response["file_path"] = credit_bureau_file_path

    result["api_response"] = bureau_response
    result["msg"] = "Credit Bureau Files Created"
    result["changed"] = True
    # in the event of a successful module execution, you will want to
    # simple AnsibleModule.exit_json(), passing the key/value results
    module.exit_json(**result)


if __name__ == "__main__":
    run_module()

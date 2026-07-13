# Copyright: (c) 2024, David Villafaña <david.villafana@capcu.org>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
from __future__ import absolute_import, division, print_function
from ansible.module_utils.basic import AnsibleModule
from typing import Callable, Any
from datetime import datetime
import requests
import logging
import os
import base64

__metaclass__ = type

DOCUMENTATION = r"""
---
module: process_window_object_data

short_description: Calls the FICS Mortgage Servicer service API and creates a payoff statement at the desired path

# If this is part of a collection, you need to use semantic versioning,
# i.e. the version is of the form "2.5.0" and not "2.4".
version_added: "2.2.0"

description:
    - Calls the FICS Mortgage Servicer service API and creates a payoff statement at the desired path.
    - The result is the path to the saved file
    - Disclaimer, this module has only been tested for our exact use case

author:
    - David Villafaña IV

requirements:
     - logging >= 0.4.9.6
     - requests >= 2.32.3

options:
    dest:
        description: This is the full path to where the directory where the file will be created, it creates parent directories if they do not exist
        required: true
        type: str
    property_address:
        description: address of the property
        required: true
        type: str
    loan_id:
        description: id of the loan as it is in FICS
        required: true
        type: int
    loan_name:
        description: full name of the loan payee
        required: true
        type: str
    city:
        description: city of the property
        required: true
        type: str
    state:
        description: state of the property
        required: true
        type: str
    zip:
        description: zip (postal) code of the property
        required: true
        type: str
    payoff_date:
        description: payoff date of loan, YYYY-MM-DD
        required: true
        type: str
    core_api_url:
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
- name: process_object_windows_data
  process_object_windows_data:
    dest: /etc/tmp/whatever/
    property_address: 105 ching ave SW
    loan_id: 1231234
    loan_name: DAVID VILLANOVA
    city: CHICAGO
    state: Illinois
    zip: 12345
    payoff_date: 2024-01-22
    core_api_url: http://mortgageservicer.fics/MortgageServicerService.svc/REST/
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


def process_window_object_data(
    api_token: str,
    api_url: str,
    loan_id: int,
    full_name: str,
    address: str,
    city_state_zip: str,
    payoff_date: datetime,
    api_log_directory: str
):
    params: dict = {
        "WindowObject": {
            "LoanId": loan_id,
            "UseLogo": True,
            "PayoffDate": payoff_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "SuppressPrinting": True,
            # TODO: get interest calculation option from custom api
            "CalcOption": "ThreeSixtyFive",
            "MailingOption": "Borrower",
            "MailingName": full_name,
            "MailingAddress1": address,
            "MailingCityStateZip": city_state_zip,
            "ItemLine1": "Release Fee",
            "ItemLine1Amount": 25,
            "Comment": """When remitting funds, please use our loan number to insure proper posting and provide us with the borrower’s forwarding address.  Funds received in this office after 12:00 noon will be processed on the next business day, with interest charged to that date.
 
All payoff figures are subject to clearance of funds in transit.  The payoff is subject to final audit when presented.  Any overpayment or refunds will be mailed directly to the borrower.""",
            # TODO: get last paid bill from custom api
            # "DateOfLastPaidBill": "2024-09-10T17:00:50",
            # TODO: get interest calculation method from custom api
            "InterestCalculationMethodEnum": "DailyInterest365",
            # "ScheduleChanges": False,
            # "AppliedInterest": True,
            "UseNetDeferredBalance": True,
            "Update": True,
            "DeferredInterestYn": True,
            "UnappliedYn": True,
            "DelLateChargesYn": True,
            "TaxAndInsuranceYn": False,
            "NegTaxAndInsuranceYn": False,
            "ExpectedTaxAndInsuranceYn": False,
            "CalcLateChargesYn": True,
            "SubsidyYn": False,
            "ForeclosureBankruptcyYn": False,
            "ReturnCheckChargesYn": False,
            "FinalMIPPMIYn": False,
            "MiscFeesYn": True,
            "LossDraftYn": False,
            "EscrowAdvanceYn": True,
            "Token": api_token,
        }
    }
    return log_function_call(
        api_log_directory,
        call_api,
        api_url,
        "post",
        "ProcessWindowObjectData",
        parameters=params,
    )


def run_module():
    module_args = dict(
        dest=dict(type="str", required=True, no_log=False),
        property_address=dict(type="str", required=True, no_log=False),
        loan_id=dict(type="int", required=True, no_log=False),
        loan_name=dict(type="str", required=True, no_log=False),
        city=dict(type="str", required=True, no_log=False),
        state=dict(type="str", required=True, no_log=False),
        zip=dict(type="str", required=True, no_log=False),
        payoff_date=dict(type="str", required=True, no_log=False),
        core_api_url=dict(type="str", required=True, no_log=False),
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

    api_url: str = module.params["core_api_url"]
    api_token: str = module.params["api_token"]
    api_log_directory: str = module.params["api_log_directory"]
    dest: list[dict] = module.params["dest"]
    property_address: str = module.params["property_address"]
    loan_id: int = module.params["loan_id"]
    loan_name: str = module.params["loan_name"]
    city: str = module.params["city"]
    state: str = module.params["state"]
    zip: str = module.params["zip"]
    payoff_date: datetime = datetime.strptime(module.params["payoff_date"], "%Y-%m-%d")

    # if the user is working with this module in only check mode we do not
    # want to make any changes to the environment, just return the current
    # state with no modifications
    if module.check_mode:
        module.exit_json(**result)

    payoff_resp: dict = process_window_object_data(
        api_url=api_url,
        api_token=api_token,
        api_log_directory=api_log_directory,
        city_state_zip=f'{city}, {state} {zip}',
        payoff_date=payoff_date,
        address=property_address,
        loan_id=loan_id,
        full_name=loan_name
    )
    if payoff_resp.get("ApiCallSuccessful", None):
        mail_name = payoff_resp.get("Data", {}).get("MailingCorrName", {}).replace(" ", "_") if payoff_resp.get("Data", {}).get("MailingCorrName", {}) else loan_name
        rest_of_name: str = "_" + str(loan_id) + "_" + datetime.now().strftime("%Y-%m-%d") + '_payoff_statement.pdf'
        file_name: str = mail_name + rest_of_name
        with open(os.path.join(dest, file_name), 'wb') as f:
            f.write(base64.b64decode(payoff_resp["Document"]["DocumentBase64"]))
        result["msg"] = "API call successful. File created"
        result["changed"] = False
        result["failed"] = False
        result["api_response"] = payoff_resp

    else:
        module.fail_json(
            msg="API call unsuccessful",
            changed=False,
            failed=True,
            api_response=payoff_resp,
        )

    # in the event of a successful module execution, you will want to
    # simple AnsibleModule.exit_json(), passing the key/value results
    module.exit_json(**result)


if __name__ == "__main__":
    run_module()

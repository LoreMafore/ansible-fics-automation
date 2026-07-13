# Copyright: (c) 2024, David Villafaña <david.villafana@capcu.org>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
from __future__ import absolute_import, division, print_function
from ansible.module_utils.basic import AnsibleModule
from typing import Optional
import requests
from datetime import datetime
import os
import base64

__metaclass__ = type

DOCUMENTATION = r"""
---
module: allied_insurance_interface_program

short_description: Calls the FICS Mortgage Servicer special services API to create the Allied Insurance file at the specified destination

# If this is part of a collection, you need to use semantic versioning,
# i.e. the version is of the form "2.5.0" and not "2.4".
version_added: "1.0.2"

description:
    - Calls the FICS Mortgage Servicer special services API to create the Allied Insurance file at the specified destination
    - Disclaimer: this module has only been tested for our exact use case

author:
    - David Villafaña IV

requirements: [ ]

options:
    dest:
        description: This is the full path to where the file will be created, it creates parent directories if they do not exist
        required: true
        type: str
    special_service_api_url:
        description: This is the URL of the special service API
        required: true
        type: str
    api_token:
        description: this is the api token used for authentication to the API
        required: true
        type: str

"""

EXAMPLES = r"""
- name: create file to send
  allied_insurance_interface_program:
    dest: /mnt/fics/Mortgage Services/MS_TEST/Allied Interface/file.txt
    special_service_api_url: http://mortgageservicer.fics/SpecialsService.svc/REST/
    api_token: ASDFASDFJSDFSHFJJSDGFSJGQWEUI123123SDFSDFJ12312801C15034264BC98B33619F4A547AECBDD412D46A24D2560D5EFDD8DEDFE74325DC2E7B156C60B942
"""

RETURN = r"""
"""


def call_api(
    base_url: str, method: str, endpoint: str, parameters: dict, module: dict
) -> Optional[dict]:
    headers = {
        "Content-Type": "application/json",
    }

    if method == "post":
        response = requests.post(base_url + endpoint, json=parameters, headers=headers)
    elif method == "get":
        response = requests.get(base_url + endpoint, json=parameters, headers=headers)
    elif method == "put":
        response = requests.put(base_url + endpoint, json=parameters, headers=headers)
    elif method == "delete":
        response = requests.delete(
            base_url + endpoint, json=parameters, headers=headers
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


def get_create_allied_insurance_interface_file(module: dict) -> Optional[dict]:
    params: dict = {
        "CreateRequest": {
            "FilePath": "sample string",
            "Loans": [500, 500],
            "Investors": [
                {
                    "Bank": "sample string",
                    "Investor": "sample string",
                    "Group": "sample string",
                    "CompositeInvestorCode": "sample string",
                },
                {
                    "Bank": "sample string",
                    "Investor": "sample string",
                    "Group": "sample string",
                    "CompositeInvestorCode": "sample string",
                },
            ],
            "Payees": [1, 1],
            "ErrorMessage": "sample string",
            "SystemDate": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "Token": module.params["api_token"],
            "ApiParameters": "sample string",
        }
    }
    return call_api(
        module.params["special_service_api_url"],
        "post",
        "CreateAlliedInsuranceInterfaceFile",
        parameters=params,
        module=module,
    )


def run_module():
    module_args = dict(
        dest=dict(type="str", required=True, no_log=False),
        special_service_api_url=dict(type="str", required=True, no_log=False),
        api_token=dict(type="str", required=True, no_log=True),
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

    output_file_path: str = module.params["dest"]

    # if the user is working with this module in only check mode we do not
    # want to make any changes to the environment, just return the current
    # state with no modifications
    if module.check_mode:
        module.exit_json(**result)

    try:
        os.makedirs(name=str(os.path.dirname(output_file_path)), exist_ok=True)
    except Exception as e:
        module.fail_json(
            msg=f"failed to create parent directories: {e}", changed=False, failed=True
        )

    api_response: dict = get_create_allied_insurance_interface_file(module)
    try:
        if api_response.get("ApiCallSuccessful", None):
            base64_file = api_response.get("File", None)
            if base64_file:
                txt_data = base64.b64decode(base64_file)
                with open(module.params["dest"], "wb") as txt_file:
                    txt_file.write(txt_data)
                result["changed"] = True
                result["failed"] = False
                result["msg"] = f"Wrote file at {module.params['dest']}"
                result["api_response"] = api_response
            else:
                result["changed"] = False
                result["failed"] = False
                result["msg"] = "no file retrieved"
                result["api_response"] = api_response
        else:
            module.fail_json(
                msg="API call unsuccessful",
                changed=False,
                failed=True,
                api_response=api_response,
            )
    except Exception as e:
        module.fail_json(msg=f"failed to create file: {e}", changed=False, failed=True)

    # in the event of a successful module execution, you will want to
    # simple AnsibleModule.exit_json(), passing the key/value results
    module.exit_json(**result)


if __name__ == "__main__":
    run_module()

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


def get_ots_schedule_cmr_report(
    api_url: str, 
    api_token: str, 
    api_log_directory: str,
) -> dict:
    params: dict = {
        "Message":{
            "SystemDate": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "Token": api_token,
        }
    }
    return log_function_call(
        api_log_directory,
        call_api,
        base_url=api_url,
        method="post",
        endpoint="BuildOtsScheduleCmrReport",
        parameters=params,
    )

def pdf_to_csv(pdf_path: str, csv_path: str):
    """
    Convert PDF to CSV by reconstructing rows from line-by-line text extraction.
    
    The PyMuPDF extraction gives each table cell on its own line, so we need to:
    1. Identify when we hit the header (starts with "Loan #")
    2. Collect all header fields until we hit data
    3. For each data row, collect fields starting from the loan number
    4. Reconstruct proper CSV rows
    """
    doc = fitz.open(pdf_path)
    all_rows = []
    header_added = False
    total_size = 17
    
    for page_num in range(len(doc)):
        page = doc[page_num]
        text = page.get_text("text")
        lines = [line.strip() for line in text.strip().split('\n') if line.strip()]
        # single_item_list = [
        #     'Investor'
        # ]
        
        i = 0
        while i < len(lines):
            line = lines[i]
            
            if 'Investor Codes' in line:
                expected_fields = len(all_rows[0]) if all_rows else total_size
                label_row = [''] * expected_fields
                label_row[0] = line +" "+ lines[i-1]
                all_rows.append(label_row)
                i += 1
                continue

           # Skip header/title lines
            if any(skip in line for skip in [
                'Capital Credit Union', 'Mortgage Servicer System',
                'OTS Schedule CMR', 
                'LOANS & MORTGAGE', '30-Year Mortgages', 'Mortgage Loans',
                'WARM', 'WAC', 'FHA/VA', 'Less Than', 'CMR 0', 
                'February', '15-Year Mortgages', 'Balloon', 'Adjustable'
            ]):
                i += 1
                continue


            # if 'FIXED-RATE' in line:
            #     i += 1
            #     conjoined_line = line + "\n" + lines[i]
            #     all_rows.append([conjoined_line])
            #     i += 1
            #     continue

            # Check if we're at the start of the header
            #and not header_added:
            if line == 'Loan #':
                # Collect all header parts until we hit a loan number or page marker
                header = ['Loan #']
                j = i + 1
                
                while j < len(lines):
                    current = lines[j]
                    if any(group in current for group in [
                        'Loan', 'Rem', 'Balloon', 
                        'Percent', 'Principal', 'Box'
                    ]) and not current == 'Loan Name':
                        
                        j += 1
                        current = current + " " + lines[j]
                   

                    # Stop when we hit a data row (loan number after enough header fields)
                    if re.match(r'^\d{4,}$', current) and len(header) > 10:
                        break
                    
                    # Stop at page marker after collecting headers
                    if current.startswith('Page ') and len(header) > 10:
                        j += 1
                        break
                    
                    # Skip page markers in the middle of headers
                    if current.startswith('Page '):
                        j += 1
                        continue
                    
                    header.append(current)
                    j += 1
                    
                    # Safety: don't collect more than 30 fields
                    if len(header) > 30:
                        break
                
                all_rows.append(header)
                header_added = True
                i = j
                continue

            # Check if this is a group total line
            # These appear as standalone dollar amounts after a group of loans
            if header_added and re.match(r'^[\d,]+\.\d{2}$', line) and not re.match(r'^\d{4,}$', line):
                # Build a total row with "BkInvGrpTotal" as Loan Name
                expected_fields = len(all_rows[0]) if all_rows else total_size
                total_row = [''] * expected_fields
                total_row[1] = 'BkInvGrpTotal'  # Loan Name column
                # Find Principal Balance column index (should be index 15)
                principal_idx = 15
                if principal_idx < expected_fields:
                    total_row[principal_idx] = line
                all_rows.append(total_row)
                i += 1
                continue
            
            # Check if this is the start of a data row (loan number)
            # Must be 4+ digits and we must have seen the header
            if header_added and re.match(r'^\d{4,}$', line):
                # This is a loan number - start collecting the row
                row = [line]
                j = i + 1
                
                # Collect fields based on the header length
                expected_fields = len(all_rows[0]) if all_rows else total_size
                
                while j < len(lines) and len(row) < expected_fields:
                    current = lines[j]
                    
                    # Stop if we hit the next loan number (with some safety margin)
                    if re.match(r'^\d{4,}$', current) and len(row) >= expected_fields - 3:
                        break
                    
                    # Skip page markers
                    if current.startswith('Page '):
                        j += 1
                        continue
                    
                    # Skip section headers
                    if any(skip in current for skip in [
                        'Capital Credit Union', 'Mortgage Servicer System',
                        'OTS Schedule CMR'
                    ]):
                        j += 1
                        continue
                    
                    name_with_bk = re.match(r'^(.+)\s*(\d{2})$', current)
                    if name_with_bk and len(row) == 1:  # Only check for 2nd field (Loan Name)
                        # Split into loan name and Bk field
                        row.append(name_with_bk.group(1))  # Loan Name without the trailing digits
                        row.append(name_with_bk.group(2))  # Bk (2-digit code)
                        j += 1
                        continue

                    date_pattern = r'^(\d+)\s+(\d{1,2}/\d{1,2}/\d{2,4})$'
                    match = re.match(date_pattern, current)
                    if match:
                        # Split into two separate fields
                        row.append(match.group(1))  # The number (Rem Term)
                        row.append(match.group(2))  # The date (Balloon Date)
                    else:
                        row.append(current)
                    j += 1

                # Validate Balloon Date (index 11) - if blank in PDF, it gets skipped
                if len(row) > 11 and not re.match(r'\d{1,2}/\d{1,2}/\d{2,4}', row[11]):
                    row.insert(11, '')  # Insert empty Balloon Date
                
                # Add the row if it has enough fields (at least 80% of expected)
                if len(row) >= (expected_fields * 0.8):
                    # Pad with empty strings if needed
                    while len(row) < expected_fields:
                        row.append('')
                    # Trim if too long
                    row = row[:expected_fields]
                    all_rows.append(row)
                
                i = j
                continue
            
            i += 1
    
    doc.close()
    
    # Write to CSV
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(all_rows)


def run_module():
    module_args = dict(
        pdf_dest=dict(type="str", required=True, no_log=False),
        csv_dest=dict(type="str", required=True, no_log=False),
        fics_api_url=dict(type="str", required=True, no_log=False),
        api_token=dict(type="str", required=True, no_log=True),
        api_log_directory=dict(type="str", required=False, no_log=False),
        api_due_date=dict(type="str", required=True, no_log=False),
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

    api_url: str = module.params["fics_api_url"]
    api_token: str = module.params["api_token"]
    api_log_directory: str = module.params["api_log_directory"]
    pdf_dest: str = module.params["pdf_dest"]
    csv_dest: str = module.params["csv_dest"]

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
        if trial_resp.get("ApiCallSuccessful", None):
            try:
                os.makedirs(name=str(os.path.dirname(pdf_dest)), exist_ok=True)
            except Exception as e:
                module.fail_json(
                    msg=f"failed to create parent directories: {e}",
                    changed=False,
                    failed=True,
                )

            base64_file = trial_resp.get("Document", {}).get("DocumentBase64", None)
            if base64_file:
                ots_schedule_cmr_report = base64.b64decode(base64_file)

                with open(module.params["pdf_dest"], "wb") as ots_schedule_cmr_report_file:
                    ots_schedule_cmr_report_file.write(ots_schedule_cmr_report)

                try:
                    pdf_to_csv(pdf_dest, csv_dest)
                    result["changed"] = True
                    result["failed"] = False
                    result["msg"] = f"Wrote PDF at {module.params['pdf_dest']} and CSV at {module.params['csv_dest']}"
                    result["api_response"] = trial_resp

                except Exception as e:
                    result["changed"] = True
                    result["failed"] = False
                    result["msg"] = f"Wrote PDF at {module.params['pdf_dest']} but failed to write CSV"
                    result["api_response"] = trial_resp

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

    module.exit_json(**result)


if __name__ == "__main__":
    run_module()

# ansible-collection-fics

> Ansible collection of modules for automating FICS Mortgage Servicer and FICS Mortgage Accountant workflows.

[![Version](https://img.shields.io/badge/version-3.9.0-blue)](galaxy.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Table of Contents

- [Overview](#overview)
- [Requirements](#requirements)
- [Installation](#installation)
- [Authentication](#authentication)
- [Modules](#modules)
  - [allied\_insurance\_interface\_program](#allied_insurance_interface_program)
  - [create\_metro\_2\_file\_and\_report](#create_metro_2_file_and_report)
  - [get\_account\_history\_report](#get_account_history_report)
  - [get\_advanced\_selector\_request](#get_advanced_selector_request)
  - [get\_amortized\_delinquent\_report](#get_amortized_delinquent_report)
  - [get\_delinquent\_principal\_balances](#get_delinquent_principal_balances)
  - [get\_ffiec\_call\_report](#get_ffiec_call_report)
  - [get\_general\_ledger\_report](#get_general_ledger_report)
  - [get\_interest\_accrual\_report](#get_interest_accrual_report)
  - [get\_new\_loans\_entered\_report](#get_new_loans_entered_report)
  - [get\_ots\_schedule\_cmr\_pdf\_report](#get_ots_schedule_cmr_pdf_report)
  - [get\_portfolio\_report](#get_portfolio_report)
  - [get\_trial\_balance\_report](#get_trial_balance_report)
  - [process\_window\_object\_data](#process_window_object_data)
  - [run\_late\_notices\_report](#run_late_notices_report)
- [Authors](#authors)
- [License](#license)

---

## Overview

This collection provides Ansible modules that interface with the FICS Mortgage Servicer and FICS Mortgage Accountant REST APIs. Each module handles a distinct report or special service operation — authenticating against the API, calling the appropriate endpoint, decoding any base64-encoded document payloads, writing output files to disk, and optionally logging all API interactions.

Common behaviors shared across all modules:

- Parent directories for all output paths are created automatically.
- The `api_token` parameter is always marked `no_log: true` and is never surfaced in Ansible output or logs.
- An optional `api_log_directory` parameter enables detailed API call logging to `<directory>/api_calls.log`.
- All modules set `changed: true` only when a file is actually written to disk.
- No module supports check mode (`supports_check_mode: false`).

---

## Requirements

- Ansible >= 2.9
- Python >= 3.8 on the managed host
- Network access from the Ansible controller to your FICS API endpoints
- A valid FICS API bearer token

---

## Installation

### From Ansible Galaxy

```bash
ansible-galaxy collection install dtvillafana.fics
```

### From source

```bash
git clone https://github.com/dtvillafana/ansible-collection-fics.git
cd ansible-collection-fics
ansible-galaxy collection build
ansible-galaxy collection install dtvillafana-fics-*.tar.gz
```

---

## Authentication

All modules accept an `api_token` parameter (bearer token). Store it securely using Ansible Vault or an external secrets manager and pass it as a variable:

```yaml
vars:
  fics_api_token: "{{ vault_fics_api_token }}"
```

---

## Modules

---

### `allied_insurance_interface_program`

Calls the FICS Mortgage Servicer Special Services API to generate the Allied Insurance interface file and write it to a local path.

**FICS endpoint:** `CreateAlliedInsuranceInterfaceFile`

#### Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `dest` | str | Yes | Full path where the output file will be written. Parent directories are created automatically. |
| `special_service_api_url` | str | Yes | Base URL of the FICS Special Services API. |
| `api_token` | str | Yes | Bearer token used for API authentication. Never logged. |

#### Example

```yaml
- name: Generate Allied Insurance interface file
  dtvillafana.fics.allied_insurance_interface_program:
    dest: /data/fics/allied_insurance/output.txt
    special_service_api_url: https://fics.example.com/SpecialService
    api_token: "{{ vault_fics_api_token }}"
```

---

### `create_metro_2_file_and_report`

Calls the FICS Mortgage Servicer Special Services API to create Metro 2 credit bureau files for Equifax, Experian, and TransUnion. Internally retrieves company information first to resolve the output file path, then calls `CreateMetro2FileAndReport` for each bureau.

**FICS endpoints:** `GetMsCompanyInformation`, `CreateMetro2FileAndReport`

#### Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `api_url` | str | Yes | Base URL of the FICS API. |
| `api_token` | str | Yes | Bearer token used for API authentication. Never logged. |
| `api_log_directory` | str | No | Directory where API interaction logs will be written. |

#### Example

```yaml
- name: Create Metro 2 credit bureau files
  dtvillafana.fics.create_metro_2_file_and_report:
    api_url: https://fics.example.com/SpecialService
    api_token: "{{ vault_fics_api_token }}"
    api_log_directory: /var/log/fics/api
```

---

### `get_account_history_report`

Calls the FICS Mortgage Accountant Special Services API to generate an Account History Report PDF. The date range is automatically set to the full prior calendar month — no date parameters are required.

**FICS endpoint:** `GetAccountHistory`

#### Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `dest` | str | Yes | Full path where the PDF will be written. Parent directories are created automatically. |
| `fics_api_url` | str | Yes | Base URL of the FICS Mortgage Accountant Special Services API. |
| `api_token` | str | Yes | Bearer token used for API authentication. Never logged. |
| `api_log_directory` | str | No | Directory where API interaction logs will be written. |

#### Return Values

| Key | Type | Description |
|---|---|---|
| `msg` | str | Human-readable result, e.g. `"Wrote files to /data/fics/..."` |
| `changed` | bool | `true` if a file was written to disk. |
| `api_response` | str | Full API response payload including the base64-encoded document. |

#### Example

```yaml
- name: Generate Account History Report for prior month
  dtvillafana.fics.get_account_history_report:
    dest: /data/fics/reports/account_history.pdf
    fics_api_url: https://fics.example.com/MortgageAccountantSpecialService
    api_token: "{{ vault_fics_api_token }}"
    api_log_directory: /var/log/fics/api
```

---

### `get_advanced_selector_request`

Calls the FICS Mortgage Servicer Service API to run an advanced selector query. Returns the full API response in `api_response` without writing any local file.

**FICS endpoint:** `GetAdvancedSelectorRequest`

#### Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `query_list` | list | Yes | List of query parameter dictionaries passed directly to the API. |
| `core_api_url` | str | Yes | Base URL of the FICS Core Service API. |
| `api_token` | str | Yes | Bearer token used for API authentication. Never logged. |
| `api_log_directory` | str | No | Directory where API interaction logs will be written. |

#### Return Values

| Key | Type | Description |
|---|---|---|
| `msg` | str | Human-readable result message. |
| `changed` | bool | Always `false` — this module does not write files. |
| `api_response` | dict | Full API response payload containing query results. |

#### Example

```yaml
- name: Run advanced selector query
  dtvillafana.fics.get_advanced_selector_request:
    core_api_url: https://fics.example.com/MortgageServicerService
    api_token: "{{ vault_fics_api_token }}"
    query_list:
      - FieldName: LoanStatus
        Operator: EqualTo
        Value: Active
  register: selector_result

- name: Show results
  debug:
    var: selector_result.api_response
```

---

### `get_amortized_delinquent_report`

Calls the FICS Mortgage Servicer Special Services API to generate an Amortized Delinquent Report PDF for a given due date.

**FICS endpoint:** `ProcessAmortizedDelinquentReportData`

#### Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `dest` | str | Yes | Full path where the PDF will be written. Parent directories are created automatically. |
| `fics_api_url` | str | Yes | Base URL of the FICS Mortgage Servicer Special Services API. |
| `api_token` | str | Yes | Bearer token used for API authentication. Never logged. |
| `api_due_date` | str | Yes | Report due date in ISO 8601 format, e.g. `2026-01-31T23:59:59`. |
| `api_log_directory` | str | No | Directory where API interaction logs will be written. |

#### Return Values

| Key | Type | Description |
|---|---|---|
| `msg` | str | Human-readable result, e.g. `"Wrote files to /data/fics/..."` |
| `changed` | bool | `true` if a file was written to disk. |
| `api_response` | str | Full API response payload including the base64-encoded document. |

#### Example

```yaml
- name: Generate Amortized Delinquent Report
  dtvillafana.fics.get_amortized_delinquent_report:
    dest: /data/fics/reports/amortized_delinquent.pdf
    fics_api_url: https://fics.example.com/SpecialService
    api_token: "{{ vault_fics_api_token }}"
    api_due_date: "2026-01-31T23:59:59"
    api_log_directory: /var/log/fics/api
```

---

### `get_delinquent_principal_balances`

Calls the FICS Mortgage Servicer Special Services API to generate a Delinquent Principal Balances report for a given due date.

**FICS endpoint:** `GetManageDelinqPrinBalanceReportData`

#### Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `dest` | str | Yes | Full path where the output file will be written. Parent directories are created automatically. |
| `fics_api_url` | str | Yes | Base URL of the FICS Mortgage Servicer Special Services API. |
| `api_token` | str | Yes | Bearer token used for API authentication. Never logged. |
| `api_due_date` | str | Yes | Report due date in ISO 8601 format, e.g. `2026-01-31T23:59:59`. |
| `api_log_directory` | str | No | Directory where API interaction logs will be written. |

#### Return Values

| Key | Type | Description |
|---|---|---|
| `msg` | str | Human-readable result, e.g. `"Wrote files to /data/fics/..."` |
| `changed` | bool | `true` if a file was written to disk. |
| `api_response` | str | Full API response payload including the base64-encoded document. |

#### Example

```yaml
- name: Generate Delinquent Principal Balances report
  dtvillafana.fics.get_delinquent_principal_balances:
    dest: /data/fics/reports/delinquent_principal.pdf
    fics_api_url: https://fics.example.com/SpecialService
    api_token: "{{ vault_fics_api_token }}"
    api_due_date: "2026-01-31T23:59:59"
    api_log_directory: /var/log/fics/api
```

---

### `get_ffiec_call_report`

Calls the FICS Mortgage Servicer Special Services API to generate an FFIEC Call Report file.

**FICS endpoint:** `GetFFIECReportLoans`

#### Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `dest` | str | Yes | Full path where the output file will be written. Parent directories are created automatically. |
| `fics_api_url` | str | Yes | Base URL of the FICS Mortgage Servicer Special Services API. |
| `api_token` | str | Yes | Bearer token used for API authentication. Never logged. |
| `api_log_directory` | str | No | Directory where API interaction logs will be written. |

#### Return Values

| Key | Type | Description |
|---|---|---|
| `msg` | str | Human-readable result, e.g. `"Wrote files to /data/fics/..."` |
| `changed` | bool | `true` if a file was written to disk. |
| `api_response` | str | Full API response payload including the base64-encoded document. |

#### Example

```yaml
- name: Generate FFIEC Call Report
  dtvillafana.fics.get_ffiec_call_report:
    dest: /data/fics/reports/ffiec_call_report.txt
    fics_api_url: https://fics.example.com/SpecialService
    api_token: "{{ vault_fics_api_token }}"
    api_log_directory: /var/log/fics/api
```

---

### `get_general_ledger_report`

Calls the FICS Mortgage Accountant Special Services API to generate a General Ledger Report PDF. The date range is automatically set to the full prior calendar month — no date parameters are required.

**FICS endpoint:** `GetGeneralLedger`

#### Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `dest` | str | Yes | Full path where the PDF will be written. Parent directories are created automatically. |
| `fics_api_url` | str | Yes | Base URL of the FICS Mortgage Accountant Special Services API. |
| `api_token` | str | Yes | Bearer token used for API authentication. Never logged. |
| `api_log_directory` | str | No | Directory where API interaction logs will be written. |

#### Return Values

| Key | Type | Description |
|---|---|---|
| `msg` | str | Human-readable result, e.g. `"Wrote files to /data/fics/..."` |
| `changed` | bool | `true` if a file was written to disk. |
| `api_response` | str | Full API response payload including the base64-encoded document. |

#### Example

```yaml
- name: Generate General Ledger Report for prior month
  dtvillafana.fics.get_general_ledger_report:
    dest: /data/fics/reports/general_ledger.pdf
    fics_api_url: https://fics.example.com/MortgageAccountantSpecialService
    api_token: "{{ vault_fics_api_token }}"
    api_log_directory: /var/log/fics/api
```

---

### `get_interest_accrual_report`

Calls the FICS Mortgage Servicer Special Services API to generate an Interest Accrual Report PDF. The accrual period is derived automatically from `api_due_date` as the full prior calendar month. Uses fixed calculation settings: Actual method, Bank/Investor Group sort, Factor 360.

**FICS endpoint:** `RunInterestAccrual`

#### Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `dest` | str | Yes | Full path where the PDF will be written. Parent directories are created automatically. |
| `fics_api_url` | str | Yes | Base URL of the FICS Mortgage Servicer Special Services API. |
| `api_token` | str | Yes | Bearer token used for API authentication. Never logged. |
| `api_due_date` | str | Yes | Reference due date used to derive the accrual period, in ISO 8601 format, e.g. `2026-01-31T23:59:59`. |
| `api_log_directory` | str | No | Directory where API interaction logs will be written. |

#### Return Values

| Key | Type | Description |
|---|---|---|
| `msg` | str | Human-readable result, e.g. `"Wrote files to /data/fics/..."` |
| `changed` | bool | `true` if a file was written to disk. |
| `api_response` | str | Full API response payload including the base64-encoded document. |

#### Example

```yaml
- name: Generate Interest Accrual Report
  dtvillafana.fics.get_interest_accrual_report:
    dest: /data/fics/reports/interest_accrual.pdf
    fics_api_url: https://fics.example.com/SpecialService
    api_token: "{{ vault_fics_api_token }}"
    api_due_date: "2026-01-31T23:59:59"
    api_log_directory: /var/log/fics/api
```

---

### `get_new_loans_entered_report`

Calls the FICS Mortgage Servicer Special Services API to generate a New Loans Entered Report PDF. If no new loans exist for the period, the module exits cleanly with `changed: false` and a descriptive message rather than failing.

**FICS endpoint:** `CreateNewLoansEnteredReport`

#### Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `dest` | str | Yes | Full path where the PDF will be written. Parent directories are created automatically. |
| `fics_api_url` | str | Yes | Base URL of the FICS Mortgage Servicer Special Services API. |
| `api_token` | str | Yes | Bearer token used for API authentication. Never logged. |
| `api_update_database` | bool | Yes | Whether to commit results to the FICS database. Set to `false` when testing. |
| `api_log_directory` | str | No | Directory where API interaction logs will be written. |

#### Return Values

| Key | Type | Description |
|---|---|---|
| `msg` | str | Human-readable result, e.g. `"Wrote files to /data/fics/..."` or `"no new loans for this period"`. |
| `changed` | bool | `true` if a file was written to disk. |
| `api_response` | str | Full API response payload including the base64-encoded document. |

#### Example

```yaml
- name: Generate New Loans Entered Report
  dtvillafana.fics.get_new_loans_entered_report:
    dest: /data/fics/reports/new_loans.pdf
    fics_api_url: https://fics.example.com/SpecialService
    api_token: "{{ vault_fics_api_token }}"
    api_update_database: true
    api_log_directory: /var/log/fics/api
```

---

### `get_ots_schedule_cmr_pdf_report`

Calls the FICS Mortgage Servicer Special Services API to generate an OTS Schedule CMR Report PDF.

**FICS endpoint:** `BuildOtsScheduleCmrReport`

#### Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `dest` | str | Yes | Full path where the PDF will be written. Parent directories are created automatically. |
| `fics_api_url` | str | Yes | Base URL of the FICS Mortgage Servicer Special Services API. |
| `api_token` | str | Yes | Bearer token used for API authentication. Never logged. |
| `api_log_directory` | str | No | Directory where API interaction logs will be written. |

#### Return Values

| Key | Type | Description |
|---|---|---|
| `msg` | str | Human-readable result, e.g. `"Wrote files to /data/fics/..."` |
| `changed` | bool | `true` if a file was written to disk. |
| `api_response` | str | Full API response payload including the base64-encoded document. |

#### Example

```yaml
- name: Generate OTS Schedule CMR PDF Report
  dtvillafana.fics.get_ots_schedule_cmr_pdf_report:
    dest: /data/fics/reports/ots_schedule_cmr.pdf
    fics_api_url: https://fics.example.com/SpecialService
    api_token: "{{ vault_fics_api_token }}"
    api_log_directory: /var/log/fics/api
```

---

### `get_portfolio_report`

Calls the FICS Mortgage Servicer Special Services API to generate a Portfolio Report file. Report settings are fixed: groups and zero-balance loans are excluded.

**FICS endpoint:** `GetPortfolioReport`

#### Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `dest` | str | Yes | Full path where the output file will be written. Parent directories are created automatically. |
| `fics_api_url` | str | Yes | Base URL of the FICS Mortgage Servicer Special Services API. |
| `api_token` | str | Yes | Bearer token used for API authentication. Never logged. |
| `api_log_directory` | str | No | Directory where API interaction logs will be written. |

#### Return Values

| Key | Type | Description |
|---|---|---|
| `msg` | str | Human-readable result, e.g. `"Wrote files to /data/fics/..."` |
| `changed` | bool | `true` if a file was written to disk. |
| `api_response` | str | Full API response payload including the base64-encoded document. |

#### Example

```yaml
- name: Generate Portfolio Report
  dtvillafana.fics.get_portfolio_report:
    dest: /data/fics/reports/portfolio.txt
    fics_api_url: https://fics.example.com/SpecialService
    api_token: "{{ vault_fics_api_token }}"
    api_log_directory: /var/log/fics/api
```

---

### `get_trial_balance_report`

Calls the FICS Mortgage Servicer Batch Service API to generate a Trial Balance Report PDF. Report settings are fixed: all loans included, history created, sorted by Bank/Investor Group.

**FICS endpoint:** `GetTrialBalanceReport`

#### Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `dest` | str | Yes | Full path where the PDF will be written. Parent directories are created automatically. |
| `batch_service_api_url` | str | Yes | Base URL of the FICS Batch Service API. |
| `api_token` | str | Yes | Bearer token used for API authentication. Never logged. |
| `api_log_directory` | str | No | Directory where API interaction logs will be written. |

#### Example

```yaml
- name: Generate Trial Balance Report
  dtvillafana.fics.get_trial_balance_report:
    dest: /data/fics/reports/trial_balance.pdf
    batch_service_api_url: https://fics.example.com/BatchService
    api_token: "{{ vault_fics_api_token }}"
    api_log_directory: /var/log/fics/api
```

---

### `process_window_object_data`

Calls the FICS Mortgage Servicer Service API to generate a mortgage payoff statement PDF for a specific loan. The output filename is derived from the borrower name, loan ID, and today's date in the format `<name>_<loan_id>_<YYYY-MM-DD>_payoff_statement.pdf`.

**FICS endpoint:** `ProcessWindowObjectData`

#### Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `dest` | str | Yes | Directory where the payoff statement PDF will be saved. Parent directories are created automatically. |
| `property_address` | str | Yes | Street address of the mortgaged property. |
| `loan_id` | int | Yes | FICS loan ID. |
| `loan_name` | str | Yes | Full name of the loan borrower/payee. |
| `city` | str | Yes | City of the mortgaged property. |
| `state` | str | Yes | State of the mortgaged property. |
| `zip` | str | Yes | ZIP/postal code of the mortgaged property. |
| `payoff_date` | str | Yes | Loan payoff date in `YYYY-MM-DD` format. |
| `core_api_url` | str | Yes | Base URL of the FICS Core Service API. |
| `api_token` | str | Yes | Bearer token used for API authentication. Never logged. |
| `api_log_directory` | str | No | Directory where API interaction logs will be written. |

#### Return Values

| Key | Type | Description |
|---|---|---|
| `msg` | str | Human-readable result message. |
| `changed` | bool | Always `false` — this module does not write local files directly. |
| `api_response` | dict | Full API response payload. |

#### Example

```yaml
- name: Generate payoff statement
  dtvillafana.fics.process_window_object_data:
    dest: /data/fics/payoff_statements/
    property_address: "123 Main St"
    loan_id: 100042
    loan_name: "Jane Doe"
    city: Springfield
    state: IL
    zip: "62701"
    payoff_date: "2026-08-01"
    core_api_url: https://fics.example.com/MortgageServicerService
    api_token: "{{ vault_fics_api_token }}"
    api_log_directory: /var/log/fics/api
```

---

### `run_late_notices_report`

Calls the FICS Mortgage Servicer Batch Service API to generate two PDFs: a Late Notices report and a Late Notices Summary report. The date range is automatically set to the past 365 days from the current date.

**FICS endpoint:** `RunLateNoticesReport`

#### Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `dest` | str | Yes | Full path where the Late Notices PDF will be written. Parent directories are created automatically. |
| `summary_dest` | str | Yes | Full path where the Late Notices Summary PDF will be written. Parent directories are created automatically. |
| `batch_service_api_url` | str | Yes | Base URL of the FICS Batch Service API. |
| `api_token` | str | Yes | Bearer token used for API authentication. Never logged. |
| `api_log_directory` | str | No | Directory where API interaction logs will be written. |

#### Example

```yaml
- name: Generate Late Notices and Summary reports
  dtvillafana.fics.run_late_notices_report:
    dest: /data/fics/reports/late_notices.pdf
    summary_dest: /data/fics/reports/late_notices_summary.pdf
    batch_service_api_url: https://fics.example.com/BatchService
    api_token: "{{ vault_fics_api_token }}"
    api_log_directory: /var/log/fics/api
```

---

## Authors

- Conrad Mercer ([@LoreMafore](https://github.com/LoreMafore)) — [conrad.mercer@capcu.org](mailto:conrad.mercer@capcu.org)
- David Villafaña — [david.villafana@capcu.org](mailto:david.villafana@capcu.org)

---

## License

This project is licensed under the [MIT License](LICENSE).

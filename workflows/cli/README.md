# GraphDB CLI Command Reference
[![Rust](https://img.shields.io/badge/Rust-CLI-orange?logo=rust&logoColor=white)](https://www.rust-lang.org)
[![Documentation](https://img.shields.io/badge/Commands-Detailed-blue)](./COMMANDS.md)
[![License](https://img.shields.io/badge/License-MIT-green)](./LICENSE)

This document provides the complete, authoritative reference for all commands, subcommands, and flags available in the GraphDB Command Line Interface (CLI).

## 📁 Table of Contents

* [1. Service & Lifecycle Management](#1-service--lifecycle-management)
* [2. Configuration, Data Storage, and Core Utilities](#2-configuration-data-storage-and-core-utilities)
* [3. Query, Graph, and Indexing](#3-query-graph-and-indexing)
* [4. History Management](#4-history-management)
* [5. Clinical Workflow and Patient Records](#5-clinical-workflow-and-patient-records)
* [6. Specialty Care and Diagnostics](#6-specialty-care-and-diagnostics)
* [7. Governance, Analytics, and Administration](#7-governance-analytics-and-administration)
* [8. General Utilities](#8-general-utilities)

***

## 1. Service & Lifecycle Management

These commands control the starting, stopping, status, and configuration of the core system components: `daemon`, `rest` (REST API), and `storage` (Storage Daemon).

| Command | Aliases | Subcommand/Action | Arguments/Flags | Description & Use Case |
| :--- | :--- | :--- | :--- | :--- |
| **start** | *(None)* | `daemon`, `rest`, `storage` | `--port <u16>`, `--cluster <NAME>`, `--daemon-port <u16>`, `--rest-port <u16>`, `--storage-port <u16>`, `--config-file <PATH>` | Initiates the specified service. Uses dedicated port flags for overrides. |
| | | `all` | *(Combines all flags)* | Starts all essential services simultaneously. |
| **stop** | *(None)* | `daemon`, `rest`, `storage` | `--port <u16>`, `-p <u16>` | Terminates the specified service running on the given port. |
| | | `all` | *(None)* | Stops all running services. |
| **status** | *(None)* | `summary` / `all` | *(None)* | Displays the health and operational status of all services. |
| | | `daemon`, `rest`, `storage` | `--port <u16>`, `--cluster <NAME>` | Gets the status of a specific service instance. |
| | | `cluster` | *(None)* | Gets the status of the entire cluster. |
| | | `raft` | `--port <u16>` | Displays the state of the **Raft consensus group** for distributed systems. |
| **reload** | *(None)* | `all`, `rest`, `storage`, `cluster` | `--port <u16>` (for `daemon` only) | Forces the specified component(s) to reload their configuration without stopping the process. |
| **restart** | *(None)* | `all`, `daemon`, `rest`, `storage` | *(Same flags as `start`)* | Stops and immediately restarts the specified service(s). |
| **daemon**, **rest**, **storage** | *(None)* | *(Implied start/stop/status)* | *(Varies)* | These act as aliases/shorthands for the respective `start`/`stop`/`status` subcommands. |

***

## 2. Configuration, Data Storage, and Core Utilities

These commands manage configuration settings, data persistence, key-value storage, migration, cleanup, and user authentication.

| Command | Aliases | Subcommand/Action | Arguments/Flags | Description & Use Case |
| :--- | :--- | :--- | :--- | :--- |
| **use** | *(None)* | `<ENGINE_TYPE>` | `<engine>` (e.g., `rocksdb`), `--permanent`, `--migrate` | Sets the default storage engine. `--migrate` moves existing data to the new engine. |
| | | `plugin` | `--enable <BOOL>` | Enables or disables experimental plugin functionality. |
| **save** | *(None)* | `storage` | *(None)* | Persists the current storage engine configuration. |
| | | `config` / `configuration` | *(None)* | Saves the active CLI configuration settings. |
| **show** | *(None)* | `storage-engine`, `config-path`, `version`, `history-stats`, `logs-path` | *(Varies)* | Displays various system properties, status, or file paths. |
| **kv** | *(None)* | `set`, `get`, `delete` | `<KEY> <VALUE>` | Manipulates raw key-value pairs in the underlying store. |
| **migrate** | *(None)* | *(Action)* | `[FROM] [TO]`, `--from <ENGINE>`, `--to <ENGINE>`, `--source`, `--dest` | Performs a comprehensive data migration between two storage engines. |
| **cleanup** | `clean` | `logs` | `--days-retention <i32>`, `--force` | Removes old log files. |
| | | `storage` | `<engine>`, `--force` | **DANGEROUS**: Clears persistent data within a specific storage engine. |
| | | `temporary-files`, `all` | `--days-retention <i32>`, `--force` | Removes cache/temporary files or runs all cleanup tasks. |
| | | `graph` |  | Removes orphaned edges and vertices, does garbage collection on graph. |
| **register** | *(None)* | `user` | `<USERNAME> <PASSWORD>` | Creates a new user account. |
| **auth** | `authenticate` | *(Action)* | `<USERNAME> <PASSWORD>` | User Session Authentication |
| **access** | *(None)* | `login`, `logout`, `whoami` | `[USERNAME]` | User Session Management |

***

## 3. Query, Graph, and Indexing

| Command | Aliases | Subcommand/Action | Arguments/Flags | Description & Use Case |
| :--- | :--- | :--- | :--- | :--- |
| **query** | `exec` | *(Action)* | `<QUERY_STRING>`, `--query <QUERY>`, `--language <LANG>` | Executes a graph query (Cypher, SQL, GraphQL). |
| **visualize** | *(None)* | *(Action)* | `<QUERY_STRING>`, `--query <QUERY>`, `--language <LANG>` | Executes a query and returns the result as a **graph visualization**. |
| **graph** | *(None)* | `insert-person` | `name=<NAME> age=<AGE> city=<CITY>` | Creates a sample `Person` node. |
| | | `delete`, `load`, `medical` | *(Varies)* | General graph manipulation operations. |
| **index** | *(None)* | `create` | `<Label> <property>` | Creates a standard B-Tree index. |
| | | `create fulltext` | `FULLTEXT <Label> <property>` | Creates a single-field full-text index. |
| | | `create-fulltext-index` | `<NAME> --labels <L1,L2> --properties <P1,P2>` | Creates a **multi-field, multi-label full-text index**. |
| | | `drop`, `drop-fulltext`, `search` | `<Label/NAME>`, `<QUERY>` | Drops indexes or executes a full-text search. |
| | | `rebuild`, `list`, `stats` | *(None)* | Manages index maintenance and reporting. |

***

## 4. History Management

| Command | Aliases | Subcommand/Action | Arguments/Flags | Description & Use Case |
| :--- | :--- | :--- | :--- | :--- |
| **history** | *(None)* | `list` | `--limit <N>`, `-n <N>`, `--since <TIME>`, `--until <TIME>`, `--status <STATUS>`, `--full-command`, `--verbose`, `--sort-by <FIELD>`, `--format <FMT>` | Lists command history, filtered and formatted. |
| | | `top` / `head`, `tail` / `bottom` | `<N>`, `--fail-only`, `--successful-only` | Shows the **N** most recent or oldest commands. |
| | | `window`, `stats`, `search` | `<DURATION>`, `--by <FIELD>`, `<KEYWORD>` | Analyzes history by time window, groups statistics, or searches content. |
| | | `clear` | `--user <USERNAME>`, `--force` | Clears command history (requires `--force`). |

***

## 5. Clinical Workflow and Patient Records

These commands are the foundation for managing patient visits, records, and orders in a healthcare setting.

| Command | Subcommand/Action | Arguments/Flags | Functional Domain |
| :--- | :--- | :--- | :--- |
| **patient** | `create`, `view`, `search`, `timeline` | `id`, `name`, `dob`, `mrn`, `--ssn`, `--mrn` | Patient Demographics & Records |
| **encounter** | `start`, `end`, `list`, `transfer` | `patient_id`, `location`, `reason` | Patient Visits and Stays (Inpatient/Outpatient) |
| **diagnosis** | `add`, `update`, `list` | `encounter_id`, `description`, `--icd10`, `--status` | Disease and Condition Management |
| **problem** | `add`, `update`, `resolve`, `list` | `patient_id`, `problem`, `--icd10`, `--severity`, `--onset-date` | Active and Resolved Patient Problems |
| **prescription** | `add`, `checkinteractions`, `update` | `encounter_id`, `med_name`, `dose`, `--refills`, `--route`, `--duration` | Medication Ordering and Safety Checks |
| **note** | `add`, `list` | `patient_id`, `author_id`, `text`, `--note-type` | Clinical Documentation and Charting |
| **order** | `admit`, `lab`, `imaging`, `medication` | `encounter_id`, *(specific order args)*, `--priority`, `--notes` | Physician/Provider Orders |
| **procedure** | `order`, `perform`, `result`, `analytics` | `encounter_id`, `cpt_code`, `--scheduled-date`, `--location` | Surgical and Diagnostic Procedure Management |
| **vitals** | `add`, `list` | `encounter_id`, `--bp`, `--hr`, `--temp`, `--pain-score`, `--spo2`, `--weight` | Patient Physiological Data Recording |
| **observation** | `add`, `list` | `encounter_id`, `type`, `value`, `unit`, `--method` | General Clinical Observations |
| **triage** | `assign`, `update`, `queue` | `patient_id`, `acuity`, `--priority` | Patient Acuity Assessment (e.g., ED) |
| **disposition**| `transfer`, `admit`, `discharge` | `patient_id`, `location`, `status` | Patient Location and Status Tracking |
| **referral** | `create`, `pending`, `complete` | `patient_id`, `specialty`, `urgency` | Management of Internal and External Referrals |
| **nursing** | `task`, `document`, `shift-report` | `patient_id`, `task_type`, `notes` | Nursing Workflow and Documentation |
| **education** | `add`, `document`, `list-material` | `patient_id`, `topic`, `--method` | Patient and Family Education |
| **discharge** | `plan`, `readiness`, `summary`, `finalize` | `patient_id`, `--actual-date`, `disposition` | Patient Discharge Process and Planning |
| **discharge-planning**| `update`, `barrier`, `post-acute` | `patient_id`, `status` | Detailed Discharge Readiness Management |

***

## 6. Specialty Care and Diagnostics

| Command | Subcommand/Action | Arguments/Flags | Functional Domain |
| :--- | :--- | :--- | :--- |
| **dosing** | `calculate`, `verify`, `vancomycin`, `warfarin` | `patient_id`, `medication`, `--weight-kg`, `--creatinine`, `--age` | Advanced Pharmacokinetic Dosing and Verification |
| **lab** | `order`, `result`, `history` | `encounter_id`, `tests`, `--priority`, `--clinical-indication` | Laboratory Test Management |
| **imaging** | `order`, `view`, `report` | `encounter_id`, `study`, `--modality`, `--priority` | Radiology and Imaging Study Management |
| **pathology** | `register`, `add-result`, `search` | `specimen_id`, `test`, `variant`, `--interpretation` | Tissue and Molecular Pathology |
| **microbiology** | `culture`, `preliminary`, `final` | `culture_id`, `organism`, `--sensitivities`, `--source` | Infectious Disease and Culture Results |
| **chemo** | `regimen`, `cycle-add`, `toxicity` | `patient_id`, `regimen_name`, `grade` | Chemotherapy Planning and Tracking |
| **radiation** | `plan`, `deliver`, `review` | `patient_id`, `site`, `total_dose_gy`, `--fractions` | Radiation Oncology Planning and Delivery |
| **surgery** | `schedule`, `pre-op`, `intra-op`, `post-op` | `procedure_id`, `provider`, `--location` | Comprehensive Surgical Workflow |

***

## 7. Governance, Analytics, and Administration

| Command | Subcommand/Action | Arguments/Flags | Functional Domain |
| :--- | :--- | :--- | :--- |
| **financial** | `service-line` | `<SERVICE_NAME>` | Displays financial performance by service line. |
| | `payer-mix`, `denials` | *(None)* | Financial and Revenue Cycle Analysis. |
| **alert** | `create`, `list`, `ack`, `dashboard` | `type`, `message`, `--severity`, `--target-user` | System and Clinical Alert Management |
| **metrics** | `clinical` | `sepsis`, `stroke`, `vtp`, `pain`, `fall`, `readmission` | Calculates and displays **Clinical Quality Metrics**. |
| | `operational` | *(Varies)* | Calculates system and process operational metrics. |
| **quality** | `measure`, `audit`, `report` | `<MEASURE_ID>`, `--timeframe`, `--facility` | Healthcare Quality Improvement |
| **incident** | `report`, `review`, `close` | `type`, `severity`, `--patient-id` | Safety Incident and Event Reporting |
| **compliance** | `check`, `report`, `audit` | `<REGULATION>`, `--scope`, `--force-audit` | Regulatory Compliance Auditing |
| **population** | `query`, `segment`, `list` | `cohort_name`, `criteria` | Population Health Management |
| **research** | `query`, `export`, `anonymize` | `protocol_id`, `--data-set`, `--level` | Clinical and Biomedical Research Data |
| **ml** | `train`, `predict`, `deploy` | `model_id`, `data_source`, `feature_set` | Machine Learning Model Lifecycle |
| **model** | `list`, `deploy`, `retire` | `model_name`, `--version`, `--path` | AI/ML Model Catalog and Management |
| **clinical-trial**| `enroll`, `track`, `report` | `trial_id`, `patient_id`, `arm` | Management of Clinical Trials |
| **facility** | `add`, `list`, `config` | `name`, `address`, `capacity` | Hospital and Facility Configuration |

***

## 8. General Utilities

| Command | Aliases | Description & Usage |
| :--- | :--- | :--- |
| **interactive** | *(None)* | Launches the multi-session interactive CLI environment. |
| **version** | *(None)* | Displays the version of the application. |
| **health** | *(None)* | Reports the overall health status of the application. |
| **clear** | `clean` | Clears the interactive console screen. |
| **help** | *(None)* | Displays this command reference or detailed help for a specific command path. |
| **exit** | `quit`, `q` | Terminates the CLI session. |


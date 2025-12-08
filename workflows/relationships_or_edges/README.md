# GraphDB Medical Data Model – Edge Relationships Reference
![Rust](https://img.shields.io/badge/Rust-Model-orange?logo=rust&logoColor=white)
![Graph](https://img.shields.io/badge/Graph-Property_Graph-blue)
![Healthcare](https://img.shields.io/badge/Domain-Healthcare-green)

This document provides a **complete, authoritative reference** of all meaningful relationships (edges) that should exist in the GraphDB medical knowledge graph, derived from the full set of clinical and non-clinical models.

These relationships are used throughout the system for:
- Clinical reasoning & decision support
- MPI (Master Patient Index) operations
- Patient journey reconstruction
- Analytics, reporting, and population health
- Interoperability (FHIR/HL7 mapping)

## Table of Contents
- [Core Patient Relationships](#core-patient-relationships)
- [Encounter & Care Delivery](#encounter--care-delivery)
- [Clinical Documentation & Observations](#clinical-documentation--observations)
- [Diagnostics & Procedures](#diagnostics--procedures)
- [Medications & Pharmacy](#medications--pharmacy)
- [Administrative & Financial](#administrative--financial)
- [Emergency & Acute Care](#emergency--acute-care)
- [Organizational Structure](#organizational-structure)
- [Identity & Access](#identity--access)

---

## Core Patient Relationships

| Edge Label                  | Direction (From → To)         | Description & Purpose                                                                                  | Key Fields / Linkage |
|----------------------------|-------------------------------|--------------------------------------------------------------------------------------------------------|----------------------|
| `HAS_ALLERGY`              | Patient → Allergy             | Patient has a known allergy (critical for safety checks)                                              | `Allergy.patient_id` |
| `HAS_PROBLEM`              | Patient → Problem             | Active or resolved problems (problem list)                                                             | `Problem.patient_id` |
| `HAS_DIAGNOSIS`            | Patient → Diagnosis           | Diagnoses assigned to the patient                                                                      | `Diagnosis.patient_id` |
| `HAS_IMMUNIZATION`         | Patient → Immunization        | Vaccination history                                                                                     | `Immunization.patient_id` |
| `HAS_PRESCRIPTION`         | Patient → Prescription        | Medications prescribed to the patient                                                                  | `Prescription.patient_id` |
| `HAS_VITALS`               | Patient → Vitals              | Vital signs recorded for the patient                                                                   | `Vitals.patient_id` |
| `HAS_SOCIAL_DETERMINANT`   | Patient → SocialDeterminant   | Social determinants of health (SDOH) impacting care                                                    | `SocialDeterminant.patient_id` |
| `HAS_ADDRESS`              | Patient → Address             | Patient's physical or mailing address                                                                 | `Patient.address_id` → `Address.id` |
| `HAS_MASTER_INDEX`         | Patient → MasterPatientIndex  | Links patient to their MPI record for identity resolution                                             | `MasterPatientIndex.patient_id` |
| `HAS_BILLING_ADDRESS`      | Patient → BillingAddress      | Billing-specific address (may differ from clinical)                                                    | `BillingAddress.patient_id` |
| `HAS_INSURANCE`            | Patient → Insurance           | Primary/secondary insurance coverage (inferred via claims or patient fields)                          | via `Claim`, `Patient.primary_insurance_id` |

---

## Encounter & Care Delivery

| Edge Label             | Direction                     | Description & Purpose                                                                 | Key Fields |
|------------------------|-------------------------------|---------------------------------------------------------------------------------------|------------|
| `HAS_ENCOUNTER`        | Patient → Encounter           | All visits/admissions belong to a patient                                            | `Encounter.patient_id` |
| `HAS_ENCOUNTER`        | Doctor → Encounter            | Attending/primary physician for the encounter                                        | `Encounter.doctor_id` |
| `HAS_APPOINTMENT`      | Patient → Appointment         | Scheduled outpatient or follow-up visits                                             | `Appointment.patient_id` |
| `HAS_DISPOSITION`      | Encounter → Disposition       | Final outcome of the encounter (admit, discharge, transfer, expired)                 | `Disposition.encounter_id` |
| `HAS_TRIAGE`           | Encounter → Triage            | Emergency department triage assessment                                               | `Triage.encounter_id` |
| `HAS_REFERRAL`         | Encounter → Referral          | Referrals generated during the encounter                                             | `Referral.encounter_id` |
| `HAS_ORDER`            | Encounter → Order             | All orders (labs, imaging, meds, etc.) placed during encounter                       | `Order.encounter_id` |

---

## Clinical Documentation & Observations

| Edge Label               | Direction                      | Description & Purpose                                                              | Key Fields |
|--------------------------|--------------------------------|------------------------------------------------------------------------------------|------------|
| `HAS_CLINICAL_NOTE`      | Patient → ClinicalNote         | Progress notes, H&P, consults                                                      | `ClinicalNote.patient_id` |
| `HAS_CLINICAL_NOTE`      | Doctor → ClinicalNote          | Authorship of note                                                                 | `ClinicalNote.doctor_id` |
| `HAS_OBSERVATION`        | Encounter → Observation        | Labs, vitals, assessments recorded during encounter                               | `Observation.encounter_id` |
| `HAS_OBSERVATION`        | Patient → Observation          | Longitudinal observation history                                                  | `Observation.patient_id` |
| `HAS_ED_EVENT`           | Encounter → EdEvent            | Real-time events in emergency department (med admin, consults, etc.)              | `EdEvent.encounter_id` |
| `HAS_ED_PROCEDURE`       | Encounter → EdProcedure        | Procedures performed in ED                                                         | `EdProcedure.encounter_id` |

---

## Diagnostics & Procedures

| Edge Label               | Direction                        | Description & Purpose                                                              | Key Fields |
|--------------------------|----------------------------------|------------------------------------------------------------------------------------|------------|
| `HAS_DIAGNOSIS`          | Doctor → Diagnosis               | Doctor who made the diagnosis                                                      | `Diagnosis.doctor_id` |
| `HAS_MEDICAL_CODE`       | Diagnosis → MedicalCode          | ICD-10 / SNOMED code for diagnosis                                                 | `Diagnosis.code_id` |
| `HAS_PROCEDURE_CODE`     | EdProcedure → MedicalCode        | CPT/HCPCS code for billing/standardization                                         | `EdProcedure.procedure_code_id` |
| `HAS_ED_PROCEDURE`       | Doctor → EdProcedure             | Performing physician                                                               | `EdProcedure.performed_by_doctor_id` |
| `HAS_ED_PROCEDURE`       | Nurse → EdProcedure              | Assisting nurse                                                                    | `EdProcedure.assist_nurse_id` |

---

## Medications & Pharmacy

| Edge Label                  | Direction                           | Description & Purpose                                                              | Key Fields |
|-----------------------------|-------------------------------------|------------------------------------------------------------------------------------|------------|
| `HAS_PRESCRIPTION`          | Doctor → Prescription               | Prescriber of medication                                                           | `Prescription.doctor_id` |
| `HAS_DOSAGE`                | Medication → Dosage                 | Dosage instructions for a medication                                              | `Dosage.medication_id` |
| `HAS_SIDE_EFFECT`           | Medication → SideEffect             | Known adverse reactions                                                            | `SideEffect.medication_id` |
| `HAS_MEDICAL_INTERACTION`   | Medication → MedicalInteraction    | Drug-drug or drug-food interactions                                                | `primary_medication_id`, `secondary_medication_id` |
| `HAS_REFILL`                | Prescription → Refill               | Refill requests and fulfillment                                                    | `Refill.prescription_id` |
| `HAS_INTEGRATION`           | Pharmacy → PharmacyIntegration      | e-Prescribing or fulfillment integration                                          | `PharmacyIntegration.pharmacy_id` |

---

## Administrative & Financial

| Edge Label             | Direction                     | Description & Purpose                                                              | Key Fields |
|------------------------|-------------------------------|------------------------------------------------------------------------------------|------------|
| `HAS_CLAIM`            | Patient → Claim               | Insurance claims submitted for patient                                             | `Claim.patient_id` |
| `HAS_CLAIM`            | Insurance → Claim             | Payer receiving the claim                                                          | `Claim.insurance_id` |
| `HAS_FACILITY_UNIT`    | Department → FacilityUnit     | Physical units (ICU, ED, Ward) within a department                                 | `FacilityUnit.department_id` |
| `HAS_DEPARTMENT`       | Hospital → Department         | Organizational structure                                                           | `Department.hospital_id` |
| `HAS_HEAD`             | Department → User             | Head of department                                                                 | `head_of_department_user_id` |
| `HAS_ADMIN_CONTACT`    | Hospital → User               | Administrative contact                                                             | `admin_contact_user_id` |

---

## Emergency & Acute Care

| Edge Label             | Direction                     | Description & Purpose                                                              | Key Fields |
|------------------------|-------------------------------|------------------------------------------------------------------------------------|------------|
| `HAS_TRIAGE`           | Nurse → Triage                | Nurse who performed triage                                                        | `Triage.triage_nurse_id` |
| `HAS_ED_EVENT`         | User → EdEvent                | Who recorded the ED event                                                          | `EdEvent.recorded_by_user_id` |
| `HAS_OBSERVATION`      | User → Observation            | Clinician who recorded observation                                                 | `Observation.observed_by_user_id` |

---

## Organizational Structure

| Edge Label                  | Direction                           | Description & Purpose                                                              | Key Fields |
|-----------------------------|-------------------------------------|------------------------------------------------------------------------------------|------------|
| `HAS_STAFF_ASSIGNMENT`      | User → StaffAssignment             | Role and location assignment                                                       | `StaffAssignment.user_id` |
| `HAS_STAFF_ASSIGNMENT`      | Hospital → StaffAssignment         | Staff assigned to hospital                                                         | `hospital_id` |
| `HAS_STAFF_ASSIGNMENT`      | Department → StaffAssignment        | Department-level assignment                                                       | `department_id` |
| `HAS_STAFF_ASSIGNMENT`      | FacilityUnit → StaffAssignment      | Unit-level assignment (e.g., ED nurse)                                             | `facility_unit_id` |
| `HAS_ASSIGNED_ROLE`         | StaffAssignment → Role              | Role within the assignment                                                        | `assigned_role_id` |
| `HAS_NURSE`                 | User → Nurse                        | User has nurse specialization                                                     | `Nurse.user_id` |

---

## Identity & Access

| Edge Label             | Direction                     | Description & Purpose                                                              | Key Fields |
|------------------------|-------------------------------|------------------------------------------------------------------------------------|------------|
| `HAS_USER`             | Patient → User                | Patient has a portal/user account                                                  | `Patient.user_id` |
| `HAS_ROLE`             | User → Role                   | User has security role                                                             | `User.role_id` |
| `HAS_PRIMARY_DOCTOR`   | Patient → Doctor              | Patient's primary care provider                                                    | `Patient.primary_care_provider_id` |

---

## Legend & Best Practices

| Symbol             | Meaning |
|--------------------|--------|
| →                  | Directed edge (outbound → inbound) |
| `HAS_*`            | Ownership / containment (most common) |
| `IS_*`             | Type hierarchy or classification (less common) |
| Bold labels        | Critical for patient safety or core workflows |

> **Recommendation**: Use consistent edge labeling (`HAS_X`, `PERFORMED`, `RECORDED_BY`, etc.) across all services to enable powerful graph queries (e.g., "All active problems for patients with allergy to penicillin seen in last 30 days").

This schema supports:
- Real-time clinical decision support
- Master Patient Index (MPI) merging & linking
- Full patient journey reconstruction
- Population health analytics
- Regulatory reporting (MIPS, Joint Commission)

**Last Updated**: December 2025  
**License**: MIT

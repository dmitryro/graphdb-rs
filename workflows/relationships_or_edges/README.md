# GraphDB Medical Data Model – Complete Edge Relationship Reference
![Rust](https://img.shields.io/badge/Rust-Model-orange?logo=rust&logoColor=white)
![Graph](https://img.shields.io/badge/Graph-Property_Graph-blue)
![Healthcare](https://img.shields.io/badge/Domain-Healthcare-green)
![SNOMED](https://img.shields.io/badge/Ontology-SNOMED_CT-blueviolet)

**Authoritative, production-ready list of all clinical and non-clinical relationships (edges)** used in the GraphDB healthcare knowledge graph.

This reference includes:
- All relationships derived from existing models
- Standard clinical ontology patterns (`IS_A`, `PART_OF`, `CAUSES`, `TREATS`)
- Workflow & governance relationships (`REQUIRES_APPROVAL_BY`, `MUST_NOTIFY`, `PARTICIPATES_IN`)
- Drug interaction & pharmacovigilance relationships
- Organizational hierarchies (`REPORTS_TO`, `SUPERVISES`)

Used for: Clinical Decision Support • MPI • Patient Journey • Analytics • FHIR/HL7 Mapping

---

## Core Patient Relationships

| Edge Label                  | Direction (From → To)             | Description & Clinical Purpose                                                      | Source Field / Example |
|----------------------------|-----------------------------------|-------------------------------------------------------------------------------------|-------------------------|
| `HAS_ALLERGY`              | Patient → Allergy                 | Critical allergy (e.g., penicillin) – used in CDS alerts                            | `Allergy.patient_id` |
| `HAS_PROBLEM`              | Patient → Problem                 | Active problem list (e.g., Diabetes Type 2)                                         | `Problem.patient_id` |
| `HAS_DIAGNOSIS`            | Patient → Diagnosis               | Confirmed diagnoses (e.g., Acute Myocardial Infarction)                             | `Diagnosis.patient_id` |
| `HAS_IMMUNIZATION`         | Patient → Immunization            | Vaccine history (e.g., COVID-19, Flu)                                               | `Immunization.patient_id` |
| `HAS_PRESCRIPTION`         | Patient → Prescription            | Active and historical medications                                                   | `Prescription.patient_id` |
| `HAS_VITALS`               | Patient → Vitals                  | Vital signs over time (BP, HR, SpO2, etc.)                                          | `Vitals.patient_id` |
| `HAS_SOCIAL_DETERMINANT`   | Patient → SocialDeterminant       | SDOH factors (housing instability, food insecurity)                                | `SocialDeterminant.patient_id` |
| `HAS_ADDRESS`              | Patient → Address                 | Current residence                                                                           | `Patient.address_id` |
| `HAS_BILLING_ADDRESS`      | Patient → BillingAddress          | Payer/billing address (may differ from clinical)                                   | `BillingAddress.patient_id` |
| `HAS_MASTER_INDEX`         | Patient → MasterPatientIndex      | MPI golden record linkage                                                           | `MasterPatientIndex.patient_id` |
| `HAS_INSURANCE`            | Patient → Insurance               | Primary/secondary coverage                                                          | via `Claim` or patient fields |

---

## Encounter & Care Delivery

| Edge Label             | Direction                        | Description                                                                   | Source |
|------------------------|----------------------------------|-------------------------------------------------------------------------------|--------|
| `HAS_ENCOUNTER`        | Patient → Encounter              | All inpatient/outpatient visits                                               | `Encounter.patient_id` |
| `HAS_ENCOUNTER`        | Doctor → Encounter               | Attending/consulting physician                                               | `Encounter.doctor_id` |
| `HAS_APPOINTMENT`      | Patient → Appointment            | Scheduled visits                                                              | `Appointment.patient_id` |
| `HAS_DISPOSITION`      | Encounter → Disposition          | Final outcome (Admit, Discharge, Transfer, Expired)                          | `Disposition.encounter_id` |
| `HAS_TRIAGE`           | Encounter → Triage               | ED acuity assessment                                                         | `Triage.encounter_id` |
| `HAS_REFERRAL`         | Encounter → Referral             | Specialist referral                                                           | `Referral.encounter_id` |
| `HAS_ORDER`            | Encounter → Order                | All orders placed during encounter (labs, meds, imaging)                     | `Order.encounter_id` |

---

## Clinical Documentation & Observations

| Edge Label               | Direction                        | Description                                                                   | Source |
|--------------------------|----------------------------------|-------------------------------------------------------------------------------|--------|
| `HAS_CLINICAL_NOTE`      | Patient → ClinicalNote           | Progress notes, H&P, consult notes                                            | `ClinicalNote.patient_id` |
| `HAS_CLINICAL_NOTE`      | Doctor → ClinicalNote            | Authorship                                                                    | `ClinicalNote.doctor_id` |
| `HAS_OBSERVATION`        | Encounter → Observation          | Labs, vitals, assessments                                                     | `Observation.encounter_id` |
| `HAS_OBSERVATION`        | Patient → Observation            | Longitudinal trends                                                           | `Observation.patient_id` |
| `HAS_ED_EVENT`           | Encounter → EdEvent              | Real-time ED events (med admin, consult, code)                                | `EdEvent.encounter_id` |
| `HAS_ED_PROCEDURE`       | Encounter → EdProcedure          | Procedures in ED (intubation, central line)                                   | `EdProcedure.encounter_id` |

---

## Diagnostics & Procedures

| Edge Label               | Direction                        | Description                                                                   | Source |
|--------------------------|----------------------------------|-------------------------------------------------------------------------------|--------|
| `HAS_DIAGNOSIS`          | Doctor → Diagnosis               | Diagnosing clinician                                                         | `Diagnosis.doctor_id` |
| `HAS_MEDICAL_CODE`       | Diagnosis → MedicalCode          | ICD-10/SNOMED coding                                                          | `Diagnosis.code_id` |
| `HAS_PROCEDURE_CODE`     | EdProcedure → MedicalCode        | CPT/HCPCS billing code                                                        | `EdProcedure.procedure_code_id` |
| `HAS_ED_PROCEDURE`       | Doctor → EdProcedure             | Performing physician                                                          | `EdProcedure.performed_by_doctor_id` |
| `HAS_ED_PROCEDURE`       | Nurse → EdProcedure              | Assisting nurse                                                               | `EdProcedure.assist_nurse_id` |

---

## Medications & Pharmacy

| Edge Label                  | Direction                        | Description                                                                   | Source |
|-----------------------------|----------------------------------|-------------------------------------------------------------------------------|--------|
| `HAS_PRESCRIPTION`          | Doctor → Prescription            | Prescriber                                                                    | `Prescription.doctor_id` |
| `HAS_DOSAGE`                | Medication → Dosage              | Dosing instructions (e.g., 500mg BID)                                         | `Dosage.medication_id` |
| `HAS_SIDE_EFFECT`           | Medication → SideEffect          | Known adverse reaction                                                        | `SideEffect.medication_id` |
| `HAS_MEDICAL_INTERACTION`   | Medication → MedicalInteraction  | Drug-drug/food interaction                                                   | `primary/secondary_medication_id` |
| `HAS_REFILL`                | Prescription → Refill            | Refill requests                                                               | `Refill.prescription_id` |
| `HAS_INTEGRATION`           | Pharmacy → PharmacyIntegration   | e-Prescribing connection                                                      | `PharmacyIntegration.pharmacy_id` |

---

## Administrative & Financial

| Edge Label             | Direction                        | Description                                                                   | Source |
|------------------------|----------------------------------|-------------------------------------------------------------------------------|--------|
| `HAS_CLAIM`            | Patient → Claim                  | Submitted insurance claim                                                    | `Claim.patient_id` |
| `HAS_CLAIM`            | Insurance → Claim                | Payer                                                                         | `Claim.insurance_id` |
| `HAS_FACILITY_UNIT`    | Department → FacilityUnit        | Physical units (ICU, Ward 4B)                                                 | `FacilityUnit.department_id` |
| `HAS_DEPARTMENT`       | Hospital → Department            | Organizational unit                                                           | `Department.hospital_id` |
| `HAS_HEAD`             | Department → User                | Department chair                                                              | `head_of_department_user_id` |
| `HAS_ADMIN_CONTACT`    | Hospital → User                  | Administrative contact                                                        | `admin_contact_user_id` |

---

## Emergency & Acute Care

| Edge Label             | Direction                        | Description                                                                   | Source |
|------------------------|----------------------------------|-------------------------------------------------------------------------------|--------|
| `HAS_TRIAGE`           | Nurse → Triage                   | Triage performer                                                              | `Triage.triage_nurse_id` |
| `HAS_ED_EVENT`         | User → EdEvent                   | Event recorder (nurse, doctor)                                                | `EdEvent.recorded_by_user_id` |
| `HAS_OBSERVATION`      | User → Observation               | Clinician who recorded                                                        | `Observation.observed_by_user_id` |

---

## Organizational Hierarchy

| Edge Label                  | Direction                        | Description                                                                   | Source |
|-----------------------------|----------------------------------|-------------------------------------------------------------------------------|--------|
| `HAS_STAFF_ASSIGNMENT`      | User → StaffAssignment           | Current role & location                                                       | `StaffAssignment.user_id` |
| `HAS_STAFF_ASSIGNMENT`      | Hospital → StaffAssignment      | Hospital-level assignment                                                     | `hospital_id` |
| `HAS_STAFF_ASSIGNMENT`      | Department → StaffAssignment     | Department assignment                                                         | `department_id` |
| `HAS_STAFF_ASSIGNMENT`      | FacilityUnit → StaffAssignment   | Unit assignment (e.g., ED nurse)                                              | `facility_unit_id` |
| `HAS_ASSIGNED_ROLE`         | StaffAssignment → Role           | Role in assignment                                                            | `assigned_role_id` |
| `REPORTS_TO`                | User → User                      | Direct supervisor (e.g., Resident → Attending)                                | via `StaffAssignment` |
| `SUPERVISES`                | User → User                      | Reverse of REPORTS_TO                                                         | bidirectional |

---

## Ontology & Hierarchical Relationships (SNOMED CT Inspired)

| Edge Label             | Direction                        | Description                                                                   | Example |
|------------------------|----------------------------------|-------------------------------------------------------------------------------|---------|
| `IS_A`                 | Subtype → Supertype              | Taxonomic hierarchy (inheritance)                                             | `Cellulitis IS_A BacterialInfection` |
| `PART_OF`              | Part → Whole                     | Anatomical or organizational composition                                      | `Heart PART_OF CardiovascularSystem` |
| `HAS_FINDING_SITE`     | Finding → BodyStructure          | Location of pathology                                                         | `Pneumonia HAS_FINDING_SITE Lung` |
| `HAS_CAUSATIVE_AGENT`  | Disorder → Agent                 | Etiology                                                                      | `Cellulitis HAS_CAUSATIVE_AGENT Streptococcus` |

---

## Clinical Workflow & Governance

| Edge Label                  | Direction                        | Description                                                                   | Example |
|-----------------------------|----------------------------------|-------------------------------------------------------------------------------|---------|
| `PARTICIPATES_IN`           | User → Task/Workflow             | Role in process                                                               | `Nurse PARTICIPATES_IN Triage` |
| `REQUIRES_APPROVAL_BY`      | Order/Task → User/Role           | Mandatory approval gate                                                       | `ControlledSubstance REQUIRES_APPROVAL_BY Pharmacist` |
| `MUST_NOTIFY`               | Event → User/Role                | Mandatory notification/escalation                                             | `CriticalLabResult MUST_NOTIFY Attending` |
| `PRECEDES`                  | Task A → Task B                  | Workflow sequence                                                             | `Triage PRECEDES InitialAssessment` |
| `TRIGGERS`                  | Event → Action                   | Automatic workflow trigger                                                    | `PositiveTroponin TRIGGERS CardiologyConsult` |

---

## Drug Interactions & Clinical Semantics

| Edge Label                  | Direction                        | Description                                                                   | Example |
|-----------------------------|----------------------------------|-------------------------------------------------------------------------------|---------|
| `CAUSES`                    | Drug/Agent → Condition           | Adverse effect or disease causation                                           | `NSAID CAUSES Gastritis` |
| `TREATS`                    | Drug/Procedure → Condition       | Therapeutic relationship                                                      | `Metformin TREATS DiabetesType2` |
| `INDICATED_FOR`             | Drug → Indication                | Approved clinical use                                                         | `Aspirin INDICATED_FOR MyocardialInfarctionPrevention` |
| `CONTRAINDICATED_IN`        | Drug → Condition                 | Absolute contraindication                                                    | `Warfarin CONTRAINDICATED_IN ActiveBleeding` |
| `CONTRAINDICATED_WITH`      | Drug A → Drug B                  | Drug-drug contraindication                                                   | `MAOI CONTRAINDICATED_WITH SSRI` |
| `HAS_SEVERITY`              | Interaction → SeverityLevel      | Risk classification (Minor, Moderate, Major)                                 | `Interaction HAS_SEVERITY Major` |
| `HAS_MECHANISM`             | Interaction → Mechanism          | Pharmacological mechanism                                                     | `DDI HAS_MECHANISM CYP3A4Inhibition` |

---

## Identity & Access

| Edge Label             | Direction                        | Description                                                                   | Source |
|------------------------|----------------------------------|-------------------------------------------------------------------------------|--------|
| `HAS_USER`             | Patient → User                   | Patient portal account                                                        | `Patient.user_id` |
| `HAS_ROLE`             | User → Role                      | Security & access control                                                     | `User.role_id` |
| `HAS_PRIMARY_DOCTOR`   | Patient → Doctor                 | Primary Care Provider (PCP)                                                   | `Patient.primary_care_provider_id` |

---

## Summary Legend

| Type             | Prefix Pattern         | Use Case |
|------------------|------------------------|----------|
| Ownership        | `HAS_*`                | Containment & clinical history |
| Hierarchy        | `IS_A`, `PART_OF`      | Ontology & inheritance |
| Causation        | `CAUSES`, `TREATS`     | Clinical reasoning |
| Workflow         | `REQUIRES_*`, `TRIGGERS`| Process automation |
| Governance       | `MUST_NOTIFY`, `REPORTS_TO` | Escalation & accountability |

**Last Updated**: December 08, 2025  
**License**: MIT  
**Recommended for**: EHRs • CDSS • Population Health • Research • Interoperability
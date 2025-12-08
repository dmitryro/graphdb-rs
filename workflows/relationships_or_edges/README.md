# GraphDB Medical Data Model – **Complete & Exhaustive Edge Relationship Reference**  
**Full Clinical, Acute Care, Emergency, Laboratory, Imaging, MPI, Patient Journey, Drug Interactions, SNOMED CT, FHIR/HL7 Coverage**  

![Rust](https://img.shields.io/badge/Rust-Model-orange?logo=rust&logoColor=white)
![Graph](https://img.shields.io/badge/Graph-Property_Graph-blue)
![Healthcare](https://img.shields.io/badge/Domain-Healthcare-green)
![SNOMED](https://img.shields.io/badge/Ontology-SNOMED_CT-blueviolet)
![FHIR](https://img.shields.io/badge/Interop-FHIR_|_HL7-yellow)
![Acute Care](https://img.shields.io/badge/Specialty-Acute_|_Emergency-red)
![Lab](https://img.shields.io/badge/Domain-Laboratory_|_Pathology-purple)
![Imaging](https://img.shields.io/badge/Domain-Radiology_|_Imaging-teal)

**This is the definitive, exhaustive, production-grade reference** of **every meaningful relationship (edge)** in the GraphDB healthcare knowledge graph — **no shortcuts, no omissions**.

Fully covers:
- Master Patient Index (MPI) & Identity Resolution
- Clinical Notes & Structured Documentation
- Full Patient Journey & Care Continuum
- Laboratory Medicine (Chemistry, Hematology, Microbiology, Pathology)
- Diagnostic Imaging (X-Ray, CT, MRI, PET, US, Mammography, Interventional)
- Acute Care & Emergency Medicine (Trauma, Stroke, Sepsis, Cardiac Arrest)
- Procedures & High-Risk Interventions
- Drug Interactions & Pharmacovigilance
- SNOMED CT Semantic Relationships
- FHIR & HL7 Interoperability Mapping

Used in: Real-time CDSS • Trauma Registry • Stroke Alert • Sepsis Bundle • Critical Lab Notification • Population Health • Research

---

## 1. Master Patient Index (MPI) & Identity Management

| Edge Label                        | Direction                                  | Description & Clinical Purpose                                                                                 | Source Field / FHIR Mapping |
|-----------------------------------|--------------------------------------------|---------------------------------------------------------------------------------------------------------------|-----------------------------|
| `HAS_MPI_RECORD`                  | Patient → MasterPatientIndex               | Links local patient record to the enterprise golden record (MPI)                                              | `Patient.link` → `Patient` |
| `IS_LINKED_TO`                    | Patient A → Patient B                      | Probabilistic soft link — candidate duplicate before human review                                            | `Patient.link.type = "seealso"` |
| `IS_MERGED_INTO`                  | SourcePatient → TargetPatient              | Permanent identity merge — source record retired, all data migrated                                           | `Patient.link.type = "replaced-by"` |
| `IS_SPLIT_FROM`                   | NewPatient → OriginalPatient               | Reverse of erroneous merge — identity split                                                                   | Audit trail |
| `HAS_POTENTIAL_DUPLICATE`         | Patient A → Patient B                      | Temporary flag during automated matching workflow                                                             | Internal MPI engine |
| `RESOLVED_BY`                     | MPI_Conflict → User                        | Clinician or data steward who resolved duplicate/merge conflict                                               | Audit trail |
| `HAS_MATCH_SCORE`                 | MasterPatientIndex → MatchScoreVertex      | Stores probabilistic match confidence (0.0 – 1.0) from Fellegi-Sunter or ML model                             | `match_score` field |
| `HAS_BLOCKING_KEY`                | Patient → BlockingKeyVertex                | Deterministic keys used in blocking phase (e.g., Soundex(LastName)+DOB)                                        | Internal |
| `HAS_SURVIVORSHIP_RULE`           | MasterPatientIndex → SurvivorshipRule      | Which source field wins during merge (e.g., "Most recent MRN", "Longest name")                                | Governance |

---

## 2. Clinical Notes & Structured Documentation

| Edge Label                        | Direction                                  | Description & Clinical Purpose                                                                                 | FHIR Mapping |
|-----------------------------------|--------------------------------------------|---------------------------------------------------------------------------------------------------------------|--------------|
| `HAS_CLINICAL_NOTE`               | Patient → ClinicalNote                     | All clinical documentation (H&P, Progress, Consult, Discharge Summary)                                        | `Composition` |
| `HAS_CLINICAL_NOTE`               | Encounter → ClinicalNote                   | Notes tied to specific admission/visit                                                                        | `Composition.encounter` |
| `AUTHORED_BY`                     | ClinicalNote → User (Doctor/NP/PA)         | Signing/authoring provider                                                                                     | `Composition.author` |
| `COSIGNED_BY`                     | ClinicalNote → User                        | Attending co-signature (resident notes)                                                                        | `Composition.attester` |
| `HAS_SECTION`                     | ClinicalNote → SectionVertex               | Structured sections: Chief Complaint, HPI, ROS, PE, A/P, etc.                                                  | `Composition.section` |
| `HAS_SUBSECTION`                  | Section → Subsection                       | Nested structure (e.g., A/P → Differential Diagnosis)                                                         | `section.section` |
| `MENTIONS_CONCEPT`                | ClinicalNote → SNOMED_Concept              | NLP-extracted clinical entities (e.g., "cellulitis", "diabetes")                                               | `Observation` derived |
| `HAS_PROBLEM_REFERENCE`           | ClinicalNote → Problem                     | Note references active problem list item                                                                      | `Composition.section.entry` |
| `HAS_ADDENDUM`                    | OriginalNote → AddendumNote                | Later correction or additional information                                                                     | `Composition.relatesTo` |
| `SUPERSEDES`                      | NewNote → OldNote                          | Amended note replaces previous version                                                                         | `status = amended` |
| `DICTATED_BY`                     | ClinicalNote → User                        | Voice dictation author (may differ from signer)                                                                | Internal |
| `TRANSCRIBED_BY`                  | ClinicalNote → User                        | Medical transcriptionist                                                                                       | Internal |

---

## 3. Full Patient Journey & Care Continuum

| Edge Label                        | Direction                                  | Description & Clinical Purpose                                                                                 | FHIR Mapping |
|-----------------------------------|--------------------------------------------|---------------------------------------------------------------------------------------------------------------|--------------|
| `HAS_JOURNEY`                     | Patient → PatientJourney                   | Master timeline of all care events                                                                             | `CarePlan` + `EpisodeOfCare` |
| `HAS_ENCOUNTER`                   | PatientJourney → Encounter                 | Each admission, ED visit, outpatient visit in chronological order                                             | `EpisodeOfCare` |
| `HAS_ENCOUNTER`                   | Patient → Encounter                        | Direct ownership (alternative pattern)                                                                         | `Encounter.subject` |
| `HAS_APPOINTMENT`                 | PatientJourney → Appointment               | Scheduled future or past visits                                                                                | `Appointment` |
| `HAS_ORDER`                       | PatientJourney → Order                     | All orders across lifetime                                                                                     | `ServiceRequest` |
| `HAS_LAB_ORDER`                   | PatientJourney → LabOrder                  | Laboratory test orders                                                                                         | `ServiceRequest` |
| `HAS_IMAGING_ORDER`               | PatientJourney → ImagingOrder              | Radiology orders                                                                                               | `ServiceRequest` |
| `HAS_PROCEDURE`                   | PatientJourney → Procedure                 | Surgical and bedside procedures                                                                               | `Procedure` |
| `HAS_REFERRAL`                    | PatientJourney → Referral                  | Transitions to specialists or facilities                                                                       | `ReferralRequest` |
| `HAS_CARE_PLAN`                   | PatientJourney → CarePlan                  | Chronic disease management plans                                                                               | `CarePlan` |
| `HAS_DISPOSITION`                 | PatientJourney → Disposition               | Final outcome of each encounter                                                                                | `Encounter.hospitalization.dischargeDisposition` |
| `TRANSITIONED_TO`                 | Encounter A → Encounter B                  | Care handoff: ED → ICU → Floor → SNF → Home                                                                    | `Encounter.partOf` |
| `READMITTED_WITHIN_30_DAYS`       | Encounter A → Encounter B                  | Readmission tracking for quality metrics                                                                       | CMS Measure |

---

## 4. Laboratory Medicine – Full Panel

| Edge Label                             | Direction                                  | Description & Clinical Purpose                                                                                 | LOINC / Example |
|----------------------------------------|--------------------------------------------|---------------------------------------------------------------------------------------------------------------|-----------------|
| `HAS_LAB_ORDER`                        | Encounter → LabOrder                       | Physician order for lab testing                                                                                | `ServiceRequest` |
| `HAS_LAB_RESULT`                       | Patient → LabResult                        | All results over time (longitudinal trends)                                                                    | `Observation` |
| `HAS_LAB_RESULT`                       | Encounter → LabResult                      | Results from current admission                                                                                 | `DiagnosticReport` |
| `HAS_LAB_PANEL`                        | LabOrder → LabPanel                        | Grouped tests: CMP, CBC w/ Diff, Lipid Panel, Coags                                                    | `Observation` |
| `HAS_COMPONENT`                        | LabPanel → LabResult                       | Individual analytes in panel (e.g., Sodium, WBC)                                                               | `hasMember` |
| `HAS_REFERENCE_RANGE`                  | LabResult → ReferenceRange                 | Normal range (age/sex adjusted)                                                                                | `Observation.referenceRange` |
| `HAS_SPECIMEN`                         | LabResult → Specimen                       | Blood, Urine, CSF, Stool, Swab, Tissue                                                                         | `Specimen` |
| `COLLECTED_FROM`                       | Specimen → BodyStructure                   | Anatomic site: Left Arm, Clean-Catch Urine, Lumbar Puncture                                                    | `Specimen.collection.site` |
| `HAS_COLLECTION_METHOD`                | Specimen → CollectionMethod                | Venipuncture, Arterial, Catheterized                                                                           | SNOMED |
| `HAS_MICROBIOLOGY_CULTURE`             | LabResult → MicrobiologyCulture            | Culture & sensitivity                                                                                          | `Observation` |
| `HAS_ORGANISM`                         | MicrobiologyCulture → Organism             | Identified pathogen: MRSA, Candida, E. coli                                                                    | SNOMED |
| `HAS_ANTIBIOGRAM`                      | Organism → Antibiotic                      | Susceptibility: S, I, R                                                                                        | `Observation.interpretation` |
| `HAS_PATHOLOGY_REPORT`                 | Patient → PathologyReport                  | Surgical pathology, biopsy, cytology                                                                           | `DiagnosticReport` |
| `HAS_GROSS_DESCRIPTION`                | PathologyReport → GrossFinding             | Macroscopic findings                                                                                           | Text |
| `HAS_MICROSCOPIC_DESCRIPTION`         | PathologyReport → MicroFinding             | Histology                                                                                                      | Text |
| `HAS_IMMUNOHISTOCHEMISTRY`             | PathologyReport → IHC_Stain                | ER/PR, HER2, Ki-67, PD-L1                                                                                      | `Observation` |
| `HAS_MOLECULAR_TEST`                   | PathologyReport → MolecularResult          | NGS, FISH, PCR (EGFR, KRAS, BRAF)                                                                              | `Observation` |
| `HAS_TUMOR_MARKER`                     | LabResult → TumorMarker                    | PSA, CA-19-9, CEA, AFP, Beta-HCG                                                                               | LOINC |

---

## 5. Diagnostic Imaging – Complete Radiology Coverage

| Edge Label                             | Direction                                  | Description & Clinical Purpose                                                                                 | DICOM / FHIR |
|----------------------------------------|--------------------------------------------|---------------------------------------------------------------------------------------------------------------|--------------|
| `HAS_IMAGING_ORDER`                    | Encounter → ImagingOrder                   | Order for any imaging study                                                                                    | `ServiceRequest` |
| `HAS_IMAGING_STUDY`                    | Patient → ImagingStudy                     | Complete imaging exam                                                                                          | `ImagingStudy` |
| `HAS_IMAGING_STUDY`                    | Encounter → ImagingStudy                   | Study performed during visit                                                                                   | `DiagnosticReport` |
| `HAS_SERIES`                           | ImagingStudy → ImagingSeries               | DICOM series (e.g., T1 Pre-Contrast, CT Chest w/ Contrast)                                                     | `series` |
| `HAS_IMAGE`                            | ImagingSeries → DICOM_Image                | Individual image instance                                                                                      | `Media` |
| `HAS_MODALITY`                         | ImagingStudy → Modality                    | CR, DX, CT, MR, US, MG, PT, XA                                                                                 | DICOM (0008,0060) |
| `HAS_PROTOCOL`                         | ImagingStudy → Protocol                    | Stroke Protocol, PE Protocol, Trauma Pan-Scan                                                                  | `ImagingStudy.reasonCode` |
| `HAS_BODY_PART`                        | ImagingStudy → BodyStructure               | Chest, Brain, Abdomen/Pelvis, Spine, Extremity                                                                 | SNOMED |
| `HAS_CONTRAST`                         | ImagingStudy → ContrastAgent               | With/Without IV Contrast, Oral Contrast                                                                        | `ImagingStudy.series` |
| `HAS_RADIOLOGY_REPORT`                 | ImagingStudy → RadiologyReport             | Final signed interpretation                                                                                    | `DiagnosticReport` |
| `HAS_FINDING`                          | RadiologyReport → RadiologyFinding         | Mass, Infiltrate, Fracture, Pneumothorax, etc.                                                                | `Observation` |
| `HAS_IMPRESSION`                       | RadiologyReport → Impression               | Final clinical summary                                                                                         | `DiagnosticReport.conclusion` |
| `HAS_RECOMMENDATION`                   | RadiologyReport → Recommendation           | "Follow-up MRI in 3 months", "Biopsy recommended"                                                             | `DiagnosticReport.conclusionCode` |

### Specific Imaging Modalities (IS_A Hierarchy)

| Edge Label                        | Direction                                  | Description |
|-----------------------------------|--------------------------------------------|-----------|
| `IS_XRAY`                         | ImagingStudy → XRay                        | Plain radiography |
| `IS_CT_SCAN`                      | ImagingStudy → CTScan                      | Computed Tomography |
| `IS_MRI`                          | ImagingStudy → MRI                         | Magnetic Resonance Imaging |
| `IS_PET_SCAN`                     | ImagingStudy → PETScan                     | Positron Emission Tomography |
| `IS_ULTRASOUND`                   | ImagingStudy → Ultrasound                  | Sonography (including Echo) |
| `IS_MAMMOGRAPHY`                  | ImagingStudy → Mammography                 | Breast imaging |
| `IS_INTERVENTIONAL_RADIOLOGY`     | ImagingStudy → InterventionalProcedure     | Angio, Biopsy, Drainage |

---

## 6. Acute Care & Emergency Medicine – Full Coverage

| Edge Label                             | Direction                                  | Description & Clinical Purpose                                                                                 | Use Case |
|----------------------------------------|--------------------------------------------|---------------------------------------------------------------------------------------------------------------|----------|
| `HAS_CHIEF_COMPLAINT`                  | Encounter → ChiefComplaint                 | Primary reason for visit: "Chest pain", "Shortness of breath", "Altered mental status"                         | Triage |
| `HAS_TRIAGE`                           | Encounter → Triage                         | Initial nursing assessment                                                                                     | ESI, CTS |
| `HAS_ESI_LEVEL`                        | Triage → ESILevel                          | Emergency Severity Index (1–5)                                                                                 | Resource prediction |
| `HAS_TRAUMA_ALERT`                     | Encounter → TraumaAlert                    | Level 1 / Level 2 Trauma Activation                                                                            | Trauma Team |
| `HAS_STROKE_ALERT`                     | Encounter → StrokeAlert                    | Code Stroke — tPA / Thrombectomy candidate                                                                     | Stroke Team |
| `HAS_STEMI_ALERT`                      | Encounter → STEMIAlert                     | ST-Elevation MI — Cath Lab activation                                                                          | Cardiac |
| `HAS_SEPSIS_ALERT`                     | Encounter → SepsisAlert                    | qSOFA / SIRS → Early Goal-Directed Therapy                                                                     | Sepsis Bundle |
| `HAS_CARDIAC_ARREST`                   | Encounter → CodeBlue                       | ACLS event — ROSC, downtime tracking                                                                           | Resuscitation |
| `HAS_RAPID_RESPONSE`                   | Encounter → RapidResponse                 | MEWS trigger → RRT activation                                                                                  | Deterioration |
| `HAS_CRITICAL_LAB`                     | LabResult → CriticalAlert                  | Panic values → auto-notification                                                                              | K+ 6.5, Glucose <40 |
| `HAS_CODE_STATUS`                      | Patient → CodeStatus                       | Full Code, DNR, DNI, Comfort Care Only                                                                         | Ethics |
| `HAS_ESCALATION`                       | Event → EscalationEvent                    | Nurse → Resident → Fellow → Attending → Specialist                                                             | Chain of command |
| `HAS_LEFT_WITHOUT_BEING_SEEN`          | Encounter → LWBS                           | Patient left before provider evaluation                                                                        | Quality metric |
| `HAS_ELOPED`                           | Encounter → Elopement                      | Left against medical advice during treatment                                                                   | Risk |
| `HAS_BOARDING`                         | Encounter → BoardingEvent                  | Admitted but no inpatient bed — ED boarding                                                                    | Overcrowding |
| `HAS_REPRESENTATION`                   | Patient → EDReturnVisit                    | Returned to ED within 72 hours                                                                                 | Bounce-back |

---

## 7. Procedures & High-Risk Interventions

| Edge Label                             | Direction                                  | Description & Clinical Purpose                                                                                 | FHIR |
|----------------------------------------|--------------------------------------------|---------------------------------------------------------------------------------------------------------------|------|
| `HAS_PROCEDURE`                        | Encounter → Procedure                      | Any invasive or bedside procedure                                                                              | `Procedure` |
| `HAS_PROCEDURE`                        | Patient → Procedure                        | Lifetime procedure history                                                                                     | `Procedure` |
| `PERFORMED_BY`                         | Procedure → Doctor                         | Primary operator                                                                                               | `performer.actor` |
| `ASSISTED_BY`                          | Procedure → User (Resident/PA/Nurse)       | Assistant                                                                                                      | `performer` |
| `SUPERVISED_BY`                        | Procedure → AttendingDoctor                | Teaching case supervision                                                                                      | `performer.onBehalfOf` |
| `HAS_COMPLICATION`                     | Procedure → Complication                   | Bleeding, infection, perforation                                                                               | `Condition` |
| `HAS_ANESTHESIA`                       | Procedure → AnesthesiaRecord               | General, Regional, Sedation                                                                                    | `Procedure` |
| `REQUIRES_CONSENT`                     | Procedure → ConsentForm                    | Signed informed consent                                                                                       | `Consent` |
| `HAS_IMPLANT`                          | Procedure → ImplantDevice                  | Pacemaker, Joint Prosthesis, Stent                                                                             | `Device` |
| `HAS_TIMEOUT`                          | Procedure → UniversalProtocol             | WHO Surgical Safety Checklist                                                                                  | Internal |

### High-Risk Procedures (Explicit Types)

| Edge Label                        | Direction                                  | Example |
|-----------------------------------|--------------------------------------------|---------|
| `IS_INTUBATION`                   | Procedure → Intubation                     | RSI, Video laryngoscopy |
| `IS_CENTRAL_LINE`                 | Procedure → CentralLine                    | Internal Jugular, Subclavian |
| `IS_ARTERIAL_LINE`                | Procedure → ArterialLine                   | Radial, Femoral |
| `IS_CHEST_TUBE`                   | Procedure → Thoracostomy                   | Trauma, Pneumothorax |
| `IS_LUMBAR_PUNCTURE`              | Procedure → LP                             | Diagnostic, Therapeutic |
| `IS_PARACENTESIS`                 | Procedure → Paracentesis                   | Ascites, SBP workup |
| `IS_THORACENTESIS`                | Procedure → Thoracentesis                  | Pleural effusion |
| `IS_BRONCHOSCOPY`                 | Procedure → Bronchoscopy                   | BAL, Biopsy |
| `IS_ENDOSCOPY`                    | Procedure → Endoscopy                      | EGD, Colonoscopy |

---

## 8. Drug Interactions & Pharmacovigilance (Full Depth)

| Edge Label                             | Direction                                  | Description & Purpose                                                                                 | Source |
|----------------------------------------|--------------------------------------------|-------------------------------------------------------------------------------------------------------|--------|
| `INTERACTS_WITH`                       | Medication A → Medication B                | Any drug-drug interaction                                                                            | FDB, Lexicomp |
| `CAUSES_ADVERSE_REACTION`              | Medication → AdverseReaction               | Known side effect (e.g., Statin → Myopathy)                                                           | FAERS, SNOMED |
| `HAS_CONTRAINDICATION`                 | Medication → Condition                     | Absolute contraindication (e.g., Live vaccine in immunocompromised)                                  | SNOMED |
| `HAS_PRECAUTION`                       | Medication → Condition                     | Use with caution (e.g., NSAID in CKD)                                                                 | Lexicomp |
| `HAS_ALLERGY_CLASS_REACTION`           | Patient → DrugClass                        | Allergic to entire class (Penicillins, Sulfa)                                                         | `AllergyIntolerance` |
| `HAS_DUPLICATE_THERAPY`                | Medication A → Medication B                | Therapeutic duplication (e.g., two opioids)                                                           | Rules engine |
| `HAS_SEVERITY`                         | Interaction → SeverityLevel                | Minor / Moderate / Major / Contraindicated                                                            | FDB |
| `HAS_MECHANISM`                        | Interaction → PharmacologicalMechanism     | CYP3A4 inhibition, QT prolongation, serotonin syndrome                                               | Lexicomp |
| `REQUIRES_MONITORING`                  | Medication → Lab/Test                      | Therapeutic drug monitoring (e.g., Vancomycin → Trough level)                                         | Clinical rule |
| `DOSE_ADJUST_IN`                       | Medication → Condition                     | Renal dosing, hepatic impairment                                                                      | Lexicomp |
| `HAS_BLACK_BOX_WARNING`                | Medication → Warning                       | FDA-mandated warning (e.g., Teratogenicity)                                                           | FDA |
| `HAS_BEERS_CRITERIA`                   | Medication → BeersListEntry                | Potentially inappropriate in elderly                                                                 | AGS Beers |

---

## 9. SNOMED CT Semantic Relationships (Full Set)

| Edge Label                        | Direction                                  | Description & Example                                                                                 | SNOMED |
|-----------------------------------|--------------------------------------------|-------------------------------------------------------------------------------------------------------|--------|
| `IS_A`                            | Concept → ParentConcept                    | Core hierarchy: `Myocardial Infarction IS_A Ischemic Heart Disease`                                   | Core |
| `PART_OF`                         | Part → Whole                               | `Left Ventricle PART_OF Heart`                                                                        | Core |
| `HAS_FINDING_SITE`                | Disorder → BodyStructure                   | `Pneumonia HAS_FINDING_SITE Lung`                                                                     | Core |
| `HAS_CAUSATIVE_AGENT`             | Disorder → Agent                           | `Streptococcal Pharyngitis HAS_CAUSATIVE_AGENT Streptococcus Pyogenes`                                | Core |
| `HAS_METHOD`                      | Procedure → Method                         | `CT Scan HAS_METHOD Computed Tomography Imaging`                                                      | Core |
| `HAS_SPECIMEN`                    | Procedure → Specimen                       | `Biopsy HAS_SPECIMEN Tissue Sample`                                                                   | Core |
| `HAS_DIRECT_SUBSTANCE`            | Pharmaceutical → ActiveIngredient          | `Amoxicillin 500mg Capsule HAS_DIRECT_SUBSTANCE Amoxicillin`                                          | Core |
| `HAS_INTENT`                      | Procedure → Intent                         | `Colonoscopy HAS_INTENT Screening Intent`                                                             | Core |
| `HAS_SEVERITY`                    | Finding → Severity                         | `Pain HAS_SEVERITY Severe`                                                                            | Qualifier |
| `HAS_LATERALITY`                  | Finding → Side                             | `Stroke HAS_LATERALITY Left`                                                                          | Qualifier |

---

## 10. FHIR & HL7 Interoperability Mapping (Complete)

| Edge Label                        | Direction                                  | FHIR Resource                                                                                 | HL7 v2 |
|-----------------------------------|--------------------------------------------|-----------------------------------------------------------------------------------------------|--------|
| `HAS_FHIR_PATIENT`                | Patient → FHIR_Patient                     | Canonical FHIR Patient                                                                        | PID |
| `HAS_FHIR_ENCOUNTER`              | Encounter → FHIR_Encounter                 | Admission/visit                                                                               | PV1/PV2 |
| `HAS_FHIR_OBSERVATION`            | LabResult/Vitals → FHIR_Observation        | Labs, vitals, social determinants                                                             | OBX |
| `HAS_FHIR_MEDICATION_REQUEST`     | Prescription → FHIR_MedicationRequest       | e-Prescribing                                                                                 | RXO/RXE |
| `HAS_FHIR_CONDITION`              | Diagnosis/Problem → FHIR_Condition         | Problem list & diagnoses                                                                      | DG1 |
| `HAS_FHIR_ALLERGY`                | Allergy → FHIR_AllergyIntolerance          | Allergy & intolerance                                                                         | AL1 |
| `HAS_FHIR_PROCEDURE`              | Procedure → FHIR_Procedure                 | Surgical & bedside procedures                                                                 | PR1 |
| `HAS_FHIR_DOCUMENT`               | ClinicalNote → FHIR_Composition            | Clinical documents (CDA → FHIR)                                                               | ORU/MDM |
| `HAS_FHIR_CLAIM`                  | Claim → FHIR_Claim                         | 837 Professional/Institutional                                                                | 837 |
| `HAS_FHIR_EXPLANATION_OF_BENEFIT` | Claim → FHIR_ExplanationOfBenefit          | 835 Payment/Remittance                                                                        | 835 |
| `SENT_AS_HL7`                     | Message → HL7Message                       | Outbound ADT, ORU, MDM, etc.                                                                  | MSH |
| `RECEIVED_AS_HL7`                 | Message → HL7Message                       | Inbound lab, radiology, admission messages                                                    | MSH |

---

**Last Updated**: December 08, 2025  
**Status**: **Complete • Production-Ready • No Compromises**  
**License**: MIT
use once_cell::sync::Lazy; // Explicitly import Lazy
// FIX: Import the EdgeSchema trait to bring the static method `edge_label()` into scope.
use crate::edges::relationship::EdgeSchema;
// Import the concrete types to get their labels
use crate::constraints::PropertyConstraint;
use crate::edges::{has_allergy::HasAllergy, has_diagnosis::HasDiagnosis, has_encounter::HasEncounter, has_medication::HasMedication};


/// Represents a single edge relationship in the GraphDB Medical Data Model.
/// This structure defines the *meta-data* for the edge label, direction, and source,
/// but not the property constraints (which are defined in the concrete structs or loaded separately).
#[derive(Debug, Clone)]
pub struct MedicalRelationship {
    /// The unique label of the edge (e.g., "HAS_APPOINTMENT").
    pub edge_label: &'static str,
    /// Human-readable direction (e.g., "Patient → Appointment").
    pub direction: &'static str,
    /// Detailed explanation of the edge's purpose.
    pub description: &'static str,
    /// Source data field or system that generates this edge (e.g., "Composition.encounter").
    pub source: &'static str,
}

/// A list of labels for concrete relationship types that implement custom ToEdge/FromEdge logic.
/// The SchemaService must load constraints for these types from their respective `EdgeSchema` implementation.
/// FIX: Changed from `const` array to `static Lazy` vector to allow calling non-const associated functions.
pub static CONCRETE_RELATIONSHIPS: Lazy<Vec<&'static str>> = Lazy::new(|| {
    vec![
        HasAllergy::edge_label(),
        HasDiagnosis::edge_label(),
        HasEncounter::edge_label(),
        HasMedication::edge_label(),
    ]
});

/// Complete, exhaustive, production-ready vector of all medical relationships.
/// This vector acts as the canonical source for all known edge types in the graph schema.
/// NOTE: It only contains purely metadata-driven relationships.
pub static ALL_MEDICAL_RELATIONSHIPS: Lazy<Vec<MedicalRelationship>> = 
    Lazy::new(|| {
        vec![
            // 1. Master Patient Index (MPI)
            MedicalRelationship { edge_label: "HAS_MPI_RECORD", direction: "Patient → MasterPatientIndex", description: "Links local patient record to the enterprise golden MPI record for identity resolution", source: "Patient.link → Patient" },
            MedicalRelationship { edge_label: "IS_LINKED_TO", direction: "Patient A → Patient B", description: "Probabilistic soft link — candidate duplicate awaiting review", source: "Patient.link.type = \"seealso\"" },
            MedicalRelationship { edge_label: "IS_MERGED_INTO", direction: "SourcePatient → TargetPatient", description: "Permanent identity merge — source record retired, all data migrated", source: "Patient.link.type = \"replaced-by\"" },
            MedicalRelationship { edge_label: "IS_SPLIT_FROM", direction: "NewPatient → OriginalPatient", description: "Reverse of erroneous merge — identity split performed", source: "Audit trail" },
            MedicalRelationship { edge_label: "HAS_POTENTIAL_DUPLICATE", direction: "Patient A → Patient B", description: "Temporary flag during automated MPI matching workflow", source: "Internal MPI engine" },
            MedicalRelationship { edge_label: "RESOLVED_BY", direction: "MPI_Conflict → User", description: "Clinician or data steward who resolved duplicate/merge conflict", source: "Audit trail" },
            MedicalRelationship { edge_label: "HAS_MATCH_SCORE", direction: "MasterPatientIndex → MatchScoreVertex", description: "Stores probabilistic match confidence (0.0 – 1.0) from Fellegi-Sunter or ML model", source: "match_score field" },
            MedicalRelationship { edge_label: "HAS_BLOCKING_KEY", direction: "Patient → BlockingKeyVertex", description: "Deterministic keys used in blocking phase (e.g., Soundex(LastName)+DOB)", source: "Internal" },
            MedicalRelationship { edge_label: "HAS_SURVIVORSHIP_RULE", direction: "MasterPatientIndex → SurvivorshipRule", description: "Which source field wins during merge (e.g., \"Most recent MRN\", \"Longest name\")", source: "Governance" },

            // 2. Clinical Notes & Documentation
            MedicalRelationship { edge_label: "HAS_CLINICAL_NOTE", direction: "Patient → ClinicalNote", description: "All clinical documentation (H&P, Progress, Consult, Discharge Summary)", source: "Composition" },
            MedicalRelationship { edge_label: "HAS_CLINICAL_NOTE", direction: "Encounter → ClinicalNote", description: "Notes tied to specific admission/visit", source: "Composition.encounter" },
            MedicalRelationship { edge_label: "AUTHORED_BY", direction: "ClinicalNote → User (Doctor/NP/PA)", description: "Primary author of the note", source: "Composition.author" },
            MedicalRelationship { edge_label: "COSIGNED_BY", direction: "ClinicalNote → User", description: "Attending co-signature (e.g., resident note)", source: "Composition.attester" },
            MedicalRelationship { edge_label: "HAS_SECTION", direction: "ClinicalNote → SectionVertex", description: "Structured sections: Chief Complaint, HPI, ROS, PE, Assessment/Plan", source: "Composition.section" },
            MedicalRelationship { edge_label: "HAS_SUBSECTION", direction: "Section → Subsection", description: "Nested structure within a section", source: "section.section" },
            MedicalRelationship { edge_label: "MENTIONS_CONCEPT", direction: "ClinicalNote → SNOMED_Concept", description: "NLP-extracted clinical entities (e.g., \"cellulitis\", \"diabetes\")", source: "SNOMED CT" },
            MedicalRelationship { edge_label: "HAS_PROBLEM_REFERENCE", direction: "ClinicalNote → Problem", description: "Note references an active problem list item", source: "Composition.section.entry" },
            MedicalRelationship { edge_label: "HAS_ADDENDUM", direction: "OriginalNote → AddendumNote", description: "Later correction or additional information", source: "Composition.relatesTo" },
            MedicalRelationship { edge_label: "SUPERSEDES", direction: "NewNote → OldNote", description: "Amended note replaces previous version", source: "status = amended" },
            MedicalRelationship { edge_label: "DICTATED_BY", direction: "ClinicalNote → User", description: "Voice dictation author (may differ from signer)", source: "Internal" },
            MedicalRelationship { edge_label: "TRANSCRIBED_BY", direction: "ClinicalNote → User", description: "Medical transcriptionist", source: "Internal" },

            // 3. Full Patient Journey & Care Continuum
            MedicalRelationship { edge_label: "HAS_JOURNEY", direction: "Patient → PatientJourney", description: "Master timeline of all care events across lifetime", source: "CarePlan + EpisodeOfCare" },
            MedicalRelationship { edge_label: "HAS_ENCOUNTER", direction: "PatientJourney → Encounter", description: "Each admission, ED visit, outpatient visit in chronological order", source: "EpisodeOfCare" },
            MedicalRelationship { edge_label: "HAS_ENCOUNTER", direction: "Patient → Encounter", description: "Direct ownership (alternative pattern)", source: "Encounter.subject" },
            MedicalRelationship { edge_label: "HAS_APPOINTMENT", direction: "PatientJourney → Appointment", description: "Scheduled future or past visits", source: "Appointment" },
            MedicalRelationship { edge_label: "HAS_ORDER", direction: "PatientJourney → Order", description: "All orders (labs, meds, imaging) across lifetime", source: "ServiceRequest" },
            MedicalRelationship { edge_label: "HAS_LAB_ORDER", direction: "PatientJourney → LabOrder", description: "Laboratory test orders", source: "ServiceRequest" },
            MedicalRelationship { edge_label: "HAS_IMAGING_ORDER", direction: "PatientJourney → ImagingOrder", description: "Radiology orders", source: "ServiceRequest" },
            MedicalRelationship { edge_label: "HAS_PROCEDURE", direction: "PatientJourney → Procedure", description: "Surgical and bedside procedures", source: "Procedure" },
            MedicalRelationship { edge_label: "HAS_REFERRAL", direction: "PatientJourney → Referral", description: "Transitions to specialists or facilities", source: "ReferralRequest" },
            MedicalRelationship { edge_label: "HAS_CARE_PLAN", direction: "PatientJourney → CarePlan", description: "Chronic disease management plans", source: "CarePlan" },
            MedicalRelationship { edge_label: "HAS_DISPOSITION", direction: "PatientJourney → Disposition", description: "Final outcome of each encounter", source: "Encounter.hospitalization.dischargeDisposition" },
            MedicalRelationship { edge_label: "TRANSITIONED_TO", direction: "Encounter A → Encounter B", description: "Care handoff: ED → ICU → Floor → SNF → Home", source: "Encounter.partOf" },
            MedicalRelationship { edge_label: "READMITTED_WITHIN_30_DAYS", direction: "Encounter A → Encounter B", description: "Readmission tracking for quality metrics", source: "CMS Measure" },

            // 4. Laboratory Medicine – Full Panel
            MedicalRelationship { edge_label: "HAS_LAB_ORDER", direction: "Encounter → LabOrder", description: "Physician order for lab testing", source: "ServiceRequest" },
            MedicalRelationship { edge_label: "HAS_LAB_RESULT", direction: "Patient → LabResult", description: "All results over time (longitudinal trends)", source: "Observation (LOINC)" },
            MedicalRelationship { edge_label: "HAS_LAB_RESULT", direction: "Encounter → LabResult", description: "Results from current admission", source: "DiagnosticReport" },
            MedicalRelationship { edge_label: "HAS_LAB_PANEL", direction: "LabOrder → LabPanel", description: "Grouped tests: CMP, CBC w/ Diff, Lipid Panel, Coags", source: "Observation" },
            MedicalRelationship { edge_label: "HAS_COMPONENT", direction: "LabPanel → LabResult", description: "Individual analytes in panel (e.g., Sodium, WBC)", source: "hasMember" },
            MedicalRelationship { edge_label: "HAS_REFERENCE_RANGE", direction: "LabResult → ReferenceRange", description: "Normal range (age/sex adjusted)", source: "Observation.referenceRange" },
            MedicalRelationship { edge_label: "HAS_SPECIMEN", direction: "LabResult → Specimen", description: "Blood, Urine, CSF, Stool, Swab, Tissue", source: "Specimen" },
            MedicalRelationship { edge_label: "COLLECTED_FROM", direction: "Specimen → BodyStructure", description: "Anatomic site: Left Arm, Clean-Catch Urine, Lumbar Puncture", source: "Specimen.collection.site" },
            MedicalRelationship { edge_label: "HAS_COLLECTION_METHOD", direction: "Specimen → CollectionMethod", description: "Venipuncture, Arterial, Catheterized", source: "SNOMED" },
            MedicalRelationship { edge_label: "HAS_MICROBIOLOGY_CULTURE", direction: "LabResult → MicrobiologyCulture", description: "Culture & sensitivity results", source: "Observation" },
            MedicalRelationship { edge_label: "HAS_ORGANISM", direction: "MicrobiologyCulture → Organism", description: "Identified pathogen: MRSA, Candida, E. coli", source: "SNOMED" },
            MedicalRelationship { edge_label: "HAS_ANTIBIOGRAM", direction: "Organism → Antibiotic", description: "Susceptibility: S, I, R", source: "Observation.interpretation" },
            MedicalRelationship { edge_label: "HAS_PATHOLOGY_REPORT", direction: "Patient → PathologyReport", description: "Surgical pathology, biopsy, cytology", source: "DiagnosticReport" },
            MedicalRelationship { edge_label: "HAS_GROSS_DESCRIPTION", direction: "PathologyReport → GrossFinding", description: "Macroscopic findings", source: "Text" },
            MedicalRelationship { edge_label: "HAS_MICROSCOPIC_DESCRIPTION", direction: "PathologyReport → MicroFinding", description: "Histology", source: "Text" },
            MedicalRelationship { edge_label: "HAS_IMMUNOHISTOCHEMISTRY", direction: "PathologyReport → IHC_Stain", description: "ER/PR, HER2, Ki-67, PD-L1", source: "Observation" },
            MedicalRelationship { edge_label: "HAS_MOLECULAR_TEST", direction: "PathologyReport → MolecularResult", description: "NGS, FISH, PCR (EGFR, KRAS, BRAF)", source: "Observation" },
            MedicalRelationship { edge_label: "HAS_TUMOR_MARKER", direction: "LabResult → TumorMarker", description: "PSA, CA-19-9, CEA, AFP, Beta-HCG", source: "LOINC" },

            // 5. Diagnostic Imaging – Complete Radiology Coverage
            MedicalRelationship { edge_label: "HAS_IMAGING_ORDER", direction: "Encounter → ImagingOrder", description: "Order for any imaging study", source: "ServiceRequest" },
            MedicalRelationship { edge_label: "HAS_IMAGING_STUDY", direction: "Patient → ImagingStudy", description: "Complete imaging exam", source: "ImagingStudy" },
            MedicalRelationship { edge_label: "HAS_IMAGING_STUDY", direction: "Encounter → ImagingStudy", description: "Study performed during visit", source: "DiagnosticReport" },
            MedicalRelationship { edge_label: "HAS_SERIES", direction: "ImagingStudy → ImagingSeries", description: "DICOM series (e.g., T1 Pre-Contrast, CT Chest w/ Contrast)", source: "series" },
            MedicalRelationship { edge_label: "HAS_IMAGE", direction: "ImagingSeries → DICOM_Image", description: "Individual image instance", source: "Media" },
            MedicalRelationship { edge_label: "HAS_MODALITY", direction: "ImagingStudy → Modality", description: "CR, DX, CT, MR, US, MG, PT, XA", source: "DICOM (0008,0060)" },
            MedicalRelationship { edge_label: "HAS_PROTOCOL", direction: "ImagingStudy → Protocol", description: "Stroke Protocol, PE Protocol, Trauma Pan-Scan", source: "ImagingStudy.reasonCode" },
            MedicalRelationship { edge_label: "HAS_BODY_PART", direction: "ImagingStudy → BodyStructure", description: "Chest, Brain, Abdomen/Pelvis, Spine, Extremity", source: "SNOMED" },
            MedicalRelationship { edge_label: "HAS_CONTRAST", direction: "ImagingStudy → ContrastAgent", description: "With/Without IV Contrast, Oral Contrast", source: "ImagingStudy.series" },
            MedicalRelationship { edge_label: "HAS_RADIOLOGY_REPORT", direction: "ImagingStudy → RadiologyReport", description: "Final signed interpretation", source: "DiagnosticReport" },
            MedicalRelationship { edge_label: "HAS_FINDING", direction: "RadiologyReport → RadiologyFinding", description: "Mass, Infiltrate, Fracture, Pneumothorax, etc.", source: "Observation" },
            MedicalRelationship { edge_label: "HAS_IMPRESSION", direction: "RadiologyReport → Impression", description: "Final clinical summary", source: "DiagnosticReport.conclusion" },
            MedicalRelationship { edge_label: "HAS_RECOMMENDATION", direction: "RadiologyReport → Recommendation", description: "\"Follow-up MRI in 3 months\", \"Biopsy recommended\"", source: "DiagnosticReport.conclusionCode" },
            MedicalRelationship { edge_label: "IS_XRAY", direction: "ImagingStudy → XRay", description: "Plain radiography", source: "DICOM Modality CR/DX" },
            MedicalRelationship { edge_label: "IS_CT_SCAN", direction: "ImagingStudy → CTScan", description: "Computed Tomography", source: "DICOM Modality CT" },
            MedicalRelationship { edge_label: "IS_MRI", direction: "ImagingStudy → MRI", description: "Magnetic Resonance Imaging", source: "DICOM Modality MR" },
            MedicalRelationship { edge_label: "IS_PET_SCAN", direction: "ImagingStudy → PETScan", description: "Positron Emission Tomography", source: "DICOM Modality PT" },
            MedicalRelationship { edge_label: "IS_ULTRASOUND", direction: "ImagingStudy → Ultrasound", description: "Sonography (including Echo)", source: "DICOM Modality US" },
            MedicalRelationship { edge_label: "IS_MAMMOGRAPHY", direction: "ImagingStudy → Mammography", description: "Breast imaging", source: "DICOM Modality MG" },
            MedicalRelationship { edge_label: "IS_INTERVENTIONAL_RADIOLOGY", direction: "ImagingStudy → InterventionalProcedure", description: "Angio, Biopsy, Drainage", source: "Procedure-specific" },

            // 6. Acute Care & Emergency Medicine – Full Coverage
            MedicalRelationship { edge_label: "HAS_CHIEF_COMPLAINT", direction: "Encounter → ChiefComplaint", description: "Primary reason for visit: \"Chest pain\", \"SOB\", \"Altered mental status\"", source: "Triage" },
            MedicalRelationship { edge_label: "HAS_TRIAGE", direction: "Encounter → Triage", description: "Initial nursing assessment", source: "ESI, CTS" },
            MedicalRelationship { edge_label: "HAS_ESI_LEVEL", direction: "Triage → ESILevel", description: "Emergency Severity Index (1–5)", source: "ESI" },
            MedicalRelationship { edge_label: "HAS_TRAUMA_ALERT", direction: "Encounter → TraumaAlert", description: "Level 1 / Level 2 Trauma Activation", source: "Trauma Team" },
            MedicalRelationship { edge_label: "HAS_STROKE_ALERT", direction: "Encounter → StrokeAlert", description: "Code Stroke — tPA / Thrombectomy candidate", source: "Stroke Team" },
            MedicalRelationship { edge_label: "HAS_STEMI_ALERT", direction: "Encounter → STEMIAlert", description: "ST-Elevation MI — Cath Lab activation", source: "Cardiac" },
            MedicalRelationship { edge_label: "HAS_SEPSIS_ALERT", direction: "Encounter → SepsisAlert", description: "qSOFA / SIRS → Early Goal-Directed Therapy", source: "Sepsis Bundle" },
            MedicalRelationship { edge_label: "HAS_CARDIAC_ARREST", direction: "Encounter → CodeBlue", description: "ACLS event — ROSC, downtime tracking", source: "Resuscitation" },
            MedicalRelationship { edge_label: "HAS_RAPID_RESPONSE", direction: "Encounter → RapidResponse", description: "MEWS trigger → RRT activation", source: "Deterioration" },
            MedicalRelationship { edge_label: "HAS_CRITICAL_LAB", direction: "LabResult → CriticalAlert", description: "Panic values → auto-notification", source: "K+ 6.5, Glucose <40" },
            MedicalRelationship { edge_label: "HAS_CODE_STATUS", direction: "Patient → CodeStatus", description: "Full Code, DNR, DNI, Comfort Care Only", source: "Ethics" },
            MedicalRelationship { edge_label: "HAS_ESCALATION", direction: "Event → EscalationEvent", description: "Nurse → Resident → Fellow → Attending → Specialist", source: "Chain of command" },
            MedicalRelationship { edge_label: "HAS_LEFT_WITHOUT_BEING_SEEN", direction: "Encounter → LWBS", description: "Patient left before provider evaluation", source: "Quality metric" },
            MedicalRelationship { edge_label: "HAS_ELOPED", direction: "Encounter → Elopement", description: "Left against medical advice during treatment", source: "Risk" },
            MedicalRelationship { edge_label: "HAS_BOARDING", direction: "Encounter → BoardingEvent", description: "Admitted but no inpatient bed — ED boarding", source: "Overcrowding" },
            MedicalRelationship { edge_label: "HAS_REPRESENTATION", direction: "Patient → EDReturnVisit", description: "Returned to ED within 72 hours", source: "Bounce-back" },

            // 7. Procedures & High-Risk Interventions
            MedicalRelationship { edge_label: "HAS_PROCEDURE", direction: "Encounter → Procedure", description: "Any invasive or bedside procedure", source: "Procedure" },
            MedicalRelationship { edge_label: "HAS_PROCEDURE", direction: "Patient → Procedure", description: "Lifetime procedure history", source: "Procedure" },
            MedicalRelationship { edge_label: "PERFORMED_BY", direction: "Procedure → Doctor", description: "Primary operator", source: "performer.actor" },
            MedicalRelationship { edge_label: "ASSISTED_BY", direction: "Procedure → User (Resident/PA/Nurse)", description: "Assistant", source: "performer" },
            MedicalRelationship { edge_label: "SUPERVISED_BY", direction: "Procedure → AttendingDoctor", description: "Teaching case supervision", source: "performer.onBehalfOf" },
            MedicalRelationship { edge_label: "HAS_COMPLICATION", direction: "Procedure → Complication", description: "Bleeding, infection, perforation", source: "Condition" },
            MedicalRelationship { edge_label: "HAS_ANESTHESIA", direction: "Procedure → AnesthesiaRecord", description: "General, Regional, Sedation", source: "Procedure" },
            MedicalRelationship { edge_label: "REQUIRES_CONSENT", direction: "Procedure → ConsentForm", description: "Signed informed consent", source: "Consent" },
            MedicalRelationship { edge_label: "HAS_IMPLANT", direction: "Procedure → ImplantDevice", description: "Pacemaker, Joint Prosthesis, Stent", source: "Device" },
            MedicalRelationship { edge_label: "HAS_TIMEOUT", direction: "Procedure → UniversalProtocol", description: "WHO Surgical Safety Checklist", source: "Internal" },
            MedicalRelationship { edge_label: "IS_INTUBATION", direction: "Procedure → Intubation", description: "RSI, Video laryngoscopy", source: "Procedure-specific" },
            MedicalRelationship { edge_label: "IS_CENTRAL_LINE", direction: "Procedure → CentralLine", description: "Internal Jugular, Subclavian", source: "Procedure-specific" },
            MedicalRelationship { edge_label: "IS_ARTERIAL_LINE", direction: "Procedure → ArterialLine", description: "Radial, Femoral", source: "Procedure-specific" },
            MedicalRelationship { edge_label: "IS_CHEST_TUBE", direction: "Procedure → Thoracostomy", description: "Trauma, Pneumothorax", source: "Procedure-specific" },
            MedicalRelationship { edge_label: "IS_LUMBAR_PUNCTURE", direction: "Procedure → LP", description: "Diagnostic, Therapeutic", source: "Procedure-specific" },
            MedicalRelationship { edge_label: "IS_PARACENTESIS", direction: "Procedure → Paracentesis", description: "Ascites, SBP workup", source: "Procedure-specific" },
            MedicalRelationship { edge_label: "IS_THORACENTESIS", direction: "Procedure → Thoracentesis", description: "Pleural effusion", source: "Procedure-specific" },
            MedicalRelationship { edge_label: "IS_BRONCHOSCOPY", direction: "Procedure → Bronchoscopy", description: "BAL, Biopsy", source: "Procedure-specific" },
            MedicalRelationship { edge_label: "IS_ENDOSCOPY", direction: "Procedure → Endoscopy", description: "EGD, Colonoscopy", source: "Procedure-specific" },

            // 8. Drug Interactions & Pharmacovigilance
            MedicalRelationship { edge_label: "INTERACTS_WITH", direction: "Medication A → Medication B", description: "Any drug-drug interaction", source: "FDB, Lexicomp" },
            MedicalRelationship { edge_label: "CAUSES_ADVERSE_REACTION", direction: "Medication → AdverseReaction", description: "Known side effect (e.g., Statin → Myopathy)", source: "FAERS, SNOMED" },
            MedicalRelationship { edge_label: "HAS_CONTRAINDICATION", direction: "Medication → Condition", description: "Absolute contraindication", source: "SNOMED" },
            MedicalRelationship { edge_label: "HAS_PRECAUTION", direction: "Medication → Condition", description: "Use with caution", source: "Lexicomp" },
            MedicalRelationship { edge_label: "HAS_ALLERGY_CLASS_REACTION", direction: "Patient → DrugClass", description: "Allergic to entire class (Penicillins, Sulfa)", source: "AllergyIntolerance" },
            MedicalRelationship { edge_label: "HAS_DUPLICATE_THERAPY", direction: "Medication A → Medication B", description: "Therapeutic duplication (e.g., two opioids)", source: "Rules engine" },
            MedicalRelationship { edge_label: "HAS_SEVERITY", direction: "Interaction → SeverityLevel", description: "Minor / Moderate / Major / Contraindicated", source: "FDB" },
            MedicalRelationship { edge_label: "HAS_MECHANISM", direction: "Interaction → PharmacologicalMechanism", description: "CYP3A4 inhibition, QT prolongation, serotonin syndrome", source: "Lexicomp" },
            MedicalRelationship { edge_label: "REQUIRES_MONITORING", direction: "Medication → Lab/Test", description: "Therapeutic drug monitoring (e.g., Vancomycin → Trough)", source: "Clinical rule" },
            MedicalRelationship { edge_label: "DOSE_ADJUST_IN", direction: "Medication → Condition", description: "Renal dosing, hepatic impairment", source: "Lexicomp" },
            MedicalRelationship { edge_label: "HAS_BLACK_BOX_WARNING", direction: "Medication → Warning", description: "FDA-mandated warning (e.g., Teratogenicity)", source: "FDA" },
            MedicalRelationship { edge_label: "HAS_BEERS_CRITERIA", direction: "Medication → BeersListEntry", description: "Potentially inappropriate in elderly", source: "AGS Beers" },

            // 9. SNOMED CT Semantic Relationships
            MedicalRelationship { edge_label: "IS_A", direction: "Concept → ParentConcept", description: "Core hierarchy", source: "SNOMED Core" },
            MedicalRelationship { edge_label: "PART_OF", direction: "Part → Whole", description: "Anatomical or organizational composition", source: "SNOMED Core" },
            MedicalRelationship { edge_label: "HAS_FINDING_SITE", direction: "Disorder → BodyStructure", description: "Location of finding", source: "SNOMED Core" },
            MedicalRelationship { edge_label: "HAS_CAUSATIVE_AGENT", direction: "Disorder → Agent", description: "Etiology", source: "SNOMED Core" },
            MedicalRelationship { edge_label: "HAS_METHOD", direction: "Procedure → Method", description: "How procedure is performed", source: "SNOMED Core" },
            MedicalRelationship { edge_label: "HAS_SPECIMEN", direction: "Procedure → Specimen", description: "Specimen used", source: "SNOMED Core" },
            MedicalRelationship { edge_label: "HAS_DIRECT_SUBSTANCE", direction: "Pharmaceutical → ActiveIngredient", description: "Active ingredient", source: "SNOMED Core" },
            MedicalRelationship { edge_label: "HAS_INTENT", direction: "Procedure → Intent", description: "Screening, therapeutic, palliative, etc.", source: "SNOMED Core" },
            MedicalRelationship { edge_label: "HAS_SEVERITY", direction: "Finding → Severity", description: "Mild, Moderate, Severe", source: "SNOMED Qualifier" },
            MedicalRelationship { edge_label: "HAS_LATERALITY", direction: "Finding → Side", description: "Left, Right, Bilateral", source: "SNOMED Qualifier" },

            // 10. FHIR & HL7 Interoperability
            MedicalRelationship { edge_label: "HAS_FHIR_PATIENT", direction: "Patient → FHIR_Patient", description: "Canonical FHIR Patient", source: "PID" },
            MedicalRelationship { edge_label: "HAS_FHIR_ENCOUNTER", direction: "Encounter → FHIR_Encounter", description: "Admission/visit", source: "PV1/PV2" },
            MedicalRelationship { edge_label: "HAS_FHIR_OBSERVATION", direction: "LabResult/Vitals → FHIR_Observation", description: "Labs, vitals, social determinants", source: "OBX" },
            MedicalRelationship { edge_label: "HAS_FHIR_MEDICATION_REQUEST", direction: "Prescription → FHIR_MedicationRequest", description: "e-Prescribing", source: "RXO/RXE" },
            MedicalRelationship { edge_label: "HAS_FHIR_CONDITION", direction: "Diagnosis/Problem → FHIR_Condition", description: "Problem list & diagnoses", source: "DG1" },
            MedicalRelationship { edge_label: "HAS_FHIR_ALLERGY", direction: "Allergy → FHIR_AllergyIntolerance", description: "Allergy & intolerance", source: "AL1" },
            MedicalRelationship { edge_label: "HAS_FHIR_PROCEDURE", direction: "Procedure → FHIR_Procedure", description: "Surgical & bedside procedures", source: "PR1" },
            MedicalRelationship { edge_label: "HAS_FHIR_DOCUMENT", direction: "ClinicalNote → FHIR_Composition", description: "Clinical documents (CDA → FHIR)", source: "ORU/MDM" },
            MedicalRelationship { edge_label: "HAS_FHIR_CLAIM", direction: "Claim → FHIR_Claim", description: "837 Professional/Institutional", source: "837" },
            MedicalRelationship { edge_label: "HAS_FHIR_EXPLANATION_OF_BENEFIT", direction: "Claim → FHIR_ExplanationOfBenefit", description: "835 Payment/Remittance", source: "835" },
            MedicalRelationship { edge_label: "SENT_AS_HL7", direction: "Message → HL7Message", description: "Outbound ADT, ORU, MDM, etc.", source: "MSH" },
            MedicalRelationship { edge_label: "RECEIVED_AS_HL7", direction: "Message → HL7Message", description: "Inbound lab, radiology, admission messages", source: "MSH" },
        ]
    });
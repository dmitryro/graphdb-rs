// models/src/medical/patient.rs
use bincode::{Encode, Decode};
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};
use crate::medical::Address;   // or wherever it lives

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Patient {
    // Primary identifiers
    pub id: i32,
    pub user_id: Option<i32>,
    pub mrn: Option<String>,  // Medical Record Number
    pub ssn: Option<String>,  // Social Security Number (encrypted in production)

    // Demographics
    pub first_name: String,
    pub middle_name: Option<String>,
    pub last_name: String,
    pub suffix: Option<String>,  // Jr, Sr, III, etc.
    pub preferred_name: Option<String>,
    pub date_of_birth: DateTime<Utc>,
    pub date_of_death: Option<DateTime<Utc>>,
    pub gender: String,  // MALE, FEMALE, OTHER, UNKNOWN
    pub sex_assigned_at_birth: Option<String>,  // MALE, FEMALE, INTERSEX
    pub gender_identity: Option<String>,  // MAN, WOMAN, TRANSGENDER_MALE, TRANSGENDER_FEMALE, NON_BINARY, OTHER
    pub pronouns: Option<String>,  // HE/HIM, SHE/HER, THEY/THEM, etc.

    // Contact Information
    pub address_id: Option<uuid::Uuid>, // foreign key to Address vertex
    pub address: Option<Address>,
    pub phone_home: Option<String>,
    pub phone_mobile: Option<String>,
    pub phone_work: Option<String>,
    pub email: Option<String>,
    pub preferred_contact_method: Option<String>,  // PHONE, EMAIL, SMS, MAIL
    pub preferred_language: Option<String>,  // EN, ES, FR, etc.
    pub interpreter_needed: bool,

    // Emergency Contact
    pub emergency_contact_name: Option<String>,
    pub emergency_contact_relationship: Option<String>,
    pub emergency_contact_phone: Option<String>,

    // Demographic Details
    pub marital_status: Option<String>,  // SINGLE, MARRIED, DIVORCED, WIDOWED, SEPARATED, DOMESTIC_PARTNER
    pub race: Option<String>,
    pub ethnicity: Option<String>,
    pub religion: Option<String>,

    // Insurance & Financial
    pub primary_insurance: Option<String>,
    pub primary_insurance_id: Option<String>,
    pub secondary_insurance: Option<String>,
    pub secondary_insurance_id: Option<String>,
    pub guarantor_name: Option<String>,
    pub guarantor_relationship: Option<String>,

    // Clinical Information
    pub primary_care_provider_id: Option<i32>,
    pub blood_type: Option<String>,  // A+, A-, B+, B-, AB+, AB-, O+, O-
    pub organ_donor: Option<bool>,
    pub advance_directive_on_file: bool,
    pub dni_status: Option<String>,  // Do Not Intubate
    pub dnr_status: Option<String>,  // Do Not Resuscitate
    pub code_status: Option<String>,  // FULL_CODE, DNR, DNI, COMFORT_CARE

    // Administrative
    pub patient_status: String,  // ACTIVE, INACTIVE, DECEASED, MERGED, TEST
    pub vip_flag: bool,
    pub confidential_flag: bool,  // Restricts who can view record
    pub research_consent: Option<bool>,
    pub marketing_consent: Option<bool>,

    // Social Determinants of Health (SDOH)
    pub employment_status: Option<String>,
    pub housing_status: Option<String>,  // HOUSED, HOMELESS, UNSTABLE, TEMPORARY
    pub education_level: Option<String>,
    pub financial_strain: Option<String>,  // NONE, MILD, MODERATE, SEVERE
    pub food_insecurity: bool,
    pub transportation_needs: bool,
    pub social_isolation: Option<String>,
    pub veteran_status: Option<bool>,
    pub disability_status: Option<String>,

    // Clinical Alerts
    pub alert_flags: Option<String>,  // JSON array of alert codes
    pub special_needs: Option<String>,

    // Audit Trail
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub created_by: Option<i32>,
    pub updated_by: Option<i32>,
    pub last_visit_date: Option<DateTime<Utc>>,
}

impl ToVertex for Patient {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("Patient".to_string()).unwrap());

        // Primary identifiers
        v.add_property("id", &self.id.to_string());
        if let Some(ref val) = self.user_id {
            v.add_property("user_id", &val.to_string());
        }
        if let Some(ref val) = self.mrn {
            v.add_property("mrn", val);
        }
        if let Some(ref val) = self.ssn {
            v.add_property("ssn", val);
        }

        // Demographics
        v.add_property("first_name", &self.first_name);
        if let Some(ref val) = self.middle_name {
            v.add_property("middle_name", val);
        }
        v.add_property("last_name", &self.last_name);
        if let Some(ref val) = self.suffix {
            v.add_property("suffix", val);
        }
        if let Some(ref val) = self.preferred_name {
            v.add_property("preferred_name", val);
        }
        v.add_property("date_of_birth", &self.date_of_birth.to_rfc3339());
        if let Some(ref val) = self.date_of_death {
            v.add_property("date_of_death", &val.to_rfc3339());
        }
        v.add_property("gender", &self.gender);
        if let Some(ref val) = self.sex_assigned_at_birth {
            v.add_property("sex_assigned_at_birth", val);
        }
        if let Some(ref val) = self.gender_identity {
            v.add_property("gender_identity", val);
        }
        if let Some(ref val) = self.pronouns {
            v.add_property("pronouns", val);
        }

        // Contact Information
        if let Some(ref addr_id) = self.address_id {
            v.add_property("address_id", &addr_id.to_string());
        }
        if let Some(ref addr) = self.address {
            // serde_json is already in your dep graph via the workspace
            v.add_property("address", &serde_json::to_string(addr).expect("Address serialise"));
        }
        if let Some(ref val) = self.phone_home {
            v.add_property("phone_home", val);
        }
        if let Some(ref val) = self.phone_mobile {
            v.add_property("phone_mobile", val);
        }
        if let Some(ref val) = self.phone_work {
            v.add_property("phone_work", val);
        }
        if let Some(ref val) = self.email {
            v.add_property("email", val);
        }
        if let Some(ref val) = self.preferred_contact_method {
            v.add_property("preferred_contact_method", val);
        }
        if let Some(ref val) = self.preferred_language {
            v.add_property("preferred_language", val);
        }
        v.add_property("interpreter_needed", &self.interpreter_needed.to_string());

        // Emergency Contact
        if let Some(ref val) = self.emergency_contact_name {
            v.add_property("emergency_contact_name", val);
        }
        if let Some(ref val) = self.emergency_contact_relationship {
            v.add_property("emergency_contact_relationship", val);
        }
        if let Some(ref val) = self.emergency_contact_phone {
            v.add_property("emergency_contact_phone", val);
        }

        // Demographic Details
        if let Some(ref val) = self.marital_status {
            v.add_property("marital_status", val);
        }
        if let Some(ref val) = self.race {
            v.add_property("race", val);
        }
        if let Some(ref val) = self.ethnicity {
            v.add_property("ethnicity", val);
        }
        if let Some(ref val) = self.religion {
            v.add_property("religion", val);
        }

        // Insurance & Financial
        if let Some(ref val) = self.primary_insurance {
            v.add_property("primary_insurance", val);
        }
        if let Some(ref val) = self.primary_insurance_id {
            v.add_property("primary_insurance_id", val);
        }
        if let Some(ref val) = self.secondary_insurance {
            v.add_property("secondary_insurance", val);
        }
        if let Some(ref val) = self.secondary_insurance_id {
            v.add_property("secondary_insurance_id", val);
        }
        if let Some(ref val) = self.guarantor_name {
            v.add_property("guarantor_name", val);
        }
        if let Some(ref val) = self.guarantor_relationship {
            v.add_property("guarantor_relationship", val);
        }

        // Clinical Information
        if let Some(ref val) = self.primary_care_provider_id {
            v.add_property("primary_care_provider_id", &val.to_string());
        }
        if let Some(ref val) = self.blood_type {
            v.add_property("blood_type", val);
        }
        if let Some(ref val) = self.organ_donor {
            v.add_property("organ_donor", &val.to_string());
        }
        v.add_property("advance_directive_on_file", &self.advance_directive_on_file.to_string());
        if let Some(ref val) = self.dni_status {
            v.add_property("dni_status", val);
        }
        if let Some(ref val) = self.dnr_status {
            v.add_property("dnr_status", val);
        }
        if let Some(ref val) = self.code_status {
            v.add_property("code_status", val);
        }

        // Administrative
        v.add_property("patient_status", &self.patient_status);
        v.add_property("vip_flag", &self.vip_flag.to_string());
        v.add_property("confidential_flag", &self.confidential_flag.to_string());
        if let Some(ref val) = self.research_consent {
            v.add_property("research_consent", &val.to_string());
        }
        if let Some(ref val) = self.marketing_consent {
            v.add_property("marketing_consent", &val.to_string());
        }

        // Social Determinants of Health
        if let Some(ref val) = self.employment_status {
            v.add_property("employment_status", val);
        }
        if let Some(ref val) = self.housing_status {
            v.add_property("housing_status", val);
        }
        if let Some(ref val) = self.education_level {
            v.add_property("education_level", val);
        }
        if let Some(ref val) = self.financial_strain {
            v.add_property("financial_strain", val);
        }
        v.add_property("food_insecurity", &self.food_insecurity.to_string());
        v.add_property("transportation_needs", &self.transportation_needs.to_string());
        if let Some(ref val) = self.social_isolation {
            v.add_property("social_isolation", val);
        }
        if let Some(ref val) = self.veteran_status {
            v.add_property("veteran_status", &val.to_string());
        }
        if let Some(ref val) = self.disability_status {
            v.add_property("disability_status", val);
        }

        // Clinical Alerts
        if let Some(ref val) = self.alert_flags {
            v.add_property("alert_flags", val);
        }
        if let Some(ref val) = self.special_needs {
            v.add_property("special_needs", val);
        }

        // Audit Trail
        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());
        if let Some(ref val) = self.created_by {
            v.add_property("created_by", &val.to_string());
        }
        if let Some(ref val) = self.updated_by {
            v.add_property("updated_by", &val.to_string());
        }
        if let Some(ref val) = self.last_visit_date {
            v.add_property("last_visit_date", &val.to_rfc3339());
        }

        v
    }
}

impl Patient {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Patient" { return None; }

        Some(Patient {
            // Primary identifiers
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            user_id: vertex.properties.get("user_id")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            mrn: vertex.properties.get("mrn")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            ssn: vertex.properties.get("ssn")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

            // Demographics
            first_name: vertex.properties.get("first_name")?.as_str()?.to_string(),
            middle_name: vertex.properties.get("middle_name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            last_name: vertex.properties.get("last_name")?.as_str()?.to_string(),
            suffix: vertex.properties.get("suffix")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            preferred_name: vertex.properties.get("preferred_name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            date_of_birth: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("date_of_birth")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            date_of_death: vertex.properties.get("date_of_death")
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc)),
            gender: vertex.properties.get("gender")?.as_str()?.to_string(),
            sex_assigned_at_birth: vertex.properties.get("sex_assigned_at_birth")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            gender_identity: vertex.properties.get("gender_identity")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            pronouns: vertex.properties.get("pronouns")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

            // Contact Information
            address_id: vertex.properties.get("address_id")
                .and_then(|v| v.as_str())
                .and_then(|s| uuid::Uuid::parse_str(s).ok()),
            address: vertex.properties.get("address")
                .and_then(|v| v.as_str())
                .and_then(|s| serde_json::from_str::<Address>(s).ok()),
            phone_home: vertex.properties.get("phone_home")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            phone_mobile: vertex.properties.get("phone_mobile")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            phone_work: vertex.properties.get("phone_work")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            email: vertex.properties.get("email")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            preferred_contact_method: vertex.properties.get("preferred_contact_method")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            preferred_language: vertex.properties.get("preferred_language")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            interpreter_needed: vertex.properties.get("interpreter_needed")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(false),

            // Emergency Contact
            emergency_contact_name: vertex.properties.get("emergency_contact_name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            emergency_contact_relationship: vertex.properties.get("emergency_contact_relationship")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            emergency_contact_phone: vertex.properties.get("emergency_contact_phone")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

            // Demographic Details
            marital_status: vertex.properties.get("marital_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            race: vertex.properties.get("race")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            ethnicity: vertex.properties.get("ethnicity")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            religion: vertex.properties.get("religion")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

            // Insurance & Financial
            primary_insurance: vertex.properties.get("primary_insurance")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            primary_insurance_id: vertex.properties.get("primary_insurance_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            secondary_insurance: vertex.properties.get("secondary_insurance")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            secondary_insurance_id: vertex.properties.get("secondary_insurance_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            guarantor_name: vertex.properties.get("guarantor_name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            guarantor_relationship: vertex.properties.get("guarantor_relationship")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

            // Clinical Information
            primary_care_provider_id: vertex.properties.get("primary_care_provider_id")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            blood_type: vertex.properties.get("blood_type")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            organ_donor: vertex.properties.get("organ_donor")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            advance_directive_on_file: vertex.properties.get("advance_directive_on_file")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(false),
            dni_status: vertex.properties.get("dni_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            dnr_status: vertex.properties.get("dnr_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            code_status: vertex.properties.get("code_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

            // Administrative
            patient_status: vertex.properties.get("patient_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "ACTIVE".to_string()),
            vip_flag: vertex.properties.get("vip_flag")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(false),
            confidential_flag: vertex.properties.get("confidential_flag")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(false),
            research_consent: vertex.properties.get("research_consent")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            marketing_consent: vertex.properties.get("marketing_consent")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),

            // Social Determinants of Health
            employment_status: vertex.properties.get("employment_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            housing_status: vertex.properties.get("housing_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            education_level: vertex.properties.get("education_level")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            financial_strain: vertex.properties.get("financial_strain")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            food_insecurity: vertex.properties.get("food_insecurity")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(false),
            transportation_needs: vertex.properties.get("transportation_needs")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(false),
            social_isolation: vertex.properties.get("social_isolation")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            veteran_status: vertex.properties.get("veteran_status")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            disability_status: vertex.properties.get("disability_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

            // Clinical Alerts
            alert_flags: vertex.properties.get("alert_flags")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            special_needs: vertex.properties.get("special_needs")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

            // Audit Trail
            created_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("created_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            updated_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("updated_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            created_by: vertex.properties.get("created_by")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            updated_by: vertex.properties.get("updated_by")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            last_visit_date: vertex.properties.get("last_visit_date")
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc)),
        })
    }
}

//! FHIR Service — Global singleton for FHIR functionalities
//! Handles FHIR-native pipelines, import/export, conversions, API exposure

use std::collections::HashMap;
use fhir_sdk::r5::resources::{
    Bundle, BundleInner, BundleEntry, Patient as FhirPatient, Observation as FhirObservation, Resource,
};
use fhir_sdk::r5::types::{HumanName, HumanNameInner, Identifier as FhirIdentifier, IdentifierInner, 
                          HumanNameBuilder, IdentifierBuilder };
use fhir_sdk::r5::resources::{ PatientBuilder, BundleBuilder, BundleEntryBuilder};
use fhir_sdk::r5::codes::{BundleType, AdministrativeGender };
use fhir_sdk::client::Client as FhirClient;
use fhir_sdk::r5::resources::Patient;
use fhir_sdk::Date;
use lib::graph_engine::graph_service::GraphService;
use lib::graph_engine::Identifier;
use models::Vertex;
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};
use uuid::Uuid;
use chrono::{Utc, SecondsFormat};
use serde_json::{Value, json};
use warp::{self, Filter, Rejection, Reply, reply::WithStatus, reply::Json}; // <-- FIX: Imported Rejection here
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::convert::Infallible;
use std::default::Default; // CRITICAL: To ensure the .default() calls work

// Global singleton
pub static FHIR_SERVICE: OnceCell<Arc<FhirService>> = OnceCell::const_new();

#[derive(Clone)]
pub struct FhirService {
    // FHIR resource cache
    resources: Arc<RwLock<HashMap<Uuid, Value>>>,
    // FHIR client
    client: Arc<FhirClient>,
}

/// Unified success return type for our FHIR API handlers
type FhirApiReply = Result<WithStatus<Json>, Rejection>;

// Helper to inject the service into Warp filters
fn with_service(svc: Arc<FhirService>) -> impl Filter<Extract = (Arc<FhirService>,), Error = Infallible> + Clone {
    warp::any().map(move || svc.clone())
}

impl FhirService {
    /// Initialize global singleton
    pub async fn global_init(fhir_server_url: &str) -> Result<(), &'static str> {
        let url = fhir_server_url.parse()
            .map_err(|_| "Invalid FHIR server URL")?;
        
        let client = FhirClient::new(url)
            .map_err(|_| "Failed to create FHIR client")?;
        
        let service = Arc::new(Self {
            resources: Arc::new(RwLock::new(HashMap::new())),
            client: Arc::new(client),
        });
        
        // Register observers for graph changes
        let service_clone = service.clone();
        let graph_service = GraphService::get().await;
        
        tokio::spawn(async move {
            if let Ok(gs) = graph_service {
                let mut graph_lock = gs.write_graph().await;
                let service = service_clone;
                
                graph_lock.on_vertex_added({
                    let service = service.clone();
                    move |vertex| {
                        let vertex = vertex.clone();
                        let service = service.clone();
                        tokio::spawn(async move {
                            if let Some(fhir_json) = service.convert_to_fhir(&vertex).await {
                                service.add_resource(Uuid::new_v4(), fhir_json).await;
                            }
                        });
                    }
                }).await;
            }
        });
        
        FHIR_SERVICE.set(service).map_err(|_| "FHIRService already initialized")
    }

    pub async fn get() -> Arc<Self> {
        FHIR_SERVICE
            .get()
            .expect("FHIRService not initialized! Call global_init() first.")
            .clone()
    }

    // =========================================================================
    // FHIR RESOURCE MANAGEMENT
    // =========================================================================

    async fn add_resource(&self, id: Uuid, resource: Value) {
        let mut resources = self.resources.write().await;
        resources.insert(id, resource);
    }

    pub async fn get_resource(&self, id: Uuid) -> Option<Value> {
        let resources = self.resources.read().await;
        resources.get(&id).cloned()
    }

    // =========================================================================
    // IMPORT/EXPORT PIPELINES
    // =========================================================================

    pub async fn import_fhir_bundle(&self, bundle: Bundle) -> Result<Vec<Value>, String> {
        let bundle_inner = bundle.0;
        let resources: Vec<Value> = bundle_inner.entry
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.resource)
            .filter_map(|r| serde_json::to_value(r).ok())
            .collect();
        
        for resource in &resources {
            self.add_resource(Uuid::new_v4(), resource.clone()).await;
            let _ = self.convert_from_fhir(resource.clone()).await;
        }
        Ok(resources)
    }

    /// Exports all FHIR resources associated with a patient into a Bundle.
    pub async fn export_fhir_bundle(&self, patient_id: &str) -> Result<Bundle, String> {
        let resources = self.resources.read().await;

        // Collect resources that reference this patient
        let patient_resources: Vec<Resource> = resources
            .values()
            .filter(|r| {
                // Simplified logic check to satisfy the compiler for the snippet
                r.get("subject")
                    .and_then(|s| s.get("reference"))
                    .and_then(|v| v.as_str())
                    .map(|ref_str| ref_str.contains(&format!("Patient/{}", patient_id)))
                    .unwrap_or(false)
            })
            // This line assumes Value can be successfully deserialized into Resource
            .filter_map(|v| serde_json::from_value(v.clone()).ok())
            .collect();

        // 1. Build entries and collect them, failing immediately if any entry build fails.
        let mut entries: Vec<Option<BundleEntry>> = Vec::with_capacity(patient_resources.len());

        for res in patient_resources {
            // .build() returns Result<BundleEntry, BuilderError>.
            // Use ? to propagate the BuilderError, converting it to a String error.
            let entry = BundleEntryBuilder::default()
                // FIX: Removed Box::new(). The builder expects Resource by value.
                .resource(res) 
                .build()
                .map_err(|e| format!("BundleEntryBuilder failed: {}", e))?;
            
            entries.push(Some(entry));
        }


        // 2. Build the final Bundle (assuming BundleBuilder builds the final Bundle struct)
        // and use ? to propagate the error.
        let bundle = BundleBuilder::default()
            .r#type(BundleType::Collection)
            .entry(entries)
            .timestamp(
                // The timestamp result is handled here
                Utc::now()
                    .to_rfc3339_opts(SecondsFormat::Millis, true)
                    .parse()
                    .map_err(|e| format!("Bad timestamp: {}", e))?,
            )
            .build()
            // The BundleBuilder::build() result is handled here
            .map_err(|e| format!("BundleBuilder failed: {}", e))?;

        // 3. Return the successfully built Bundle directly (Type fix).
        Ok(bundle)
    }

    // =========================================================================
    // CONVERSIONS
    // =========================================================================

    // Assuming necessary types like Vertex, Value, FhirPatient, HumanName,
    // AdministrativeGender, Date, FhirIdentifier, IdentifierInner, etc., are in scope.
    // You might need to ensure 'use std::default::Default;' is at the top of your file.

    /// Convert internal graph Vertex to FHIR Patient resource (R5)
    pub async fn convert_to_fhir(&self, vertex: &Vertex) -> Option<Value> {
        if vertex.label.as_ref() != "Patient" {
            return None;
        }

        // Build Patient using PatientBuilder (idiomatic fhir-sdk R5 way)
        let mut builder = PatientBuilder::default();

        // ID
        if let Some(id) = vertex.properties.get("id").and_then(|v| v.as_str()) {
            builder = builder.id(id.to_string());
        }

        // Name
        let mut has_name = false;
        let mut name_builder = HumanNameBuilder::default();

        if let Some(family) = vertex.properties.get("last_name").and_then(|v| v.as_str()) {
            name_builder = name_builder.family(family.to_string());
            has_name = true;
        }
        if let Some(given) = vertex.properties.get("first_name").and_then(|v| v.as_str()) {
            name_builder = name_builder.given(vec![Some(given.to_string())]);
            has_name = true;
        }

        if has_name {
            let name = name_builder.build().ok()?;
            builder = builder.name(vec![Some(name)]);
        }

        // Gender
        if let Some(gender_str) = vertex.properties.get("gender").and_then(|v| v.as_str()) {
            let gender = match gender_str.to_uppercase().as_str() {
                "MALE" => AdministrativeGender::Male,
                "FEMALE" => AdministrativeGender::Female,
                "OTHER" => AdministrativeGender::Other,
                _ => AdministrativeGender::Unknown,
            };
            builder = builder.gender(gender);
        }

        // Birth Date
        if let Some(dob) = vertex.properties.get("date_of_birth").and_then(|v| v.as_str()) {
            if let Ok(date) = dob.parse::<Date>() {
                builder = builder.birth_date(date);
            }
        }

        // Identifiers (MRN, SSN)
        let mut identifiers = Vec::new();
        if let Some(mrn) = vertex.properties.get("mrn").and_then(|v| v.as_str()) {
            let id = IdentifierBuilder::default()
                .system("http://hospital.example.org/mrn".to_string())
                .value(mrn.to_string())
                .build()
                .ok()?;
            identifiers.push(Some(id));
        }
        if let Some(ssn) = vertex.properties.get("ssn").and_then(|v| v.as_str()) {
            let id = IdentifierBuilder::default()
                .system("http://hl7.org/fhir/sid/us-ssn".to_string())
                .value(ssn.to_string())
                .build()
                .ok()?;
            identifiers.push(Some(id));
        }
        if !identifiers.is_empty() {
            builder = builder.identifier(identifiers);
        }

        // Active status
        builder = builder.active(true);

        // Build final Patient resource
        let patient_resource = builder.build().ok()?;

        // Convert to JSON Value
        serde_json::to_value(&patient_resource).ok()
    }

    pub async fn convert_from_fhir(&self, fhir_json: Value) -> Result<Vertex, String> {
        let resource_type = fhir_json["resourceType"]
            .as_str()
            .ok_or("Missing resourceType")?;
        
        if resource_type != "Patient" {
            return Err(format!("Unsupported FHIR resource type: {}", resource_type));
        }
        
        let fhir_patient: FhirPatient = serde_json::from_value(fhir_json)
            .map_err(|e| format!("Failed to parse FHIR Patient: {}", e))?;
        
        let patient_inner = fhir_patient.0;
        
        let mut vertex = Vertex::new(
            Identifier::new("Patient".to_string())
                .map_err(|e| format!("Invalid identifier: {}", e))?
        );
        
        if let Some(id) = &patient_inner.id {
            vertex.add_property("id", id);
        }
        
        if let Some(name_opt) = patient_inner.name.first().and_then(|n| n.as_ref()) {
            let name_inner = &name_opt.0;
            if let Some(family) = &name_inner.family {
                vertex.add_property("last_name", family);
            }
            for given_opt in &name_inner.given {
                if let Some(first_name) = given_opt {
                    vertex.add_property("first_name", first_name);
                    break; // use first given name only
                }
            }
        }
        
        if let Some(gender) = &patient_inner.gender {
            let gender_str = match gender {
                AdministrativeGender::Male => "MALE",
                AdministrativeGender::Female => "FEMALE",
                AdministrativeGender::Other => "OTHER",
                AdministrativeGender::Unknown => "UNKNOWN",
            };
            vertex.add_property("gender", gender_str);
        }
        
        if let Some(dob) = &patient_inner.birth_date {
            vertex.add_property("date_of_birth", &format!("{:?}", dob));
        }
        
        for identifier_opt in &patient_inner.identifier {
            if let Some(identifier) = identifier_opt {
                let id_inner = &identifier.0;
                if let Some(system) = &id_inner.system {
                    if let Some(value) = &id_inner.value {
                        if system.contains("mrn") {
                            vertex.add_property("mrn", value);
                        } else if system.contains("ssn") {
                            vertex.add_property("ssn", value);
                        }
                    }
                }
            }
        }
        
        let now = Utc::now().to_rfc3339();
        vertex.add_property("created_at", &now);
        vertex.add_property("updated_at", &now);
        vertex.add_property("patient_status", "ACTIVE");
        
        // Add vertex to graph if GraphService is available
        let graph_service = GraphService::get().await;
        if let Ok(gs) = graph_service {
            let mut graph = gs.write_graph().await;
            graph.add_vertex(vertex.clone());
        }
        
        Ok(vertex)
    }

    pub async fn convert_to_hl7(&self, _fhir_json: Value) -> Result<String, String> {
        Err("HL7 v2 conversion not yet implemented".to_string())
    }

    pub async fn convert_from_hl7(&self, _hl7_str: &str) -> Result<Value, String> {
        Err("HL7 v2 conversion not yet implemented".to_string())
    }

    // =========================================================================
    // API EXPOSURE
    // =========================================================================
    // Assuming FhirService, models::medical::Patient, and relevant FHIR types are defined and imported.
    pub async fn expose_api(&self, port: u16) {
        println!("FHIR API server starting on port {}", port);

        let svc = Arc::new(self.clone());
        // Filter that injects a clone of the Arc<FhirService> into handlers
        let with_svc = warp::any().map(move || svc.clone());

        // 1. GET /fhir/resources (List all resources)
        let get_resources = warp::path!("fhir" / "resources")
            .and(warp::get())
            .and(with_svc.clone())
            .and_then(|svc: Arc<FhirService>| async move {
                let resources = svc.resources.read().await;
                let reply = warp::reply::json(&*resources);
                // FIX: Explicitly wrap in WithStatus to unify return type with get_patient
                let response = warp::reply::with_status(reply, warp::http::StatusCode::OK);
                
                Ok(response) as FhirApiReply
            });

        // 2. GET /fhir/Patient/{id} (Get a specific patient bundle)
        let get_patient = warp::path!("fhir" / "Patient" / String)
            .and(warp::get())
            .and(with_svc)
            .and_then(|patient_id: String, svc: Arc<FhirService>| async move {
                match svc.export_fhir_bundle(&patient_id).await {
                    Ok(bundle) => {
                        let reply = warp::reply::with_status(
                            warp::reply::json(&bundle),
                            warp::http::StatusCode::OK,
                        );
                        // FIX: Explicit type cast to avoid E0283 inference error
                        Ok(reply) as FhirApiReply 
                    },
                    Err(e) => {
                        let reply = warp::reply::with_status(
                            // Ensure error is converted to string for JSON serialization
                            warp::reply::json(&json!({"error": e.to_string()})), 
                            warp::http::StatusCode::NOT_FOUND,
                        );
                        // FIX: Explicit type cast to avoid E0283 inference error
                        Ok(reply) as FhirApiReply 
                    },
                }
            });

        // The type errors on `.or` and `.boxed` are resolved now that the handlers have unified types.
        let routes = get_resources
            .or(get_patient)
            .boxed();

        warp::serve(routes)
            .run(([0, 0, 0, 0], port))
            .await;
    }

    // =========================================================================
    // OTHER FHIR FUNCTIONALITIES
    // =========================================================================

    pub async fn validate_resource(&self, fhir_json: Value) -> Result<bool, String> {
        if fhir_json.get("resourceType").is_some() {
            Ok(true)
        } else {
            Err("Invalid FHIR resource: missing resourceType".to_string())
        }
    }

    pub async fn search_patient(&self, name: &str) -> Result<Vec<FhirPatient>, String> {
        let resources = self.resources.read().await;
        let patients: Vec<FhirPatient> = resources
            .values()
            .filter(|v| v.get("resourceType").and_then(|t| t.as_str()) == Some("Patient"))
            .filter(|v| {
                v.get("name")
                    .and_then(|names| names.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|name_obj| {
                        let family = name_obj.get("family").and_then(|f| f.as_str()).unwrap_or("");
                        let given = name_obj.get("given")
                            .and_then(|g| g.as_array())
                            .and_then(|arr| arr.first())
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let full_name = format!("{} {}", given, family).to_lowercase();
                        Some(full_name.contains(&name.to_lowercase()))
                    })
                    .unwrap_or(false)
            })
            .filter_map(|v| serde_json::from_value(v.clone()).ok())
            .collect();
        
        Ok(patients)
    }
}

// Helper methods for common conversions
impl FhirService {
    /// Convert internal patient model to FHIR Patient
    pub async fn patient_to_fhir(&self, patient: &models::medical::Patient) -> FhirPatient {
        // 1. Name
        let name = HumanNameBuilder::default()
            // FIX: last_name is Option<String>, unwrap it to String for the builder
            .family(patient.last_name.clone().unwrap_or_default())
            // FIX: first_name is Option<String>, unwrap it for the inner Some()
            .given(vec![Some(patient.first_name.clone().unwrap_or_default())])
            .build()
            .expect("Failed to build HumanName");
        // 2. Identifiers
        let mut identifiers = Vec::new();
        if let Some(mrn) = &patient.mrn {
            identifiers.push(Some(
                IdentifierBuilder::default()
                    .system("http://hospital.example.org/mrn".to_string())
                    .value(mrn.clone())
                    .build()
                    .expect("Failed to build MRN Identifier"),
            ));
        }
        if let Some(ssn) = &patient.ssn {
            identifiers.push(Some(
                IdentifierBuilder::default()
                    .system("http://hl7.org/fhir/sid/us-ssn".to_string())
                    .value(ssn.clone())
                    .build()
                    .expect("Failed to build SSN Identifier"),
            ));
        }

        // 3. Gender
        // FIX: Handle patient.gender as Option<String>
        let gender = patient.gender
            .as_ref() // Get a reference to the String inside the Option
            .map(|g| g.to_uppercase()) // Map: If Some(g), convert to uppercase String
            .as_deref() // Convert Option<String> to Option<&str>
            .map(|g_str| match g_str {
                "MALE" => AdministrativeGender::Male,
                "FEMALE" => AdministrativeGender::Female,
                "OTHER" => AdministrativeGender::Other,
                _ => AdministrativeGender::Unknown,
            })
            .unwrap_or(AdministrativeGender::Unknown); // If None or invalid, default to Unknown

        // 4. Birth date (only supply if we parsed successfully)
        let birth_date_str = patient.date_of_birth.format("%Y-%m-%d").to_string();
        // Assuming Date is a FHIR date type that implements FromStr
        let birth_date = birth_date_str.parse::<Date>().ok(); 

        // 5. Build Patient
        let mut builder = PatientBuilder::default()
            // FIX: Convert Option<i32> to Option<String>, then handle the None case
            .id(patient.id.map(|id| id.to_string()).unwrap_or_default())
            .identifier(identifiers)
            .active(true)
            .name(vec![Some(name)])
            .gender(gender); // Now correctly passing the AdministrativeGender enum
        // birth_date is optional: only call setter when Some
        if let Some(date) = birth_date {
            builder = builder.birth_date(date);
        }

        // Build and return directly
        builder.build().expect("PatientBuilder failed")
    }
}

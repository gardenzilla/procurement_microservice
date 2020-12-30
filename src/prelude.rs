use gzlib::proto::procurement::{ProcurementItem, ProcurementObject, UplCandidate};

use crate::procurement;

pub enum ServiceError {
  InternalError(String),
  NotFound(String),
  AlreadyExists(String),
  BadRequest(String),
}

impl ServiceError {
  pub fn internal_error(msg: &str) -> Self {
    ServiceError::InternalError(msg.to_string())
  }
  pub fn not_found(msg: &str) -> Self {
    ServiceError::NotFound(msg.to_string())
  }
  pub fn already_exist(msg: &str) -> Self {
    ServiceError::AlreadyExists(msg.to_string())
  }
  pub fn bad_request(msg: &str) -> Self {
    ServiceError::BadRequest(msg.to_string())
  }
}

impl std::fmt::Display for ServiceError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      ServiceError::InternalError(msg) => write!(f, "{}", msg),
      ServiceError::NotFound(msg) => write!(f, "{}", msg),
      ServiceError::AlreadyExists(msg) => write!(f, "{}", msg),
      ServiceError::BadRequest(msg) => write!(f, "{}", msg),
    }
  }
}

impl std::fmt::Debug for ServiceError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_tuple("")
      .field(&"ServiceError".to_string())
      .field(self)
      .finish()
  }
}

impl From<ServiceError> for ::tonic::Status {
  fn from(error: ServiceError) -> Self {
    match error {
      ServiceError::InternalError(msg) => ::tonic::Status::internal(msg),
      ServiceError::NotFound(msg) => ::tonic::Status::not_found(msg),
      ServiceError::AlreadyExists(msg) => ::tonic::Status::already_exists(msg),
      ServiceError::BadRequest(msg) => ::tonic::Status::invalid_argument(msg),
    }
  }
}

impl From<::packman::PackError> for ServiceError {
  fn from(error: ::packman::PackError) -> Self {
    match error {
      ::packman::PackError::ObjectNotFound => ServiceError::not_found(&error.to_string()),
      _ => ServiceError::internal_error(&error.to_string()),
    }
  }
}

pub type ServiceResult<T> = Result<T, ServiceError>;

impl From<std::env::VarError> for ServiceError {
  fn from(error: std::env::VarError) -> Self {
    ServiceError::internal_error(&format!("ENV KEY NOT FOUND. {}", error))
  }
}

impl From<procurement::Procurement> for ProcurementObject {
  fn from(f: procurement::Procurement) -> Self {
    Self {
      id: f.id,
      source_id: f.source_id,
      reference: f.reference,
      estimated_delivery_date: match f.estimated_delivery_date {
        Some(delivery_date) => delivery_date.to_rfc3339(),
        None => "".to_string(),
      },
      items: f
        .items
        .iter()
        .map(|item| ProcurementItem {
          sku: item.sku,
          ordered_amount: item.ordered_amount,
          expected_net_price: item.expected_net_price,
        })
        .collect::<Vec<ProcurementItem>>(),
      upls: f
        .upl_candidates
        .iter()
        .map(|upl| UplCandidate {
          upl_id: upl.upl_id.clone(),
          sku: upl.sku,
          upl_piece: upl.upl_piece,
          best_before: match upl.best_before {
            Some(bbefore) => bbefore.to_rfc3339(),
            None => "".to_string(),
          },
        })
        .collect::<Vec<UplCandidate>>(),
      created_at: f.created_at.to_rfc3339(),
      created_by: f.created_by,
    }
  }
}

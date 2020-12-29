use chrono::prelude::*;
use packman::VecPackMember;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Status {
  New,
  Ordered,
  Arrived,
  Processing,
  Closed,
}

impl Default for Status {
  fn default() -> Self {
    Status::New
  }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Procurement {
  id: u32,
  source_id: u32,
  reference: String,
  estimated_delivery_date: DateTime<Utc>,
  items: Vec<ProcurementItem>,
  upl_candidates: Vec<UplCandidate>,
  status: Status,
  created_at: DateTime<Utc>,
  created_by: String,
}

impl VecPackMember for Procurement {
  type Out = u32;

  fn get_id(&self) -> &Self::Out {
    &self.id
  }
}

impl Default for Procurement {
  fn default() -> Self {
    Self {
      id: 0,
      source_id: 0,
      reference: "".into(),
      estimated_delivery_date: Utc::now(),
      items: Vec::new(),
      upl_candidates: Vec::new(),
      status: Status::default(),
      created_at: Utc::now(),
      created_by: "".into(),
    }
  }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ProcurementItem {
  sku: u32,
  ordered_amount: u32,
  expected_net_price: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct UplCandidate {
  upl_id: String,
  sku: u32,
  // if > 0 its bulk;
  // otherwise its simple
  upl_piece: u32,
  // Optional
  best_before: Option<DateTime<Utc>>,
}

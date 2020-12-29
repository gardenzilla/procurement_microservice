use chrono::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Status {
  New,
  Ordered,
  Arrived,
  Processing,
  Closed,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Procurement {
  id: u32,
  source_id: u32,
  reference: String,
  estimated_delivery_date: NaiveDate,
  items: Vec<ProcurementItem>,
  upl_candidates: Vec<UplCandidate>,
  status: Status,
  created_at: DateTime<Utc>,
  created_by: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProcurementItem {
  sku: u32,
  ordered_amount: u32,
  expected_net_price: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UplCandidate {
  upl_id: String,
  sku: u32,
  // if > 0 its bulk;
  // otherwise its simple
  upl_piece: u32,
  // Optional
  best_before: Option<NaiveDate>,
}

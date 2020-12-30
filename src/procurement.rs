use chrono::prelude::*;
use packman::VecPackMember;
use serde::{Deserialize, Serialize};

pub type ProcResult<T> = Result<T, String>;

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
  pub id: u32,
  pub source_id: u32,
  pub reference: String,
  pub estimated_delivery_date: Option<DateTime<Utc>>,
  pub sku_items: Vec<ProcurementItem>,
  pub upl_candidates: Vec<UplCandidate>,
  pub status: Status,
  pub created_at: DateTime<Utc>,
  pub created_by: String,
}

impl Procurement
where
  Self: Sized,
{
  pub fn new(id: u32, source_id: u32, created_by: String) -> Self {
    Self {
      id,
      source_id,
      reference: "".into(),
      estimated_delivery_date: None,
      sku_items: Vec::new(),
      upl_candidates: Vec::new(),
      status: Status::New,
      created_at: Utc::now(),
      created_by,
    }
  }
  pub fn set_reference(&mut self, reference: String) -> &Self {
    self.reference = reference;
    self
  }
  pub fn set_delivery_date(&mut self, delivery_date: Option<DateTime<Utc>>) -> &Self {
    self.estimated_delivery_date = delivery_date;
    self
  }
  pub fn sku_add(&mut self, sku: u32, amount: u32, net_price: u32) -> ProcResult<&Self> {
    todo!()
  }
  pub fn sku_update_amount(&mut self, sku: u32, amount: u32) -> ProcResult<&Self> {
    todo!()
  }
  pub fn sku_update_price(&mut self, sku: u32, price: u32) -> ProcResult<&Self> {
    todo!()
  }
  pub fn sku_remove(&mut self, sku: u32) -> ProcResult<&Self> {
    todo!()
  }
  pub fn upl_add(
    &mut self,
    upl_id: String,
    sku: u32,
    piece: u32,
    best_before: Option<DateTime<Utc>>,
  ) -> ProcResult<&Self> {
    todo!()
  }
  pub fn upl_update_sku(&mut self, upl_id: String, sku: u32) -> ProcResult<&Self> {
    todo!()
  }
  pub fn upl_update_piece(&mut self, upl_id: String, piece: u32) -> ProcResult<&Self> {
    todo!()
  }
  pub fn upl_update_best_before(
    &mut self,
    upl_id: String,
    best_before: Option<DateTime<Utc>>,
  ) -> ProcResult<&Self> {
    todo!()
  }
  pub fn upl_remove(&mut self, upl_id: String) -> ProcResult<&Self> {
    todo!()
  }
  // , _created_by: String for the future hystory implementation
  pub fn set_status_ordered(&mut self, _created_by: String) -> ProcResult<&Self> {
    todo!()
  }
  pub fn set_status_arrived(&mut self, _created_by: String) -> ProcResult<&Self> {
    todo!()
  }
  pub fn set_status_processing(&mut self, _created_by: String) -> ProcResult<&Self> {
    todo!()
  }
  pub fn set_status_closed(&mut self, _created_by: String) -> ProcResult<&Self> {
    todo!()
  }
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
      estimated_delivery_date: None,
      sku_items: Vec::new(),
      upl_candidates: Vec::new(),
      status: Status::default(),
      created_at: Utc::now(),
      created_by: "".into(),
    }
  }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ProcurementItem {
  pub sku: u32,
  pub ordered_amount: u32,
  pub expected_net_price: u32,
}

impl ProcurementItem {
  pub fn new(sku: u32, ordered_amount: u32, expected_net_price: u32) -> Self {
    Self {
      sku,
      ordered_amount,
      expected_net_price,
    }
  }
  pub fn update_ordered_amount(&mut self, new_amount: u32) {
    self.ordered_amount = new_amount;
  }
  pub fn update_price(&mut self, new_price: u32) {
    self.expected_net_price = new_price;
  }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct UplCandidate {
  pub upl_id: String,
  pub sku: u32,
  // if > 0 its bulk;
  // otherwise its simple
  pub upl_piece: u32,
  // Optional
  pub best_before: Option<DateTime<Utc>>,
}

impl UplCandidate {
  pub fn new(upl_id: String, sku: u32, upl_piece: u32, best_before: Option<DateTime<Utc>>) -> Self {
    Self {
      upl_id,
      sku,
      upl_piece,
      best_before,
    }
  }
  pub fn update_piece(&mut self, piece: u32) {
    self.upl_piece = piece;
  }
  pub fn update_best_before(&mut self, best_before: Option<DateTime<Utc>>) {
    self.best_before = best_before;
  }
}

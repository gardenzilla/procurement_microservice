use chrono::prelude::*;
use gzlib::id::LuhnCheck;
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
  pub items: Vec<ProcurementItem>,
  pub upl_candidates: Vec<UplCandidate>,
  pub status: Status,
  pub created_at: DateTime<Utc>,
  pub created_by: u32,
}

impl Procurement
where
  Self: Sized,
{
  /// Create a new procurement object
  pub fn new(id: u32, source_id: u32, created_by: u32) -> Self {
    Self {
      id,
      source_id,
      reference: "".into(),
      estimated_delivery_date: None,
      items: Vec::new(),
      upl_candidates: Vec::new(),
      status: Status::New,
      created_at: Utc::now(),
      created_by,
    }
  }

  /// Set reference
  pub fn set_reference(&mut self, reference: String) -> &Self {
    self.reference = reference;
    self
  }

  /// Set delivery date
  pub fn set_delivery_date(&mut self, delivery_date: Option<DateTime<Utc>>) -> &Self {
    self.estimated_delivery_date = delivery_date;
    self
  }

  /// Try add SKU
  /// Error if SKU already there
  pub fn sku_add(&mut self, sku: u32, amount: u32, net_price: u32) -> ProcResult<&Self> {
    // Check if SKU already there
    if self.items.iter().any(|item| item.sku == sku) {
      return Err("Ez a SKU már szerepel!".into());
    }
    self
      .items
      .push(ProcurementItem::new(sku, amount, net_price));
    Ok(self)
  }

  /// Try update SKU amount
  /// Error if SKU not there
  pub fn sku_update_amount(&mut self, sku: u32, amount: u32) -> ProcResult<&Self> {
    for item in &mut self.items {
      if item.sku == sku {
        item.update_ordered_amount(amount);
        return Ok(self);
      }
    }
    Err("A megadott SKU nem szerepel a rendelésben!".into())
  }

  /// Try update SKU price
  /// Error if SKU not there
  pub fn sku_update_price(&mut self, sku: u32, price: u32) -> ProcResult<&Self> {
    for item in &mut self.items {
      if item.sku == sku {
        item.update_price(price);
        return Ok(self);
      }
    }
    Err("A megadott SKU nem szerepel a rendelésben!".into())
  }

  /// Try remove SKU
  /// Error if SKU not there
  pub fn sku_remove(&mut self, sku: u32) -> ProcResult<&Self> {
    // Check if SKU not there
    if !self.items.iter().any(|item| item.sku == sku) {
      return Err("A megadott SKU nem szerepel a rendelésben".into());
    }
    // Remove SKU
    self.items.retain(|item| item.sku != sku);
    // Return self ref
    Ok(self)
  }

  /// Try add UPL
  /// Error if UPL ID already exist
  pub fn upl_add(
    &mut self,
    upl_id: String,
    sku: u32,
    piece: u32,
    best_before: Option<DateTime<Utc>>,
  ) -> ProcResult<&Self> {
    // Check if UPL ID already there
    if self.upl_candidates.iter().any(|c| c.upl_id == upl_id) {
      return Err("Az adott UPL azonosító már a rendelésben szerepel!".into());
    }
    // Push UPL candidate
    self
      .upl_candidates
      .push(UplCandidate::new(upl_id, sku, piece, best_before)?);
    // Return self ref
    Ok(self)
  }

  /// Try update UPL SKU
  /// Error if UPL ID not there
  pub fn upl_update_sku(&mut self, upl_id: &str, sku: u32) -> ProcResult<&Self> {
    for upl in &mut self.upl_candidates {
      if upl.upl_id == upl_id {
        upl.update_sku(sku);
        return Ok(self);
      }
    }
    Err("A megadott UPL azonosító nem szerepel a rendelésben!".into())
  }

  /// Try update UPL piece
  /// Error if UPL ID not there
  pub fn upl_update_piece(&mut self, upl_id: &str, piece: u32) -> ProcResult<&Self> {
    for upl in &mut self.upl_candidates {
      if upl.upl_id == upl_id {
        upl.update_piece(piece);
        return Ok(self);
      }
    }
    Err("A megadott UPL azonosító nem szerepel a rendelésben!".into())
  }

  /// Try update UPL best_before
  /// Error if UPL ID not there
  pub fn upl_update_best_before(
    &mut self,
    upl_id: &str,
    best_before: Option<DateTime<Utc>>,
  ) -> ProcResult<&Self> {
    for upl in &mut self.upl_candidates {
      if upl.upl_id == upl_id {
        upl.update_best_before(best_before);
        return Ok(self);
      }
    }
    Err("A megadott UPL azonosító nem szerepel a rendelésben!".into())
  }

  pub fn upl_update_all(
    &mut self,
    upl_id: &str,
    sku: u32,
    piece: u32,
    best_before: Option<DateTime<Utc>>,
  ) -> ProcResult<&Self> {
    self.upl_update_sku(upl_id, sku)?;
    self.upl_update_piece(upl_id, piece)?;
    self.upl_update_best_before(upl_id, best_before)?;
    Ok(self)
  }

  /// Try remove UPL
  /// Error if UPL ID not there
  pub fn upl_remove(&mut self, upl_id: String) -> ProcResult<&Self> {
    // Check if UPL ID not there
    if !self.upl_candidates.iter().any(|upl| *upl.upl_id == upl_id) {
      return Err("A megadott UPL azonosító nem szerepel a rendelésben".into());
    }
    // Remove UPL
    self.upl_candidates.retain(|upl| *upl.upl_id != upl_id);
    // Return self ref
    Ok(self)
  }

  /// Try set status to ordered
  // , _created_by: String for the future hystory implementation
  pub fn set_status_ordered(&mut self, _created_by: u32) -> ProcResult<&Self> {
    // Check if there is delivery date set
    if self.estimated_delivery_date.is_none() {
      return Err("Nincs beállítva várható érkezési dátum!".into());
    }
    // Check if its not an empty procurement
    if self.items.len() == 0 {
      return Err("A rendelés üres!".into());
    }
    // Set status ordered
    self.status = Status::Ordered;
    // Return self ref
    Ok(self)
  }

  /// Try set status to ordered
  pub fn set_status_arrived(&mut self, _created_by: u32) -> ProcResult<&Self> {
    match self.status {
      Status::Ordered => {
        self.status = Status::Arrived;
        Ok(self)
      }
      _ => Err("Csak megrendelve státuszú megrendelést lehet beérkezve státusszá állítani!".into()),
    }
  }

  /// Try set status to ordered
  pub fn set_status_processing(&mut self, _created_by: u32) -> ProcResult<&Self> {
    match self.status {
      Status::Ordered | Status::Arrived => {
        self.status = Status::Processing;
        Ok(self)
      }
      _ => Err(
        "Csak megrendelve, vagy beérkezett státuszt lehet feldolgozás alattra változtatni!".into(),
      ),
    }
  }

  /// Try set status to ordered
  pub fn set_status_closed(&mut self, _created_by: u32) -> ProcResult<&Self> {
    // Check if its status is Processing
    match self.status {
      Status::Processing => (),
      _ => return Err("Csak feldolgozás alatt lévő beszerzés zárható le!".into()),
    }

    // Check if all the requeired amount of UPLs are located in the procurement
    for item in &self.items {
      // Collect this SKU related UPL count
      let upl_count = self
        .upl_candidates
        .iter()
        .filter(|upl| upl.sku == item.sku)
        .count();
      // If UPL(s) missing! return error
      if item.ordered_amount as usize != upl_count {
        return Err(format!(
          "Az alábbi SKU-hoz ({}) még hiányzik {} db UPL!",
          item.sku,
          item.ordered_amount as usize - upl_count
        ));
      }
    }
    // Set closed status
    self.status = Status::Closed;
    // return self reference
    Ok(self)
  }

  /// Try to set status
  pub fn set_status(&mut self, status: Status, created_by: u32) -> ProcResult<&Self> {
    match status {
      Status::Ordered => self.set_status_ordered(created_by),
      Status::Arrived => self.set_status_arrived(created_by),
      Status::Processing => self.set_status_processing(created_by),
      Status::Closed => self.set_status_closed(created_by),
      _ => Err("Not allowed status change!".into()),
    }
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
      items: Vec::new(),
      upl_candidates: Vec::new(),
      status: Status::default(),
      created_at: Utc::now(),
      created_by: 0,
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
  pub fn new(
    upl_id: String,
    sku: u32,
    upl_piece: u32,
    best_before: Option<DateTime<Utc>>,
  ) -> ProcResult<Self> {
    // Check if ID correct to LuhnCheck
    upl_id
      .luhn_check_ref()
      .map_err(|_| "A megadott UPL ID hibás!".to_string())?;
    Ok(Self {
      upl_id,
      sku,
      upl_piece,
      best_before,
    })
  }
  pub fn update_sku(&mut self, sku: u32) {
    self.sku = sku;
  }
  pub fn update_piece(&mut self, piece: u32) {
    self.upl_piece = piece;
  }
  pub fn update_best_before(&mut self, best_before: Option<DateTime<Utc>>) {
    self.best_before = best_before;
  }
}

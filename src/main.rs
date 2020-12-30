use chrono::{DateTime, Local, Utc};
use gzlib::prelude::*;
use gzlib::proto::procurement::procurement_server::*;
use gzlib::proto::procurement::*;
use packman::*;
use prelude::{ServiceError, ServiceResult};
use std::{collections::HashMap, env, path::PathBuf};
use tokio::sync::{oneshot, Mutex};
use tonic::{transport::Server, Request, Response, Status};

mod prelude;
mod procurement;

struct ProcurementService {
  procurements: Mutex<VecPack<procurement::Procurement>>,
}

impl ProcurementService {
  // Create new ProcurementService
  fn new(db: VecPack<procurement::Procurement>) -> Self {
    Self {
      procurements: Mutex::new(db),
    }
  }

  /// Calculate the next procurement ID
  async fn next_id(&self) -> u32 {
    let mut last_id = 0;
    self.procurements.lock().await.iter().for_each(|proc| {
      let proc_id = proc.unpack().id;
      if proc_id > last_id {
        last_id = proc_id;
      }
    });
    last_id + 1
  }

  /// Create a new procurement
  async fn create_new(&self, r: CreateNewRequest) -> ServiceResult<ProcurementObject> {
    // Create the new procurement object
    let new_procurement =
      procurement::Procurement::new(self.next_id().await, r.source_id, r.created_by);

    // Store new procurement
    self
      .procurements
      .lock()
      .await
      .insert(new_procurement.clone())?;

    // Return procurement as ProcurementObject
    Ok(new_procurement.into())
  }

  async fn set_delivery(&self, r: SetDeliveryDateRequest) -> ServiceResult<ProcurementObject> {
    // Process delivery date
    let ddate: Option<DateTime<Utc>> = match r.delivery_date.len() {
      // If a not empty string, then try to parse as rfc3339
      x if x > 0 => {
        let date = DateTime::parse_from_rfc3339(&r.delivery_date)
          .map_err(|_| ServiceError::bad_request("A megadott dátum hibás!"))?;
        Some(date.with_timezone(&Utc))
      }
      // If empty string then None
      _ => None,
    };

    // Try to set delivery
    let res = self
      .procurements
      .lock()
      .await
      .find_id_mut(&r.procurement_id)?
      .as_mut()
      .unpack()
      .set_delivery_date(ddate)
      .clone();

    // Return self as ProcurementObject
    Ok(res.into())
  }

  async fn set_reference(&self, r: SetReferenceRequest) -> ServiceResult<ProcurementObject> {
    // Try to set reference
    let res = self
      .procurements
      .lock()
      .await
      .find_id_mut(&r.procurement_id)?
      .as_mut()
      .unpack()
      .set_reference(r.reference)
      .clone();

    // Return self as ProcurementObject
    Ok(res.into())
  }
}

#[tonic::async_trait]
impl Procurement for ProcurementService {
  async fn create_new(
    &self,
    request: Request<CreateNewRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    let res = self.create_new(request.into_inner()).await?;
    Ok(Response::new(res))
  }

  async fn set_delivery_date(
    &self,
    request: Request<SetDeliveryDateRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    let res = self.set_delivery(request.into_inner()).await?;
    Ok(Response::new(res))
  }

  async fn set_reference(
    &self,
    request: Request<SetReferenceRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    let res = self.set_reference(request.into_inner()).await?;
    Ok(Response::new(res))
  }

  async fn add_sku(
    &self,
    request: Request<AddSkuRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    todo!()
  }

  async fn remove_sku(
    &self,
    request: Request<RemoveSkuRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    todo!()
  }

  async fn set_sku_piece(
    &self,
    request: Request<SetSkuPieceRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    todo!()
  }

  async fn add_upl(
    &self,
    request: Request<AddUplRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    todo!()
  }

  async fn update_upl(
    &self,
    request: Request<UpdateUplRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    todo!()
  }

  async fn remove_upl(
    &self,
    request: Request<RemoveUplRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    todo!()
  }

  async fn remove(&self, request: Request<RemoveRequest>) -> Result<Response<()>, Status> {
    todo!()
  }

  async fn set_sku_price(
    &self,
    request: Request<SetSkuPriceRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    todo!()
  }

  async fn set_status(
    &self,
    request: Request<SetStatusRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    todo!()
  }
}

#[tokio::main]
async fn main() -> prelude::ServiceResult<()> {
  let db: VecPack<procurement::Procurement> =
    VecPack::load_or_init(PathBuf::from("data/procurement"))
      .expect("Error while loading procurement db");

  let procurement_service = ProcurementService::new(db);

  let addr = env::var("SERVICE_ADDR_PROCUREMENT")
    .unwrap_or("[::1]:50063".into())
    .parse()
    .unwrap();

  // Create shutdown channel
  let (tx, rx) = oneshot::channel();

  // Spawn the server into a runtime
  tokio::task::spawn(async move {
    Server::builder()
      .add_service(ProcurementServer::new(procurement_service))
      .serve_with_shutdown(addr, async { rx.await.unwrap() })
      .await
  });

  tokio::signal::ctrl_c().await.unwrap();

  println!("SIGINT");

  // Send shutdown signal after SIGINT received
  let _ = tx.send(());

  Ok(())
}

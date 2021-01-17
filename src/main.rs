use chrono::{DateTime, Utc};
use gzlib::proto;
use gzlib::proto::procurement::procurement_server::*;
use gzlib::proto::procurement::*;
use packman::*;
use prelude::{ServiceError, ServiceResult};
use std::{env, path::PathBuf};
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

  /// Get procurement by ID
  async fn get_by_id(&self, r: GetByIdRequest) -> ServiceResult<ProcurementObject> {
    let res = self
      .procurements
      .lock()
      .await
      .find_id(&r.procurement_id)?
      .unpack()
      .clone()
      .into();
    Ok(res)
  }

  /// Get all procurement IDs
  async fn get_all(&self) -> ServiceResult<Vec<u32>> {
    let res = self
      .procurements
      .lock()
      .await
      .iter()
      .map(|p| p.unpack().id)
      .collect::<Vec<u32>>();
    Ok(res)
  }

  /// Get info bulk
  async fn get_info_bulk(
    &self,
    r: GetInfoBulkRequest,
  ) -> ServiceResult<Vec<ProcurementInfoObject>> {
    let res = self
      .procurements
      .lock()
      .await
      .iter()
      .filter(|p| r.procurement_ids.contains(&p.unpack().id))
      .map(|p| p.unpack().clone().into())
      .collect::<Vec<ProcurementInfoObject>>();
    Ok(res)
  }

  /// Try set delivery date
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

  /// Try set reference
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

  /// Try to add SKU
  async fn add_sku(&self, r: AddSkuRequest) -> ServiceResult<ProcurementObject> {
    // Try to get SKU object
    let sku_object = r.sku.ok_or(ServiceError::internal_error(
      "Belső hiba! A SKU object üres!",
    ))?;

    // Try to add SKU
    let res = self
      .procurements
      .lock()
      .await
      .find_id_mut(&r.procurement_id)?
      .as_mut()
      .unpack()
      .sku_add(
        sku_object.sku,
        sku_object.ordered_amount,
        sku_object.expected_net_price,
      )
      .map_err(|e| ServiceError::bad_request(&e))?
      .clone();

    // Return procurement as ProcurementObject
    Ok(res.into())
  }

  /// Try to remove SKU
  async fn remove_sku(&self, r: RemoveSkuRequest) -> ServiceResult<ProcurementObject> {
    // Try to remove SKU
    let res = self
      .procurements
      .lock()
      .await
      .find_id_mut(&r.procurement_id)?
      .as_mut()
      .unpack()
      .sku_remove(r.sku)
      .map_err(|e| ServiceError::bad_request(&e))?
      .clone();

    // Return procurement as ProcurementObject
    Ok(res.into())
  }

  /// Try to set SKU piece
  async fn set_sku_piece(&self, r: SetSkuPieceRequest) -> ServiceResult<ProcurementObject> {
    // Try to set SKU piece
    let res = self
      .procurements
      .lock()
      .await
      .find_id_mut(&r.procurement_id)?
      .as_mut()
      .unpack()
      .sku_update_amount(r.sku, r.piece)
      .map_err(|e| ServiceError::bad_request(&e))?
      .clone();

    // Return procurement as ProcurementObject
    Ok(res.into())
  }

  /// Try to set SKU price
  async fn set_sku_price(&self, r: SetSkuPriceRequest) -> ServiceResult<ProcurementObject> {
    // Try to set SKU price
    let res = self
      .procurements
      .lock()
      .await
      .find_id_mut(&r.procurement_id)?
      .as_mut()
      .unpack()
      .sku_update_price(r.sku, r.expected_net_price)
      .map_err(|e| ServiceError::bad_request(&e))?
      .clone();

    // Return procurement as ProcurementObject
    Ok(res.into())
  }

  /// Try to add UPL
  async fn add_upl(&self, r: AddUplRequest) -> ServiceResult<ProcurementObject> {
    let upl_candidate = r.upl_candidate.ok_or(ServiceError::internal_error(
      "Missing UPL candidate from message!",
    ))?;

    // Process bestbefore date
    let bdate: Option<DateTime<Utc>> = match upl_candidate.best_before.len() {
      // If a not empty string, then try to parse as rfc3339
      x if x > 0 => {
        let date = DateTime::parse_from_rfc3339(&upl_candidate.best_before)
          .map_err(|_| ServiceError::bad_request("A megadott lejárati dátum hibás!"))?;
        Some(date.with_timezone(&Utc))
      }
      // If empty string then None
      _ => None,
    };

    let res = self
      .procurements
      .lock()
      .await
      .find_id_mut(&r.procurement_id)?
      .as_mut()
      .unpack()
      .upl_add(
        upl_candidate.upl_id,
        upl_candidate.sku,
        upl_candidate.upl_piece,
        bdate,
      )
      .map_err(|e| ServiceError::bad_request(&e))?
      .clone();

    // Return procurement as ProcurementObject
    Ok(res.into())
  }

  /// Try update UPL
  async fn update_upl(&self, r: UpdateUplRequest) -> ServiceResult<ProcurementObject> {
    // Process bestbefore date
    let bdate: Option<DateTime<Utc>> = match r.best_before.len() {
      // If a not empty string, then try to parse as rfc3339
      x if x > 0 => {
        let date = DateTime::parse_from_rfc3339(&r.best_before)
          .map_err(|_| ServiceError::bad_request("A megadott lejárati dátum hibás!"))?;
        Some(date.with_timezone(&Utc))
      }
      // If empty string then None
      _ => None,
    };

    let res = self
      .procurements
      .lock()
      .await
      .find_id_mut(&r.procurement_id)?
      .as_mut()
      .unpack()
      .upl_update_all(&r.upl_id, r.sku, r.piece, bdate)
      .map_err(|e| ServiceError::bad_request(&e))?
      .clone();

    // Return procurement as ProcurementObject
    Ok(res.into())
  }

  /// Try to remove UPL Candidate
  async fn remove_upl(&self, r: RemoveUplRequest) -> ServiceResult<ProcurementObject> {
    // Try to remove UPL candidate
    let res = self
      .procurements
      .lock()
      .await
      .find_id_mut(&r.procurement_id)?
      .as_mut()
      .unpack()
      .upl_remove(r.upl_id)
      .map_err(|e| ServiceError::bad_request(&e))?
      .clone();

    // Return procurement as ProcurementObject
    Ok(res.into())
  }

  /// Try to remove Procurement
  /// Only with Status::New
  async fn remove_procurement(&self, r: RemoveRequest) -> ServiceResult<()> {
    // Check if procurement exists and can be removed
    let can_remove: bool = self
      .procurements
      .lock()
      .await
      .find_id(&r.procurement_id)?
      .get(|p| {
        if let procurement::Status::New = p.status {
          return true;
        }
        false
      });

    // Try to remove as Pack
    if can_remove {
      self
        .procurements
        .lock()
        .await
        .remove_pack(&r.procurement_id)?;
    }

    // Returns Ok(())
    Ok(())
  }

  /// Try to set new Status to the procurement
  async fn set_stats(&self, r: SetStatusRequest) -> ServiceResult<ProcurementObject> {
    // Try to set new status
    let res = self
      .procurements
      .lock()
      .await
      .find_id_mut(&r.procurement_id)?
      .as_mut()
      .unpack()
      .set_status(
        match proto::procurement::Status::from_i32(r.status)
          .ok_or(ServiceError::bad_request("Nem létező státusz azonosító!"))?
        {
          proto::procurement::Status::Ordered => procurement::Status::Ordered,
          proto::procurement::Status::Arrived => procurement::Status::Arrived,
          proto::procurement::Status::Processing => procurement::Status::Processing,
          proto::procurement::Status::Closed => procurement::Status::Closed,
          proto::procurement::Status::New => procurement::Status::New,
        },
        r.created_by,
      )
      .map_err(|e| ServiceError::bad_request(&e))?
      .clone();

    // Return procurement as ProcurementObject
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

  async fn get_by_id(
    &self,
    request: Request<GetByIdRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    let res = self.get_by_id(request.into_inner()).await?;
    Ok(Response::new(res))
  }

  async fn get_all(&self, _request: Request<()>) -> Result<Response<ProcurementIds>, Status> {
    let procurement_ids = self.get_all().await?;
    Ok(Response::new(ProcurementIds { procurement_ids }))
  }

  type GetInfoBulkStream = tokio::sync::mpsc::Receiver<Result<ProcurementInfoObject, Status>>;

  async fn get_info_bulk(
    &self,
    request: Request<GetInfoBulkRequest>,
  ) -> Result<Response<Self::GetInfoBulkStream>, Status> {
    // Create channel for stream response
    let (mut tx, rx) = tokio::sync::mpsc::channel(100);

    // Get resources as Vec<SourceObject>
    let res = self.get_info_bulk(request.into_inner()).await?;

    // Send the result items through the channel
    tokio::spawn(async move {
      for ots in res.into_iter() {
        tx.send(Ok(ots)).await.unwrap();
      }
    });

    // Send back the receiver
    Ok(Response::new(rx))
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
    let res = self.add_sku(request.into_inner()).await?.clone();
    Ok(Response::new(res))
  }

  async fn remove_sku(
    &self,
    request: Request<RemoveSkuRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    let res = self.remove_sku(request.into_inner()).await?;
    Ok(Response::new(res))
  }

  async fn set_sku_piece(
    &self,
    request: Request<SetSkuPieceRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    let res = self.set_sku_piece(request.into_inner()).await?;
    Ok(Response::new(res))
  }

  async fn add_upl(
    &self,
    request: Request<AddUplRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    let res = self.add_upl(request.into_inner()).await?;
    Ok(Response::new(res))
  }

  async fn update_upl(
    &self,
    request: Request<UpdateUplRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    let res = self.update_upl(request.into_inner()).await?;
    Ok(Response::new(res))
  }

  async fn remove_upl(
    &self,
    request: Request<RemoveUplRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    let res = self.remove_upl(request.into_inner()).await?;
    Ok(Response::new(res))
  }

  async fn remove(&self, request: Request<RemoveRequest>) -> Result<Response<()>, Status> {
    let _ = self.remove_procurement(request.into_inner()).await?;
    Ok(Response::new(()))
  }

  async fn set_sku_price(
    &self,
    request: Request<SetSkuPriceRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    let res = self.set_sku_price(request.into_inner()).await?;
    Ok(Response::new(res))
  }

  async fn set_status(
    &self,
    request: Request<SetStatusRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    let res = self.set_stats(request.into_inner()).await?;
    Ok(Response::new(res))
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

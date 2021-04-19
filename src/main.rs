use chrono::{DateTime, Utc};
use futures_util::stream;
use gzlib::proto::{
  self,
  pricing::{pricing_client::PricingClient, GetPriceBulkRequest, PriceObject},
  product::{product_client::ProductClient, SkuObj},
  upl::{upl_client::UplClient, UplNew},
};
use gzlib::proto::{procurement::procurement_server::*, upl::UplObj};
use gzlib::proto::{procurement::*, product::GetSkuBulkRequest};
use packman::*;
use prelude::{service_address, ServiceError, ServiceResult};
use proto::email::{email_client::EmailClient, EmailRequest};
use std::{env, path::PathBuf};
use tokio::sync::{oneshot, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
  transport::{Channel, Server},
  Request, Response, Status,
};

mod prelude;
mod procurement;

struct ProcurementService {
  procurements: Mutex<VecPack<procurement::Procurement>>,
  client_upl: Mutex<UplClient<Channel>>,
  client_product: Mutex<ProductClient<Channel>>,
  client_pricing: Mutex<PricingClient<Channel>>,
  client_email: Mutex<EmailClient<Channel>>,
}

impl ProcurementService {
  // Create new ProcurementService
  fn new(
    db: VecPack<procurement::Procurement>,
    client_upl: UplClient<Channel>,
    client_product: ProductClient<Channel>,
    client_pricing: PricingClient<Channel>,
    client_email: EmailClient<Channel>,
  ) -> Self {
    Self {
      procurements: Mutex::new(db),
      client_upl: Mutex::new(client_upl),
      client_product: Mutex::new(client_product),
      client_pricing: Mutex::new(client_pricing),
      client_email: Mutex::new(client_email),
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
        upl_candidate.opened_sku,
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

  async fn try_close(&self, id: u32) -> ServiceResult<()> {
    if let Ok(procurement) = self.procurements.lock().await.find_id_mut(&id) {
      // 1. Check if status is Processing
      match procurement.unpack().status {
        procurement::Status::Processing => (),
        _ => {
          return Err(ServiceError::bad_request(
            "A beszerzés nem zárható le! A státusz nem Feldolgozás alatt.",
          ))
        }
      }

      // 2. Check if all new UPL IDS are not already taken
      let new_upl_ids = procurement
        .unpack()
        .upl_candidates
        .iter()
        .map(|u| u.upl_id.clone())
        .collect::<Vec<String>>();

      let mut all_upls: Vec<UplObj> = Vec::new();

      let mut all_upl_stream = self
        .client_upl
        .lock()
        .await
        .get_bulk(gzlib::proto::upl::BulkRequest {
          upl_ids: new_upl_ids,
        })
        .await
        .map_err(|e| ServiceError::bad_request(&e.to_string()))?
        .into_inner();

      while let Some(upl_obj) = all_upl_stream
        .message()
        .await
        .map_err(|e| ServiceError::bad_request(&e.to_string()))?
      {
        all_upls.push(upl_obj);
      }

      // If there is any found UPL with a new ID, then return error!
      if all_upls.len() > 0 {
        return Err(
          ServiceError::bad_request(&format!(
            "A beszerzés nem zárható le. Az alábbi UPL azonosítók már használatban vannak: {:?}",
            all_upls.into_iter().map(|u| u.id).collect::<Vec<String>>(),
          ))
          .into(),
        );
      }

      // Collect SKU IDs
      let sku_id = procurement
        .unpack()
        .items
        .iter()
        .map(|i| i.sku)
        .collect::<Vec<u32>>();

      // Load SKU objects to access SKU and product data
      let mut all_skus = self
        .client_product
        .lock()
        .await
        .get_sku_bulk(GetSkuBulkRequest {
          sku_id: sku_id.clone(),
        })
        .await
        .map_err(|e| ServiceError::bad_request(&e.to_string()))?
        .into_inner();

      let mut sku_objects: Vec<SkuObj> = Vec::new();

      while let Some(sku_obj) = all_skus
        .message()
        .await
        .map_err(|e| ServiceError::bad_request(&e.to_string()))?
      {
        sku_objects.push(sku_obj);
      }

      // Load PriceObjects to access SKU price data
      let mut all_prices = self
        .client_pricing
        .lock()
        .await
        .get_price_bulk(GetPriceBulkRequest { skus: sku_id })
        .await
        .map_err(|e| ServiceError::bad_request(&e.to_string()))?
        .into_inner();

      let mut price_objects: Vec<PriceObject> = Vec::new();

      while let Some(price_obj) = all_prices
        .message()
        .await
        .map_err(|e| ServiceError::bad_request(&e.to_string()))?
      {
        price_objects.push(price_obj);
      }

      // Create empty result vector
      let mut result_upl_candidates: Vec<UplNew> = Vec::new();

      for sku_item in procurement.unpack().items.iter() {
        // Try find related SKU object
        let sku_obj =
          sku_objects
            .iter()
            .find(|so| so.sku == sku_item.sku)
            .ok_or(ServiceError::bad_request(
              "A beszerzés nem létező SKUt tartalmaz!",
            ))?;

        // Try find related Price object
        let price_obj = price_objects
          .iter()
          .find(|po| po.sku == sku_item.sku)
          .ok_or(ServiceError::bad_request(&format!(
            "A beszerzés alábbi SKUja nem rendelkezik eladási árral: #{}, {}",
            sku_item.sku, sku_obj.display_name
          )))?;

        // Collect UPLs related to this SKU item
        let mut u_candidates = procurement
          .unpack()
          .upl_candidates
          .iter()
          .filter(|upl_candidate| upl_candidate.sku == sku_item.sku)
          .map(|uc| UplNew {
            upl_id: uc.upl_id.clone(),
            product_id: sku_obj.product_id,
            sku: uc.sku,
            best_before: match uc.best_before {
              Some(bb) => bb.to_rfc3339().clone(),
              None => "".to_string(),
            },
            stock_id: 1, // todo: refact this value to grab from ENV variable
            procurement_id: procurement.unpack().id,
            is_opened: uc.opened_sku,
            created_by: procurement.created_by,
            product_unit: sku_obj.unit.clone(),
            piece: uc.upl_piece,
            sku_divisible_amount: sku_obj.divisible_amount,
            sku_divisible: sku_obj.can_divide,
            sku_net_price: price_obj.price_net_retail,
            sku_vat: price_obj.vat.clone(),
            sku_gross_price: price_obj.price_gross_retail,
            procurement_net_price_sku: sku_item.expected_net_price,
          })
          .collect::<Vec<UplNew>>();

        // Check best_before if SKU is perishable
        if sku_obj.perishable {
          match u_candidates.iter().all(|uc| uc.best_before.len() > 0) {
            true => (),
            false => {
              return Err(
                ServiceError::bad_request(&format!(
                  "Az alábbi SKU romlandó, viszont nem minden UPL-hez van lejárat rögzítve: {}",
                  &sku_obj.display_name
                ))
                .into(),
              )
            }
          }
        }

        // Check if all UPL count is the required one
        if u_candidates.iter().fold(0, |acc, uc| {
          acc
            + match uc.is_opened {
              true => 1,
              false => uc.piece,
            }
        }) != sku_item.ordered_amount
        {
          return Err(
            ServiceError::bad_request(&format!(
              "A beszerzés nem zárható le! Az alábbi SKU nem rendelkezik minden UPL-el: {}",
              &sku_obj.display_name
            ))
            .into(),
          );
        }

        // Add SKU related upl candidates into the result upl candidates
        result_upl_candidates.append(&mut u_candidates);
      }

      // All UPL are fine, create request stream
      let request = Request::new(stream::iter(result_upl_candidates));

      // 4. Create UPLs
      let created_upl_ids = self
        .client_upl
        .lock()
        .await
        .create_new_bulk(request)
        .await
        .map_err(|e| ServiceError::bad_request(&e.to_string()))?
        .into_inner()
        .upl_ids;

      // Send email to sysadmin if not all UPLs are created!
      if procurement.unpack().upl_candidates.len() != created_upl_ids.len() {
        self
          .client_email
          .lock()
          .await
          .send_email(EmailRequest {
            to: "peter.mezei@gardenova.hu".to_string(),
            subject: "Proc hiba! Nem minden UPL jött létre!".to_string(),
            body: format!(
              "UPL létrehozás hiba! Nem minden UPL jött létre! Proc id: {}! {} helyett {}!",
              procurement.id,
              procurement.upl_candidates.len(),
              created_upl_ids.len()
            ),
          })
          .await
          .map_err(|e| ServiceError::bad_request(&e.to_string()))?;
      }
    }

    Ok(())
  }

  /// Try to set new Status to the procurement
  async fn set_status(&self, r: SetStatusRequest) -> ServiceResult<ProcurementObject> {
    // Set requested new status
    let new_status = match proto::procurement::Status::from_i32(r.status)
      .ok_or(ServiceError::bad_request("Nem létező státusz azonosító!"))?
    {
      proto::procurement::Status::Ordered => procurement::Status::Ordered,
      proto::procurement::Status::Arrived => procurement::Status::Arrived,
      proto::procurement::Status::Processing => procurement::Status::Processing,
      proto::procurement::Status::Closed => procurement::Status::Closed,
      proto::procurement::Status::New => procurement::Status::New,
    };

    match new_status {
      // If new status is closed, try to close it
      procurement::Status::Closed => self.try_close(r.procurement_id).await?,
      _ => (),
    }

    // Try to set new status
    let res = self
      .procurements
      .lock()
      .await
      .find_id_mut(&r.procurement_id)?
      .as_mut()
      .unpack()
      .set_status(new_status, r.created_by)
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

  type GetInfoBulkStream = ReceiverStream<Result<ProcurementInfoObject, Status>>;

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
    Ok(Response::new(ReceiverStream::new(rx)))
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
    let res = self.set_status(request.into_inner()).await?;
    Ok(Response::new(res))
  }
}

#[tokio::main]
async fn main() -> prelude::ServiceResult<()> {
  let db: VecPack<procurement::Procurement> =
    VecPack::load_or_init(PathBuf::from("data/procurement"))
      .expect("Error while loading procurement db");

  let client_upl = UplClient::connect(service_address("SERVICE_ADDR_UPL"))
    .await
    .expect("Could not connect to UPL service");

  let client_product = ProductClient::connect(service_address("SERVICE_ADDR_PRODUCT"))
    .await
    .expect("Could not connect to PRODUCT service");

  let client_pricing = PricingClient::connect(service_address("SERVICE_ADDR_PRICING"))
    .await
    .expect("Could not connect to PRICING service");

  let client_email = EmailClient::connect(service_address("SERVICE_ADDR_EMAIL"))
    .await
    .expect("Could not connect to email service");

  let procurement_service =
    ProcurementService::new(db, client_upl, client_product, client_pricing, client_email);

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

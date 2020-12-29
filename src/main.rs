use gzlib::prelude::*;
use gzlib::proto::procurement::procurement_server::*;
use gzlib::proto::procurement::*;
use packman::*;
use std::{collections::HashMap, env, path::PathBuf};
use tokio::sync::{oneshot, Mutex};
use tonic::{transport::Server, Request, Response, Status};

mod prelude;
mod procurement;

struct ProcurementService {
  procurements: Mutex<VecPack<procurement::Procurement>>,
}

impl ProcurementService {
  fn new(db: VecPack<procurement::Procurement>) -> Self {
    Self {
      procurements: Mutex::new(db),
    }
  }
}

#[tonic::async_trait]
impl Procurement for ProcurementService {
  async fn create_new(
    &self,
    request: Request<CreateNewRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    todo!()
  }

  async fn set_delivery_date(
    &self,
    request: Request<SetDeliveryDateRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    todo!()
  }

  async fn set_reference(
    &self,
    request: Request<SetReferenceRequest>,
  ) -> Result<Response<ProcurementObject>, Status> {
    todo!()
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

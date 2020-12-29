use packman::*;
use tokio::sync::{oneshot, Mutex};

mod procurement;

struct ProcurementService {
  procurements: Mutex<VecPack<procurement::Procurement>>,
}

fn main() {}

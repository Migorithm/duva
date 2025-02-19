use crate::domains::query_parsers::QueryIO;
use crate::services::interface::TRead;
use tokio::net::tcp::OwnedReadHalf;
use tokio::select;

pub mod follower;
pub mod leader;
pub mod non_data_peer;

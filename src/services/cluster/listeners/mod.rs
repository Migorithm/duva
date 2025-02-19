use crate::services::{interface::TRead, query_io::QueryIO};
use tokio::net::tcp::OwnedReadHalf;
use tokio::select;

pub mod follower;
pub mod leader;
pub mod non_data_peer;

use super::ReactorKillSwitch;
use tokio::net::tcp::OwnedReadHalf;

pub trait TListen {
    fn listen(
        self,
        rx: ReactorKillSwitch,
    ) -> impl std::future::Future<Output = OwnedReadHalf> + Send;
}

use tokio::net::tcp::OwnedReadHalf;

use super::ReactorKillSwitch;

pub trait TListen {
    fn listen(
        self,
        rx: ReactorKillSwitch,
    ) -> impl std::future::Future<Output = OwnedReadHalf> + Send;
}

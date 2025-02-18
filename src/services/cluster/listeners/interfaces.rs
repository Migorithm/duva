use tokio::net::tcp::OwnedReadHalf;

pub trait TListenClusterPeer {
    fn listen(
        self,
        rx: ReactorKillSwitch,
    ) -> impl std::future::Future<Output = OwnedReadHalf> + Send;
}

pub(super) type ReactorKillSwitch = tokio::sync::oneshot::Receiver<()>;

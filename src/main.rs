use duva::{
    adapters::cancellation_token::CancellationTokenFactory,
    services::config::{actor::ConfigActor, manager::ConfigManager},
    StartUpFacade,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // bootstrap dependencies
    let config_manager = ConfigManager::new(ConfigActor::default());

    let mut start_up_runner = StartUpFacade::new(CancellationTokenFactory, config_manager);

    start_up_runner.run(()).await
}

#[tokio::test]
async fn test_cancellation_token() {
    use duva::client_utils::ClientStreamHandler;
    use duva::services::interface::TCancellationNotifier;
    use duva::services::interface::TCancellationTokenFactory;
    use duva::services::interface::TCancellationWatcher;
    use duva::TNotifyStartUp;
    use std::sync::Arc;

    use tokio::net::TcpStream;
    // GIVEN

    pub struct StartFlag(pub Arc<tokio::sync::Notify>);
    impl TNotifyStartUp for StartFlag {
        fn notify_startup(&self) {
            self.0.notify_waiters();
        }
    }
    struct TestNotifier(tokio::sync::oneshot::Sender<()>);
    impl TCancellationNotifier for TestNotifier {
        fn notify(self) {
            let _ = self.0.send(());
        }
    }
    #[derive(Clone, Copy)]
    pub struct TestCancellationTokenFactory;
    impl TCancellationTokenFactory for TestCancellationTokenFactory {
        fn create(&self, _timeout: u64) -> (impl TCancellationNotifier, impl TCancellationWatcher) {
            let (tx, rx) = tokio::sync::oneshot::channel();

            (TestNotifier(tx), rx)
        }
    }

    let config = ConfigActor::default();
    let mut manager = ConfigManager::new(config);

    manager.port = 55534;

    let notify = Arc::new(tokio::sync::Notify::new());
    let start_flag = StartFlag(notify.clone());

    let mut start_up_facade = StartUpFacade::new(TestCancellationTokenFactory, manager.clone());

    let handler = tokio::spawn(async move {
        start_up_facade.run(start_flag).await.unwrap();
    });

    //warm up time
    notify.notified().await;

    let client_stream = TcpStream::connect(manager.bind_addr()).await.unwrap();
    let mut h: ClientStreamHandler = client_stream.into_split().into();

    // WHEN
    h.send(b"*5\r\n$3\r\nSET\r\n$10\r\nsomanyrand\r\n$3\r\nbar\r\n$2\r\npx\r\n$3\r\n300\r\n").await;

    // THEN
    assert_eq!(h.get_response().await, "-Error operation cancelled due to timeout\r\n");

    handler.abort();
}

use raio::runtime::Runtime;
use raio::serializer::{SerdeSerializer, Serializer};
use raio::transport::tcp::TcpTransport;
use raio::transport::{Receiver, Sender, Transport};

#[test]
fn typed_value_over_the_wire() {
    Runtime::new().block_on(async {
        let mut transport = TcpTransport::new().await;
        let endpoint = transport.endpoint().to_string();
        let (mut sender, _) = transport.connect(&endpoint).await;
        let (_, mut receiver) = transport.accept().await;

        let serializer = SerdeSerializer;
        let value: (i64, String, Vec<f32>) = (-7, "hello, port".to_string(), vec![1.5, -0.25]);

        sender.send(&serializer.serialize(&value)).await;
        let received: (i64, String, Vec<f32>) =
            serializer.deserialize(&receiver.recv().await.unwrap());
        assert_eq!(received, value);
    });
}

#[test]
fn mixed_types_share_one_connection_in_order() {
    Runtime::new().block_on(async {
        let mut transport = TcpTransport::new().await;
        let endpoint = transport.endpoint().to_string();
        let (mut sender, _) = transport.connect(&endpoint).await;
        let (_, mut receiver) = transport.accept().await;

        let serializer = SerdeSerializer;
        sender.send(&serializer.serialize(&42u64)).await;
        sender
            .send(&serializer.serialize(&String::from("between")))
            .await;
        sender
            .send(&serializer.serialize(&Option::<u8>::None))
            .await;

        let a: u64 = serializer.deserialize(&receiver.recv().await.unwrap());
        let b: String = serializer.deserialize(&receiver.recv().await.unwrap());
        let c: Option<u8> = serializer.deserialize(&receiver.recv().await.unwrap());
        assert_eq!((a, b, c), (42, String::from("between"), None));
    });
}

#[test]
fn close_after_last_message_yields_none() {
    Runtime::new().block_on(async {
        let mut transport = TcpTransport::new().await;
        let endpoint = transport.endpoint().to_string();
        let (mut sender, _) = transport.connect(&endpoint).await;
        let (_, mut receiver) = transport.accept().await;

        let serializer = SerdeSerializer;
        sender
            .send(&serializer.serialize(&String::from("last")))
            .await;
        drop(sender);

        let last: String = serializer.deserialize(&receiver.recv().await.unwrap());
        assert_eq!(last, "last");
        assert_eq!(receiver.recv().await, None);
    });
}

#[test]
fn large_typed_payload_with_concurrent_ends() {
    Runtime::new().block_on(async {
        let mut transport = TcpTransport::new().await;
        let endpoint = transport.endpoint().to_string();
        let (mut sender, _) = transport.connect(&endpoint).await;
        let (_, mut receiver) = transport.accept().await;

        let serializer = SerdeSerializer;
        let value: Vec<u32> = (0..1_000_000).collect();
        let bytes = serializer.serialize(&value);

        // Larger than any socket buffer: the two ends must run concurrently
        // on the single-threaded executor or the send would never drain.
        let (_, received) = futures::join!(async { sender.send(&bytes).await }, async {
            receiver.recv().await.unwrap()
        },);
        let received: Vec<u32> = serializer.deserialize(&received);
        assert_eq!(received, value);
    });
}

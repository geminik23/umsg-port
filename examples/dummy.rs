use std::time::Duration;
use umsg_port_rs::{create_port, Message, OpMessage, PortOption, ResultOp};

fn dummy_client() {
    let name = "DummyClient";
    let url = std::env::var("RABBITMQ_URL").unwrap();

    let option = PortOption {
        name: name.into(),
        url,
        hb_dst: "Somewhere".into(),
        hb_interval: Duration::from_millis(1500),
        expiration_ms: 6000,
    };

    let port = create_port(option);

    std::thread::sleep(Duration::from_millis(1500));
    let msg = port.receive_message();
    println!("CLIENT received state message {:?}", msg);

    // request task
    let msg = Message {
        id: Some(format!("{}:01", name)),
        src: name.into(),
        dst: Some("DummyWorker".to_string()),
        data: serde_json::from_str(r#"{"op":"Task", "data":{}}"#).unwrap(),
        ..Default::default()
    };
    port.send_task_message(msg);
    std::thread::sleep(Duration::from_millis(1000));

    loop {
        let msg = port.receive_message();
        if msg.is_none() {
            continue;
        }
        let msg = msg.unwrap();
        match msg {
            OpMessage::Result(msg) => {
                log::info!("client received data {:?}", msg);
            }
            _ => {}
        }
    }
}

fn dummy_worker() {
    let name = "DummyWorker";
    let url = std::env::var("RABBITMQ_URL").unwrap();

    let option = PortOption {
        name: name.into(),
        url,
        hb_dst: "Somewhere".into(),
        hb_interval: Duration::from_millis(1500),
        expiration_ms: 6000,
    };

    let port = create_port(option);

    let msg = Message {
        id: Some("msg:01".into()),
        src: name.into(),
        dst: Some("DummyClient".to_string()),
        data: serde_json::Value::Null,
        ..Default::default()
    };
    std::thread::sleep(Duration::from_millis(1000));
    // update state
    port.send_update_message(msg);
    // sending task
    // port.send_message(OpMessage::Task(
    std::thread::sleep(Duration::from_millis(1500));

    // req
    let msg = port.receive_message();
    let msg = msg.unwrap();
    println!("WORKER received message {:?}", msg);
    if let OpMessage::Task(msg) = msg {
        // send result
        let mut result = Message {
            id: msg.id,
            src: name.to_string(),
            dst: Some(msg.src),
            result: Some(ResultOp::TaskStarted),
            data: serde_json::json!(12.5),
            ..Default::default()
        };

        port.send_result_message(result.clone());
        std::thread::sleep(Duration::from_millis(500));

        result.result = Some(ResultOp::TaskProgress);
        result.data = serde_json::json!(13);
        port.send_result_message(result.clone());
        std::thread::sleep(Duration::from_millis(500));

        result.result = Some(ResultOp::TaskProgress);
        result.data = serde_json::json!(13);
        port.send_result_message(result.clone());
        std::thread::sleep(Duration::from_millis(500));

        result.result = Some(ResultOp::TaskFinished);
        result.data = serde_json::json!(100);
        port.send_result_message(result.clone());
    }

    std::thread::sleep(Duration::from_millis(100000));
}

use std::thread;
fn main() {
    dotenv::dotenv().ok();
    env_logger::init();

    let client = thread::spawn(|| {
        dummy_client();
    });
    let worker = thread::spawn(|| {
        dummy_worker();
    });

    client.join().unwrap();
    worker.join().unwrap();
}

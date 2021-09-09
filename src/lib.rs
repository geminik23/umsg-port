#[macro_use]
extern crate log;
use async_amqp::*;
use async_std::task;
use crossbeam::channel::{unbounded, Receiver, Sender};
use lapin::{
    options::{
        BasicAckOptions, BasicGetOptions, BasicPublishOptions, ExchangeDeclareOptions,
        QueueBindOptions, QueueDeclareOptions,
    },
    protocol::basic::AMQPProperties,
    publisher_confirm::{Confirmation, PublisherConfirm},
    types::{FieldTable, ShortString},
    Channel, Connection, ConnectionProperties,
};
use serde::{Deserialize, Serialize};
use std::thread;
use std::time::{Duration, SystemTime};
// use lapin::

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ResultOp {
    Okay,
    TaskStarted,
    TaskInQueue,
    TaskProgress,
    TaskFinished,
    Error,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Message {
    pub id: Option<String>,
    pub src: String,
    pub dst: Option<String>,
    pub result: Option<ResultOp>,
    pub data: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum OpMessage {
    Update(Message), // update from client to server
    State(Message),  // state from server to client
    Task(Message),   // task request
    Result(Message), // task response
}

struct PortInside {
    name: String,
    url: String,
    receiver: Receiver<OpMessage>,
    sender: Sender<OpMessage>,

    hb_dst: String,
    hb_interval: Duration,

    system_id: String,
    system_count: u64,

    expiration_ms: u32,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
struct MessageArg {
    op: String,
    data: Option<serde_json::Value>,
}

#[derive(thiserror::Error, Debug)]
enum PortError {
    #[error(transparent)]
    LapinError(#[from] lapin::Error),
}

type Result<T> = std::result::Result<T, PortError>;

impl PortInside {
    fn new(option: PortOption, r: Receiver<OpMessage>, s: Sender<OpMessage>) -> Self {
        Self {
            name: option.name,
            url: option.url,
            hb_dst: option.hb_dst,
            hb_interval: option.hb_interval,
            receiver: r,
            sender: s,
            system_id: uuid::Uuid::new_v4().to_string(),
            system_count: 0,
            expiration_ms: option.expiration_ms,
        }
    }

    fn system_message_id(&mut self) -> String {
        let msgid = format!("{}:sys:{}:{}", self.name, self.system_id, self.system_count);
        self.system_count += 1;
        msgid
    }

    async fn _run(&mut self) -> Result<()> {
        info!("connecting rabbitmq server... {}", self.url);
        let conn = Connection::connect(&self.url, ConnectionProperties::default().with_async_std())
            .await?;
        info!("connected");

        let channel = conn.create_channel().await?;
        channel
            .exchange_declare(
                "state",
                lapin::ExchangeKind::Topic,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        let mut qoption = QueueDeclareOptions::default();
        qoption.exclusive = true;
        let result = channel
            .queue_declare("", qoption, FieldTable::default())
            .await?;
        let stateq = result.name().to_string();

        channel
            .queue_bind(
                &stateq,
                "state",
                "All.State",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;
        channel
            .queue_bind(
                &stateq,
                "state",
                &format!("{}.State", self.name),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;

        // # task
        channel
            .queue_declare(
                &format!("{}.Task", self.name),
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;
        channel
            .queue_declare(
                &format!("{}.Reply", self.name),
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        let mut last = SystemTime::now();
        let taskq = format!("{}.Task", self.name);
        let resq = format!("{}.Reply", self.name);

        let send_hb = self.name != "MainService";

        loop {
            let recv = self.receiver.try_recv();
            if let Ok(msg) = recv {
                // sending datas
                match msg {
                    OpMessage::Update(msg) => {
                        let qname = if msg.dst.is_some() {
                            format!("{}.State", msg.dst.clone().unwrap())
                        } else {
                            "All.State".into()
                        };
                        let payload = serde_json::to_string(&msg).unwrap();
                        let _result = self
                            .publish_message(&channel, "state", &qname, payload.into(), None, None)
                            .await?;
                        // info!("sent update message {:?}", _result);
                    }
                    OpMessage::Task(msg) => {
                        if msg.dst.is_some() {
                            let dst = msg.dst.clone().unwrap();
                            let id = msg.id.clone().unwrap();
                            let qname = format!("{}.Task", dst);
                            let payload = serde_json::to_string(&msg).unwrap();
                            let _result = self
                                .publish_message(
                                    &channel,
                                    "",
                                    &qname,
                                    payload.into(),
                                    Some(resq.clone()),
                                    Some(id),
                                )
                                .await?;
                            // info!("sent task message {:?}", _result);
                        }
                    }
                    OpMessage::Result(msg) => {
                        if msg.dst.is_some() {
                            let dst = msg.dst.clone().unwrap();
                            let id = msg.id.clone().unwrap();
                            let qname = format!("{}.Reply", dst);
                            let payload = serde_json::to_string(&msg).unwrap();
                            let _result = self
                                .publish_message(
                                    &channel,
                                    "",
                                    &qname,
                                    payload.into(),
                                    None,
                                    Some(id),
                                )
                                .await?;
                            // info!("sent task result message {:?}", _result);
                        }
                    }
                    _ => {}
                }
            }

            {
                let option = BasicGetOptions::default();
                let msg = channel.basic_get(&stateq, option).await?;
                if let Some(basic_msg) = msg {
                    let _ = basic_msg.delivery.ack(BasicAckOptions::default()).await?;

                    // info!("recevied state message {:?}", basic_msg);
                    let msg = String::from_utf8(basic_msg.data.clone()).unwrap();
                    let msg = serde_json::from_str::<Message>(&msg).unwrap();
                    let mut receive = true;
                    if msg.dst.is_some() {
                        let dst = msg.dst.clone().unwrap();
                        receive = dst == self.name;
                    }
                    if receive {
                        // info!("recevied state message {:?}", msg);
                        self.sender.send(OpMessage::State(msg)).unwrap();
                    }
                }
            }

            {
                let option = BasicGetOptions::default();
                let msg = channel.basic_get(&taskq, option).await?;
                if let Some(basic_msg) = msg {
                    let _ = basic_msg.delivery.ack(BasicAckOptions::default()).await?;
                    let msg = String::from_utf8(basic_msg.data.clone()).unwrap();
                    let msg = serde_json::from_str::<Message>(&msg).unwrap();
                    // info!("recevied task message {:?}", msg);
                    self.sender.send(OpMessage::Task(msg)).unwrap();
                }
            }

            {
                let option = BasicGetOptions::default();
                let msg = channel.basic_get(&resq, option).await?;
                if let Some(basic_msg) = msg {
                    let _ = basic_msg.delivery.ack(BasicAckOptions::default()).await?;
                    let msg = String::from_utf8(basic_msg.data.clone()).unwrap();
                    let msg = serde_json::from_str::<Message>(&msg).unwrap();
                    // info!("recevied result message {:?}", msg);
                    self.sender.send(OpMessage::Result(msg)).unwrap();
                }
            }

            let now = SystemTime::now();
            if send_hb && now.duration_since(last).unwrap() >= self.hb_interval {
                // send HB
                let msgid = self.system_message_id();
                let mut msg = Message::default();
                msg.id = Some(msgid.clone());
                msg.src = self.name.clone();
                msg.dst = Some(self.hb_dst.clone());
                let mut arg = MessageArg::default();
                arg.op = "HeartBeat".into();
                msg.data = serde_json::to_value(arg).unwrap();

                let msg = serde_json::to_string(&msg).unwrap();
                // let result = self.publish_message(&channel).await?;
                let _result = self
                    .publish_message(
                        &channel,
                        "",
                        "MainService.Task",
                        msg.into(),
                        Some(resq.clone()),
                        Some(msgid),
                    )
                    .await?;
                // info!("hb result {:?}", _result);
                last = now;
            }
        }
        // Ok(())
    }

    async fn publish_message(
        &mut self,
        ch: &Channel,
        exchange: &str,
        routing_key: &str,
        payload: Vec<u8>,
        reply_to: Option<String>,
        cor_id: Option<String>,
    ) -> Result<Confirmation> {
        let mut props =
            AMQPProperties::default().with_expiration(self.expiration_ms.to_string().into());
        if reply_to.is_some() {
            props = props.with_reply_to(reply_to.unwrap().into());
        }
        if cor_id.is_some() {
            props = props.with_correlation_id(cor_id.unwrap().into());
        }
        ch.basic_publish(
            exchange,
            routing_key,
            BasicPublishOptions::default(),
            payload,
            props,
        )
        .await?
        .await
        .map_err(|err| PortError::LapinError(err))
    }

    pub fn run(&mut self) {
        loop {
            let result = task::block_on(self._run());
            match result {
                Ok(_) => {
                    // do nothing
                    info!("finished");
                }
                Err(err) => {
                    error!("error {}", err);
                }
            }
        }
    }
}

pub struct Port {
    sender: Sender<OpMessage>,
    receiver: Receiver<OpMessage>,
    _handle: thread::JoinHandle<()>,
}

impl Port {
    pub fn new(
        sender: Sender<OpMessage>,
        receiver: Receiver<OpMessage>,
        handle: thread::JoinHandle<()>,
    ) -> Self {
        Self {
            sender,
            receiver,
            _handle: handle,
        }
    }

    pub fn receive_message(&self) -> Option<OpMessage> {
        self.receiver.try_recv().ok()
    }

    pub fn send_message(&self, msg: OpMessage) {
        self.sender.send(msg).unwrap();
    }

    pub fn send_task_message(&self, msg: Message) {
        self.send_message(OpMessage::Task(msg));
    }

    pub fn send_result_message(&self, msg: Message) {
        self.send_message(OpMessage::Result(msg));
    }

    pub fn send_update_message(&self, msg: Message) {
        self.send_message(OpMessage::Update(msg));
    }
}

pub struct PortOption {
    pub name: String,
    pub url: String,
    pub hb_dst: String,
    pub hb_interval: Duration,
    pub expiration_ms: u32,
}

pub fn create_port(option: PortOption) -> Port {
    let (sclient, rclient) = unbounded::<OpMessage>();
    let (sport, rport) = unbounded::<OpMessage>();
    let handle = thread::spawn(move || {
        // one time?
        let mut port = PortInside::new(option, rclient, sport);
        port.run();
    });

    Port::new(sclient, rport, handle)
}

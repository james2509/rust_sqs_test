use aws_sdk_sqs::Client;
use serde::{Deserialize, Serialize};
use std::env;
use std::error::Error;

#[tokio::main]
async fn main() {
    let queue_url = env::var("SQS_QUEUE_URL").expect("SQS queue url not set");

    let pub_sub = PubSub::new(queue_url).await.unwrap();

    pub_sub
        .publish(Message {
            message: "nice little test".to_string(),
        })
        .await
        .unwrap();

    pub_sub.receive().await.unwrap();
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    message: String,
}

pub struct PubSub {
    client: Client,
    queue_url: String,
}

impl PubSub {
    pub async fn new(queue_url: String) -> Result<PubSub, Box<dyn Error>> {
        let config = aws_config::load_from_env().await;
        let client = Client::new(&config);

        Ok(PubSub { client, queue_url })
    }

    pub async fn publish(&self, message: Message) -> Result<(), Box<dyn Error>> {
        let message_str = serde_json::to_string(&message)?;
        let rsp = self
            .client
            .send_message()
            .queue_url(&self.queue_url)
            .message_body(message_str)
            .send()
            .await?;

        println!("Send message to the queue: {:#?}", rsp);

        Ok(())
    }

    pub async fn receive(&self) -> Result<(), Box<dyn Error>> {
        let rcv = self
            .client
            .receive_message()
            .queue_url(&self.queue_url)
            .send()
            .await?;

        println!("Messages from queue with url: {}", self.queue_url.clone());

        for message in rcv.messages().unwrap_or_default() {
            println!("Got the message: {:#?}", message);
        }

        Ok(())
    }
}

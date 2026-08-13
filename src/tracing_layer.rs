use crate::api::db::insert_db_error;
use sqlx::PgPool;
use std::sync::mpsc::{Sender, channel};
use std::thread;
use tracing::{
    Event,
    field::{Field, Visit},
    subscriber::Subscriber,
};
use tracing_subscriber::{
    layer::{Context as layer_Context, Layer},
    registry::LookupSpan,
};

struct MessageVisitor {
    message: String,
}

impl MessageVisitor {
    fn new() -> Self {
        Self {
            message: String::new(),
        }
    }
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{:?}", value).trim_matches('"').to_string();
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        }
    }
}

pub struct DbErrorLayer {
    sender: Sender<String>,
}

impl DbErrorLayer {
    pub fn new(pool: PgPool) -> Self {
        let (sender, receiver) = channel::<String>();

        thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
            rt.block_on(async move {
                while let Ok(msg) = receiver.recv() {
                    if let Err(e) = insert_db_error(&pool, &msg).await {
                        eprintln!("Failed to save error to DB: {e}");
                    }
                }
                eprintln!("DbErrorLayer: receiver closed, worker thread exiting");
            });
        });

        Self { sender }
    }
}

impl<S> Layer<S> for DbErrorLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: layer_Context<'_, S>) {
        if *event.metadata().level() != tracing::Level::ERROR {
            return;
        }

        let mut visitor = MessageVisitor::new();
        event.record(&mut visitor);

        let msg = if visitor.message.is_empty() {
            event.metadata().name().to_string()
        } else {
            visitor.message
        };

        if let Err(e) = self.sender.send(format!("{:?}", msg)) {
            eprintln!("DbErrorLayer: failed to queue error: {e}");
        }
    }
}

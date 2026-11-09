//! Capture only the bounded bootstrap completion target, never auth/payload logs.
use serde_json::Value;
use std::{
    io,
    sync::{Arc, Mutex},
};

#[derive(Clone, Default)]
pub struct Capture(Arc<Mutex<Vec<u8>>>);
impl io::Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
    type Writer = Self;
    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}
impl Capture {
    pub fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync {
        tracing_subscriber::fmt()
            .json()
            .without_time()
            .with_ansi(false)
            .with_env_filter("bootstrap=info")
            .with_writer(self.clone())
            .finish()
    }
    pub fn take(&self) -> Vec<Value> {
        let bytes = std::mem::take(&mut *self.0.lock().unwrap());
        String::from_utf8(bytes)
            .unwrap()
            .lines()
            .map(|line| {
                let event: Value = serde_json::from_str(line).unwrap();
                assert_eq!(event["target"], "bootstrap");
                let fields = event["fields"].as_object().unwrap();
                assert_eq!(fields.len(), 2, "only fixed message and typed record");
                assert_eq!(fields["message"], "bootstrap completed");
                let record: Value =
                    serde_json::from_str(fields["record"].as_str().unwrap()).unwrap();
                assert_eq!(record["branches"].as_object().unwrap().len(), 7);
                record
            })
            .collect()
    }
}

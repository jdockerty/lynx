use lynx::event::Event;

pub fn arbitrary_event() -> Event {
    Event {
        namespace: "my_namespace".to_string(),
        name: "test_event".to_string(),
        timestamp: 111,
        precision: None,
        value: 1,
        metadata: serde_json::Value::Null,
    }
}

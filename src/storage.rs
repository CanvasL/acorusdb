use std::collections::HashMap;

pub struct Storage {
    data: HashMap<String, String>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            data: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.data.insert(key.into(), value.into());
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn delete(&mut self, key: &str) -> bool {
        self.data.remove(key).is_some()
    }
}

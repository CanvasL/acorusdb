mod wal_prefix {
    pub const SET: &str = "SET";
    pub const DEL: &str = "DEL";
}

pub enum WalEntry {
    Set { key: String, value: String },
    Delete { key: String },
}

impl WalEntry {
    /// Converts a WalEntry into a line that can be written to the WAL file.
    pub fn to_line(&self) -> String {
        match self {
            WalEntry::Set { key, value } => format!("{}\t{}\t{}\n", wal_prefix::SET, key, value),
            WalEntry::Delete { key } => format!("{}\t{}\n", wal_prefix::DEL, key),
        }
    }

    /// Parses a line from the WAL file into a WalEntry. Returns None if the line is malformed.
    pub fn from_line(line: &str) -> Option<Self> {
        let parts: Vec<&str> = line.trim().split('\t').collect();
        if parts.len() < 2 {
            return None;
        }
        match parts[0] {
            wal_prefix::SET if parts.len() == 3 => Some(WalEntry::Set {
                key: parts[1].to_string(),
                value: parts[2].to_string(),
            }),
            wal_prefix::DEL if parts.len() == 2 => Some(WalEntry::Delete {
                key: parts[1].to_string(),
            }),
            _ => None,
        }
    }
}

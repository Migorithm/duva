//TODO move to a separate file
pub enum TtlCommand {
    Expiry { expiry: u64, key: String },
    StopSentinel,
}

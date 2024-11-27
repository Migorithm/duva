pub(crate) const HEADER_MAGIC_STRING: &str = "REDIS";
pub(crate) const METADATA_SECTION_INDICATOR: u8 = 0xFA;
pub(crate) const DATABASE_SECTION_INDICATOR: u8 = 0xFE;
pub(crate) const DATABASE_TABLE_SIZE_INDICATOR: u8 = 0xFB;
pub(crate) const EXPIRY_TIME_IN_MILLISECONDS_INDICATOR: u8 = 0xFC;
pub(crate) const EXPIRY_TIME_IN_SECONDS_INDICATOR: u8 = 0xFD;
pub(crate) const STRING_VALUE_TYPE_INDICATOR: u8 = 0x00;
pub(crate) const CHECKSUM_INDICATOR: u8 = 0xFF;
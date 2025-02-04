use crate::services::statefuls::persist::DecodedMetadata;

#[derive(Default)]
pub struct DecoderInit;

#[derive(Default, PartialEq, Eq, Debug)]
pub struct HeaderReady(pub(crate) String);
#[derive(Default)]
pub struct MetadataReady {
    pub(crate) metadata: DecodedMetadata,
    pub(crate) header: String,
}

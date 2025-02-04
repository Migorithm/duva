use crate::services::statefuls::persist::DumpMetadata;

#[derive(Default)]
pub struct DecoderInit;

#[derive(Default, PartialEq, Eq, Debug)]
pub struct HeaderReady(pub(crate) String);
#[derive(Default)]
pub struct MetadataReady {
    pub(crate) metadata: DumpMetadata,
    pub(crate) header: String,
}

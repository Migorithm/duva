use crate::domains::saves::snapshot::Metadata;

#[derive(Default)]
pub struct DecoderInit;

#[derive(Default, PartialEq, Eq, Debug)]
pub struct HeaderReady(pub(crate) String);

pub struct MetadataReady {
    pub(crate) metadata: Metadata,
    pub(crate) header: String,
}

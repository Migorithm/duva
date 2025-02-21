use crate::domains::saves::snapshot::snapshot::Metadata;

#[derive(Default)]
pub struct DecoderInit;

#[derive(Default, PartialEq, Eq, Debug)]
pub struct HeaderReady(pub(crate) String);
#[derive(Default)]
pub struct MetadataReady {
    pub(crate) metadata: Metadata,
    pub(crate) header: String,
}

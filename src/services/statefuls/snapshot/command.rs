use crate::services::statefuls::snapshot::dump_file::DumpFile;

pub enum SnapshotCommand {
    ReplaceSnapshot(DumpFile)
}
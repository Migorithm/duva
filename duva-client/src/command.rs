use duva::{
    domains::query_io::QueryIO, prelude::tokio::sync::oneshot,
    presentation::clients::request::ClientAction,
};

pub fn separate_command_and_args(args: Vec<&str>) -> (&str, Vec<&str>) {
    // Split the input into command and arguments
    let (cmd, args) = args.split_at(1);
    let cmd = cmd[0];
    let args = args.to_vec();
    (cmd, args)
}

#[derive(Debug)]
pub enum InputKind {
    SingleNodeInput {
        kind: ClientAction,
        callback: oneshot::Sender<(ClientAction, QueryIO)>,
    },
    MultipleNodesInput {
        kind: ClientAction,
        callback: oneshot::Sender<(ClientAction, QueryIO)>,
        results: Vec<QueryIO>,
    },
}

impl InputKind {
    pub fn new(kind: ClientAction, callback: oneshot::Sender<(ClientAction, QueryIO)>) -> Self {
        return match kind {
            | ClientAction::Keys { pattern: _ } => {
                Self::MultipleNodesInput { kind, callback, results: Vec::new() }
            },
            | _ => Self::SingleNodeInput { kind, callback },
        };
    }

    pub fn kind(&self) -> &ClientAction {
        match self {
            | InputKind::SingleNodeInput { kind, .. } => kind,
            | InputKind::MultipleNodesInput { kind, .. } => kind,
        }
    }
}

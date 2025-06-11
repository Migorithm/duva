use rustyline::{
    Context,
    completion::{Completer, Pair},
    error::ReadlineError,
};

use crate::editor::DuvaHinter;

// This function gathers all available commands for completion

pub(crate) static COMMANDS: &[&str] = &[
    "get",
    "set",
    "append",
    "cluster",
    "ping",
    "keys",
    "info",
    "exists",
    "del",
    "incr",
    "incrby",
    "decr",
    "decrby",
    "ttl",
    // subcommands
    "cluster info",
    "cluster nodes",
    "cluster forget",
    "cluster meet",
    "info replication",
    "replicaof",
    "type",
];

macro_rules! new_pair {
    ($display:expr) => {
        Pair { display: $display.to_string(), replacement: $display.to_string() }
    };
    ($display:expr, $replacement: expr) => {
        Pair { display: $display.to_string(), replacement: $replacement.to_string() }
    };
}

impl Completer for DuvaHinter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> Result<(usize, Vec<Self::Candidate>), ReadlineError> {
        // Calculate the start of the current word
        let start = if line[..pos].ends_with(' ') {
            pos
        } else {
            line[..pos].rfind(' ').map_or(0, |i| i + 1)
        };

        // Get the text before the start of the current word
        let before_start = &line[..start];
        // Split into previous words
        let previous_words: Vec<&str> = before_start.split_whitespace().collect();
        // Get the current prefix being typed
        let current_prefix = &line[start..pos];

        let mut candidates = Vec::new();

        if previous_words.is_empty() {
            // Suggest top-level commands that start with current_prefix
            for cmd in self.commands {
                if cmd.starts_with(current_prefix) {
                    candidates
                        .push(Pair { display: cmd.to_string(), replacement: cmd.to_string() });
                }
            }
            return Ok((start, candidates));
        }

        let command = previous_words[0].to_lowercase();
        match command.as_str() {
            | "cluster" => {
                if previous_words.len() == 1 {
                    // Suggest subcommands for cluster that start with current_prefix
                    let subcommands = ["info", "nodes", "forget", "meet"];
                    candidates.extend(
                        subcommands
                            .iter()
                            .filter(|s| s.starts_with(current_prefix))
                            .map(|s| new_pair!(s)),
                    );
                } else if previous_words.len() == 2 {
                    let subcommand = previous_words[1].to_lowercase();
                    if subcommand == "forget" || subcommand == "meet" {
                        // Suggest "node" for cluster forget
                        candidates.push(new_pair!("node"));
                    }
                }
            },
            | "info" => {
                if previous_words.len() == 1 {
                    // Suggest subcommands for info that start with current_prefix
                    let subcommands = ["replication", "section"];
                    candidates.extend(
                        subcommands
                            .iter()
                            .filter(|s| s.starts_with(current_prefix))
                            .map(|s| new_pair!(s)),
                    );
                }
            },
            | "set" => {
                if previous_words.len() == 1 {
                    // Suggest "key" after set
                    candidates.push(new_pair!("key"));
                } else if previous_words.len() == 2 {
                    // Suggest "value" after set key
                    candidates.push(new_pair!("value"));
                } else if previous_words.len() == 3 {
                    // Suggest "px expr" after set key value
                    candidates.push(new_pair!("px expr"));
                }
            },

            | "incrby" | "decrby" => {
                if previous_words.len() == 1 {
                    // Suggest "key" after set
                    candidates.push(new_pair!("key"));
                } else if previous_words.len() == 2 {
                    // Suggest "value" after set key
                    if command == "incrby" {
                        candidates.push(new_pair!("increment"));
                    } else {
                        candidates.push(new_pair!("decrement"));
                    }
                }
            },
            | "exists" | "del" => {
                if !previous_words.is_empty() {
                    // Suggest "key" for these commands
                    candidates.push(new_pair!("key"));
                }
            },
            | "get" | "incr" | "decr" | "ttl" => {
                if previous_words.len() == 1 {
                    // Suggest "index" after get key
                    candidates.push(new_pair!("key"));
                }
            },
            | "keys" => {
                if previous_words.len() == 1 {
                    // Suggest "pattern" after keys
                    candidates.push(new_pair!("pattern"));
                }
            },
            | "replicaof" => {
                if previous_words.len() == 1 {
                    // Suggest "host port" after replicaof
                    candidates.push(new_pair!("host"));
                } else if previous_words.len() == 2 {
                    // Suggest "port" after replicaof host
                    candidates.push(new_pair!("port"));
                }
            },
            | "type" => {
                if previous_words.len() == 1 {
                    // Suggest "key" after type
                    candidates.push(new_pair!("key"));
                }
            },
            | _ => {},
        }

        Ok((start, candidates))
    }
}

use rustyline::{
    Context,
    completion::{Completer, Pair},
    error::ReadlineError,
};

use crate::editor::DuvaHinter;

// This function gathers all available commands for completion

pub(crate) static COMMANDS: &[&str] = &[
    "SAVE",
    "echo",
    "get",
    "mget",
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
    "cluster reshard",
    "info replication",
    "replicaof",
    "lpush",
    "lpushx",
    "rpush",
    "rpushx",
    "llen",
    "lset",
    "lpop",
    "rpop",
    "lrange",
    "ltrim",
    "lindex",
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

        macro_rules! suggest_by_pos {
        (
            [$($args:expr),+]) => {
            let suggestions = [$($args),+];
            if previous_words.len() > 0 && previous_words.len() <= suggestions.len() {
                candidates.push(new_pair!(suggestions[previous_words.len() - 1]));
            }
        };
            ([$($args:expr),+], repeat_last) => {
            let suggestions = [$($args),+];
            if previous_words.len() > 0 {
                let idx = if previous_words.len() <= suggestions.len() {
                    previous_words.len() - 1
                } else {
                    suggestions.len() - 1  // Keep repeating the last argument
                };
                candidates.push(new_pair!(suggestions[idx]));
            }
            };
        }

        let command = previous_words[0].to_lowercase();
        match command.as_str() {
            | "cluster" => {
                if previous_words.len() == 1 {
                    // Suggest subcommands for cluster that start with current_prefix
                    let subcommands = ["info", "nodes", "forget", "meet", "reshard"];
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
            | "echo" => {
                suggest_by_pos!(["value"]);
            },
            | "set" => {
                suggest_by_pos!(["key", "value", "px expr"]);
            },

            | "lset" => {
                suggest_by_pos!(["key", "index", "value"]);
            },

            | "incrby" => {
                suggest_by_pos!(["key", "increment"]);
            },

            | "decrby" => {
                suggest_by_pos!(["key", "decrement"]);
            },
            | "exists" | "del" | "mget" => {
                suggest_by_pos!(["key"], repeat_last);
            },
            | "lpush" | "lpushx" | "rpush" | "rpushx" => {
                suggest_by_pos!(["key", "value"], repeat_last);
            },
            | "lpop" | "rpop" => {
                suggest_by_pos!(["key", "count"]);
            },

            | "get" | "incr" | "decr" | "ttl" | "llen" => {
                suggest_by_pos!(["key"]);
            },
            | "keys" => {
                suggest_by_pos!(["pattern"]);
            },
            | "replicaof" => {
                suggest_by_pos!(["host port"]);
            },
            | "lrange" | "ltrim" => {
                suggest_by_pos!(["key", "start", "end"]);
            },

            | _ => {},
        }

        Ok((start, candidates))
    }
}

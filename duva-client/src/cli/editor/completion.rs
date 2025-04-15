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
    "cluster",
    "ping",
    "keys",
    "info",
    "exists",
    "del",
    "incr",
    // subcommands
    "cluster info",
    "cluster nodes",
    "cluster forget",
    "info replication",
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
        let previous_words: Vec<&str> = before_start.trim().split_whitespace().collect();
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
            "cluster" => {
                if previous_words.len() == 1 {
                    // Suggest subcommands for cluster that start with current_prefix
                    let subcommands = ["info", "nodes", "forget"];
                    candidates.extend(
                        subcommands
                            .iter()
                            .filter(|s| s.starts_with(current_prefix))
                            .map(|s| new_pair!(s)),
                    );
                } else if previous_words.len() == 2 {
                    let subcommand = previous_words[1].to_lowercase();
                    if subcommand == "forget" {
                        // Suggest "node" for cluster forget
                        candidates.push(new_pair!("node"));
                    }
                }
            },
            "info" => {
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
            "set" => {
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
            "get" | "exists" | "del" | "incr" => {
                if previous_words.len() >= 1 {
                    // Suggest "key" for these commands
                    candidates.push(new_pair!("key"));
                }
            },
            "keys" => {
                if previous_words.len() == 1 {
                    // Suggest "pattern" after keys
                    candidates.push(new_pair!("pattern"));
                }
            },
            _ => {},
        }

        Ok((start, candidates))
    }
}

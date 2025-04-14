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
    // subcommands
    "cluster info",
    "cluster nodes",
    "cluster forget",
    "info replication",
];

fn new_pair(word: &str) -> Pair {
    Pair { display: word.to_string(), replacement: word.to_string() }
}

// Implement Completer for DuvaHinter manually since we need custom logic
impl Completer for DuvaHinter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> Result<(usize, Vec<Self::Candidate>), ReadlineError> {
        // Split the line by whitespace to get the command and its arguments
        let parts: Vec<&str> = line[..pos].split_whitespace().collect();

        // Start of the completion range
        let start = if line[..pos].ends_with(' ') {
            pos
        } else {
            let mut start = 0;
            for (i, c) in line[..pos].char_indices().rev() {
                if c.is_whitespace() {
                    start = i + 1;
                    break;
                }
            }
            start
        };

        let mut candidates = Vec::new();

        // Complete commands only if we're at the first word or starting a new word after completed command
        if parts.is_empty() || (parts.len() == 1 && !line[..pos].ends_with(' ')) {
            let word = parts.get(0).map_or("", |s| *s);

            // Suggest commands that start with the current word
            for cmd in self.commands {
                if cmd.starts_with(word) && !cmd.contains(' ') {
                    candidates.push(Pair {
                        display: cmd.to_string(),
                        replacement: word.to_string() + cmd[word.len()..].to_string().as_str(),
                    });
                }
            }
        }
        // Complete subcommands
        else if parts.len() == 1 && line[..pos].ends_with(' ') {
            let command = parts[0].to_lowercase();

            match command.as_str() {
                "cluster" => {
                    candidates.push(new_pair("info"));
                    candidates.push(new_pair("nodes"));
                    candidates.push(new_pair("forget"));
                },
                "info" => {
                    candidates.push(new_pair("replication"));
                    candidates.push(new_pair("section"));
                },
                "set" => {
                    candidates.push(new_pair("key"));
                },
                "get" => {
                    candidates.push(new_pair("key"));
                },
                "exists" | "del" => {
                    candidates.push(new_pair("key"));
                },
                "keys" => {
                    candidates.push(new_pair("pattern"));
                },
                _ => {},
            }
        }
        // Handle special completion for multi-word commands
        else if parts.len() == 2 && !line[..pos].ends_with(' ') {
            let command = parts[0].to_lowercase();
            let subcommand_prefix = parts[1];

            let mut closure = |subcommands: &[&str]| {
                for subcommand in subcommands {
                    if subcommand.starts_with(subcommand_prefix) {
                        candidates.push(Pair {
                            display: subcommand.to_string(),
                            replacement: subcommand.to_string()
                                + subcommand[subcommand.len()..].to_string().as_str(),
                        });
                    }
                }
            };

            match command.as_str() {
                "cluster" => {
                    let subcommands = ["info", "nodes", "forget"];
                    closure(&subcommands);
                },
                "info" => {
                    let subcommands = ["replication"];
                    closure(&subcommands);
                },
                _ => {},
            }
        }
        // Context-specific argument completion
        else if parts.len() >= 2 && line[..pos].ends_with(' ') {
            let command = parts[0].to_lowercase();
            let subcommand = parts.get(1).map(|s| s.to_lowercase());

            // Handle completing node argument for "cluster forget"
            if command == "cluster" && subcommand == Some("forget".to_string()) {
                candidates
                    .push(Pair { display: "node".to_string(), replacement: "node".to_string() });
            }
            // Handle "set" command argument completion
            else if command == "set" {
                if parts.len() == 2 {
                    candidates.push(new_pair("value"));
                } else if parts.len() == 3 {
                    candidates.push(new_pair("px expr"));
                }
            }
            // Multiple key arguments
            else if command == "exists" || command == "del" {
                candidates.push(new_pair("key"));
            }
        }

        Ok((start, candidates))
    }
}

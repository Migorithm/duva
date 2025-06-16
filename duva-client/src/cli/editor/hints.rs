use rustyline::highlight::Highlighter;
use rustyline::{
    Context,
    hint::{Hint, Hinter},
};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use crate::editor::DuvaHinter;

#[derive(Hash, Debug, PartialEq, Eq)]
pub struct CommandHint {
    display: String,
    complete_up_to: usize,
}

impl CommandHint {
    fn new(text: &str, complete_up_to: &str) -> Self {
        assert!(text.starts_with(complete_up_to));
        Self { display: text.into(), complete_up_to: complete_up_to.len() }
    }

    fn suffix(&self, strip_chars: usize) -> Self {
        Self {
            display: self.display[strip_chars..].to_owned(),
            complete_up_to: self.complete_up_to.saturating_sub(strip_chars),
        }
    }
}

impl Hint for CommandHint {
    fn display(&self) -> &str {
        &self.display
    }

    fn completion(&self) -> Option<&str> {
        if self.complete_up_to > 0 { Some(&self.display[..self.complete_up_to]) } else { None }
    }
}

impl Hinter for DuvaHinter {
    type Hint = CommandHint;

    fn hint(&self, line: &str, pos: usize, _ctx: &Context<'_>) -> Option<CommandHint> {
        if line.is_empty() || pos < line.len() {
            return None;
        }

        // Split the line by whitespace to get the command and its arguments
        let parts: Vec<&str> = line.split_whitespace().collect();

        if parts.is_empty() {
            return None;
        }

        let command = parts[0];
        let args_count = parts.len() - 1;
        let ends_with_space = line.ends_with(" ");

        if let Some(patterns) = self.dynamic_hints.get(command.to_lowercase().as_str()) {
            for hint in patterns {
                // Skip if we have more args than required (unless it's a repeating pattern)
                if args_count > hint.args_required && !hint.repeat_last_arg {
                    continue;
                }

                // For repeating patterns, show the hint after reaching minimum args
                if hint.repeat_last_arg && args_count >= hint.args_required {
                    let hint_text = if hint.args_required == 0 && args_count == 0 {
                        hint.hint_text
                    } else {
                        // After first argument, show just the repeating part
                        hint.hint_text
                            .split_once(' ')
                            .map(|(_, rest)| rest)
                            .unwrap_or(hint.hint_text)
                    };

                    return Some(CommandHint::new(
                        if ends_with_space {
                            hint_text.to_string()
                        } else {
                            format!(" {}", hint_text)
                        }
                        .as_str(),
                        "",
                    ));
                }

                // Original behavior for non-repeating patterns
                if args_count == hint.args_required {
                    return Some(CommandHint::new(
                        if ends_with_space {
                            hint.hint_text.to_string()
                        } else {
                            format!(" {}", hint.hint_text)
                        }
                        .as_str(),
                        "",
                    ));
                }
            }
        }

        // Default behavior - try to match against full hints
        let mut matching_hints =
            self.default_hints
                .iter()
                .filter_map(|hint| {
                    if hint.display.starts_with(line) { Some(hint.suffix(pos)) } else { None }
                })
                .collect::<Vec<_>>();

        matching_hints.sort_by(|a, b| a.display.len().cmp(&b.display.len()));
        matching_hints.into_iter().next()
    }
}

pub(crate) fn default_hints() -> HashSet<CommandHint> {
    let mut set = HashSet::new();
    set.insert(CommandHint::new("get key", "get "));
    set.insert(CommandHint::new("set key value", "set "));
    set.insert(CommandHint::new("set key value [px expr]", "set "));
    set.insert(CommandHint::new("append key value", "append "));
    set.insert(CommandHint::new("incr key", "incr "));
    set.insert(CommandHint::new("incrby key value", "incrby "));
    set.insert(CommandHint::new("decr key", "decr "));
    set.insert(CommandHint::new("decrby key value", "decrby "));
    set.insert(CommandHint::new("cluster info", "cluster "));
    set.insert(CommandHint::new("cluster nodes", "cluster "));
    set.insert(CommandHint::new("cluster forget node", "cluster "));

    set.insert(CommandHint::new("cluster meet node [lazy|eager]", "cluster "));
    set.insert(CommandHint::new("ping", ""));
    set.insert(CommandHint::new("keys pattern", "keys "));
    set.insert(CommandHint::new("info [section]", ""));
    set.insert(CommandHint::new("info replication", ""));
    set.insert(CommandHint::new("exists key [key ...]", "exists "));
    set.insert(CommandHint::new("mget key [key ...]", "mget "));
    set.insert(CommandHint::new("del key [key ...]", "del "));
    set.insert(CommandHint::new("ttl key", "ttl "));
    set.insert(CommandHint::new("replicaof host port", "replicaof "));

    set
}

pub(crate) struct DynamicHint {
    hint_text: &'static str,
    args_required: usize,
    repeat_last_arg: bool, // New flag to indicate repeating pattern
}

macro_rules! hint {
    ($text:expr, $args:expr) => {
        DynamicHint { hint_text: $text, args_required: $args, repeat_last_arg: false }
    };
    ($text:expr, $args:expr, repeat) => {
        DynamicHint { hint_text: $text, args_required: $args, repeat_last_arg: true }
    };
}

pub(crate) fn dynamic_hints() -> HashMap<&'static str, Vec<DynamicHint>> {
    let mut map = HashMap::new();

    // Command pattern definitions
    map.insert(
        "set",
        vec![hint!("key value", 0), hint!("value", 1), hint!("[px expr]", 2), hint!("expr", 3)],
    );
    map.insert("append", vec![hint!("key value", 0), hint!("value", 1)]);
    map.insert("incrby", vec![hint!("key increment", 0), hint!("increment", 1)]);
    map.insert("decrby", vec![hint!("key decrement", 0), hint!("decrement", 1)]);

    map.insert("cluster forget", vec![hint!("node", 0)]);
    map.insert("cluster meet", vec![hint!("node [lazy|eager]", 0), hint!("[lazy|eager]", 1)]);
    map.insert("keys", vec![hint!("pattern", 0)]);
    map.insert("get", vec![hint!("key", 0)]);
    map.insert("exists", vec![hint!("key [key ...]", 0, repeat), hint!("[key ...]", 1, repeat)]);
    map.insert("del", vec![hint!("key [key ...]", 0, repeat), hint!("[key ...]", 1, repeat)]);
    map.insert("mget", vec![hint!("key [key ...]", 0, repeat), hint!("[key ...]", 1, repeat)]);
    map.insert("replicaof", vec![hint!("host port", 0), hint!("port", 1)]);

    map
}

impl Highlighter for DuvaHinter {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> Cow<'b, str> {
        if default {
            Cow::Owned(format!("\x1b[1;32m{prompt}\x1b[m"))
        } else {
            Cow::Borrowed(prompt)
        }
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Cow::Owned(format!("\x1b[38;5;245m{hint}\x1b[m"))
    }
}

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
    fn new(text: &str) -> Self {
        let first = text.split_whitespace().next().unwrap_or("");
        Self { display: text.into(), complete_up_to: first.len() + 1 }
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

        let needs_space = !line.ends_with(" ");

        if let Some(hint) = self.get_dynamic_hint(command, args_count, needs_space) {
            return Some(hint);
        }

        // Fall back to default hints
        self.get_default_hint(line, pos)
    }
}

impl DuvaHinter {
    fn get_dynamic_hint(
        &self,
        command: &str,
        args_count: usize,
        needs_space: bool,
    ) -> Option<CommandHint> {
        let patterns = self.dynamic_hints.get(command.to_lowercase().as_str())?;

        for hint in patterns {
            let hint_text = if hint.repeat_last_arg && args_count >= hint.args_required {
                // Show repeating pattern
                self.get_repeating_hint_text(hint, args_count)
            } else if args_count == hint.args_required && !hint.repeat_last_arg {
                // Show exact match
                hint.hint_text
            } else {
                continue;
            };

            let hint_text = if needs_space { &format!(" {hint_text}") } else { hint_text };
            return Some(CommandHint::new(hint_text));
        }

        None
    }

    fn get_repeating_hint_text(&self, hint: &DynamicHint, args_count: usize) -> &str {
        if hint.args_required == 0 && args_count == 0 {
            hint.hint_text
        } else {
            hint.hint_text.split_once(' ').map(|(_, rest)| rest).unwrap_or(hint.hint_text)
        }
    }

    fn get_default_hint(&self, line: &str, pos: usize) -> Option<CommandHint> {
        self.default_hints
            .iter()
            .filter_map(
                |hint| {
                    if hint.display.starts_with(line) { Some(hint.suffix(pos)) } else { None }
                },
            )
            .min_by_key(|hint| hint.display.len())
    }
}

pub(crate) fn default_hints() -> HashSet<CommandHint> {
    let mut set = HashSet::new();
    set.insert(CommandHint::new("get key"));
    set.insert(CommandHint::new("set key value"));
    set.insert(CommandHint::new("set key value [px expr]"));
    set.insert(CommandHint::new("append key value"));
    set.insert(CommandHint::new("incr key"));
    set.insert(CommandHint::new("incrby key value"));
    set.insert(CommandHint::new("decr key"));
    set.insert(CommandHint::new("decrby key value"));
    set.insert(CommandHint::new("cluster info"));
    set.insert(CommandHint::new("cluster nodes"));
    set.insert(CommandHint::new("cluster forget node"));
    set.insert(CommandHint::new("cluster reshard"));
    set.insert(CommandHint::new("cluster meet node [lazy|eager]"));
    set.insert(CommandHint::new("ping"));
    set.insert(CommandHint::new("keys pattern"));
    set.insert(CommandHint::new("info [section]"));
    set.insert(CommandHint::new("info replication"));
    set.insert(CommandHint::new("exists key [key ...]"));
    set.insert(CommandHint::new("mget key [key ...]"));
    set.insert(CommandHint::new("del key [key ...]"));
    set.insert(CommandHint::new("ttl key"));
    set.insert(CommandHint::new("replicaof host port"));
    set.insert(CommandHint::new("lpush key value [value ...]"));
    set.insert(CommandHint::new("lpushx key value [value ...]"));
    set.insert(CommandHint::new("rpush key value [value ...]"));
    set.insert(CommandHint::new("rpushx key value [value ...]"));

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
    map.insert(
        "lpush",
        vec![hint!("key value [value ...]", 0, repeat), hint!("[value ...]", 1, repeat)],
    );
    map.insert(
        "lpushx",
        vec![hint!("key value [value ...]", 0, repeat), hint!("[value ...]", 1, repeat)],
    );
    map.insert(
        "rpush",
        vec![hint!("key value [value ...]", 0, repeat), hint!("[value ...]", 1, repeat)],
    );

    map.insert(
        "rpushx",
        vec![hint!("key value [value ...]", 0, repeat), hint!("[value ...]", 1, repeat)],
    );
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

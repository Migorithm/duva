use rustyline::highlight::Highlighter;
use rustyline::{
    Completer, Config, Context, Editor, Helper, Validator,
    hint::{Hint, Hinter},
    sqlite_history::SQLiteHistory,
};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
pub fn create() -> Editor<DuvaHinter, SQLiteHistory> {
    let editor_conf = Config::builder().auto_add_history(true).build();
    let history =
        rustyline::sqlite_history::SQLiteHistory::open(editor_conf, "duva-cli.hist").unwrap();
    let mut editor = Editor::with_history(editor_conf, history).unwrap();
    editor.set_helper(Some(DuvaHinter {
        default_hints: default_hints(),
        dynamic_hints: dynamic_hints(),
    }));

    editor
}

#[derive(Completer, Helper, Validator)]
pub struct DuvaHinter {
    // It's simple example of rustyline, for more efficient, please use ** radix trie **
    default_hints: HashSet<CommandHint>,
    dynamic_hints: HashMap<&'static str, Vec<(&'static str, usize)>>,
}

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

        // Look up the command in our patterns
        if let Some(patterns) = self.dynamic_hints.get(command.to_lowercase().as_str()) {
            for (hint_text, required_args) in patterns {
                // This prevents shwowing hints when we have more args than required
                if args_count != *required_args {
                    continue;
                }

                if ends_with_space {
                    if *required_args == 0 {
                        // For commands with no args yet provided
                        return Some(CommandHint::new(hint_text, ""));
                    }
                    // When we have required args and line ends with space
                    return Some(CommandHint::new(hint_text, ""));
                }

                // hint should be shown after a space
                return Some(CommandHint::new(format!(" {}", hint_text).as_str(), ""));
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

fn default_hints() -> HashSet<CommandHint> {
    let mut set = HashSet::new();
    set.insert(CommandHint::new("get key", "get "));
    set.insert(CommandHint::new("set key value", "set "));
    set.insert(CommandHint::new("set key value [px expr]", "set "));
    set.insert(CommandHint::new("cluster info", "cluster "));
    set.insert(CommandHint::new("cluster nodes", "cluster "));
    set.insert(CommandHint::new("cluster forget node", "cluster "));
    set.insert(CommandHint::new("ping", ""));
    set.insert(CommandHint::new("keys pattern", "keys "));
    set.insert(CommandHint::new("info [section]", ""));
    set.insert(CommandHint::new("info replication", ""));

    set
}

fn dynamic_hints() -> HashMap<&'static str, Vec<(&'static str, usize)>> {
    // Command pattern definitions - mapping commands to their expected arguments
    [
        // (command, [(hint_text, args_required), ...])
        ("set", vec![("key value", 0), ("value", 1), ("[px expr]", 2), ("expr", 3)]),
        ("cluster forget", vec![("node", 0)]),
        ("keys", vec![("pattern", 0)]),
        ("get", vec![("key", 0)]),
        // Add more commands here as needed
    ]
    .iter()
    .cloned()
    .collect()
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
        Cow::Owned(format!("\x1b[1;30m{hint}\x1b[m"))
    }
}

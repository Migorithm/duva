use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use rustyline::highlight::Highlighter;
use rustyline::{
    Completer, Config, Context, Editor, Helper, Validator,
    hint::{Hint, Hinter},
    sqlite_history::SQLiteHistory,
};
pub fn create() -> Editor<DIYHinter, SQLiteHistory> {
    let editor_conf = Config::builder().auto_add_history(true).build();
    let history =
        rustyline::sqlite_history::SQLiteHistory::open(editor_conf, "duva-cli.hist").unwrap();
    let mut editor = Editor::with_history(editor_conf, history).unwrap();
    editor.set_helper(Some(DIYHinter { hints: diy_hints(), command_patterns: command_patterns() }));
    editor
}

#[derive(Completer, Helper, Validator)]
pub struct DIYHinter {
    // It's simple example of rustyline, for more efficient, please use ** radix trie **
    hints: HashSet<CommandHint>,
    command_patterns: HashMap<&'static str, Vec<(&'static str, usize)>>,
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

impl Hinter for DIYHinter {
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
        if let Some(patterns) = self.command_patterns.get(command.to_lowercase().as_str()) {
            for (hint_text, required_args) in patterns {
                if args_count == *required_args {
                    if *required_args == 0 && ends_with_space {
                        // For commands with no args yet provided
                        return Some(CommandHint::new(hint_text, ""));
                    } else if *required_args > 0 {
                        if ends_with_space {
                            // When we have required args and line ends with space
                            return Some(CommandHint::new(hint_text, ""));
                        } else if args_count == *required_args && *required_args == 1 {
                            // Special case for "set key" without trailing space
                            return Some(CommandHint::new(format!(" {}", hint_text).as_str(), ""));
                        }
                    }
                }
            }
        }

        // Default behavior - try to match against full hints
        let mut matching_hints =
            self.hints
                .iter()
                .filter_map(|hint| {
                    if hint.display.starts_with(line) { Some(hint.suffix(pos)) } else { None }
                })
                .collect::<Vec<_>>();

        matching_hints.sort_by(|a, b| a.display.len().cmp(&b.display.len()));
        matching_hints.into_iter().next()
    }
}
fn diy_hints() -> HashSet<CommandHint> {
    let mut set = HashSet::new();
    set.insert(CommandHint::new("get key", "get "));
    set.insert(CommandHint::new("set key value", "set "));
    set.insert(CommandHint::new("set key value px expr", "set "));

    set
}

fn command_patterns() -> HashMap<&'static str, Vec<(&'static str, usize)>> {
    // Command pattern definitions - mapping commands to their expected arguments
    let command_patterns: HashMap<&str, Vec<(&str, usize)>> = [
        // (command, [(hint_text, args_required), ...])
        ("set", vec![("key value", 0), ("value", 1), ("px expr", 2)]),
        ("get", vec![("key", 0)]),
        // Add more commands here as needed
    ]
    .iter()
    .cloned()
    .collect();
    command_patterns
}

impl Highlighter for DIYHinter {
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

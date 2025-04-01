use std::collections::HashSet;

use rustyline::{
    Completer, Config, Context, Editor, Helper, Highlighter, Validator,
    hint::{Hint, Hinter},
    sqlite_history::SQLiteHistory,
};

pub fn create() -> Editor<DIYHinter, SQLiteHistory> {
    let editor_conf = Config::builder().auto_add_history(true).build();
    let history =
        rustyline::sqlite_history::SQLiteHistory::open(editor_conf, "duva-cli.hist").unwrap();
    let mut editor = Editor::with_history(editor_conf, history).unwrap();
    editor.set_helper(Some(DIYHinter { hints: diy_hints() }));
    editor
}

#[derive(Completer, Helper, Validator, Highlighter)]
pub struct DIYHinter {
    // It's simple example of rustyline, for more efficient, please use ** radix trie **
    hints: HashSet<CommandHint>,
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

        let mut matching_hints = self
            .hints
            .iter()
            .filter_map(|hint| {
                // expect hint after word complete, like redis cli, add condition:
                // line.ends_with(" ")
                if hint.display.starts_with(line) { Some(hint.suffix(pos)) } else { None }
            })
            .collect::<Vec<_>>();

        // Sort by length of display string to prioritize shorter completions
        matching_hints.sort_by(|a, b| a.display.len().cmp(&b.display.len()));

        // Return the first matching hint (shortest one)
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

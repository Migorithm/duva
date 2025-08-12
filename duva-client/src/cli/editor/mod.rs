mod completion;
mod hints;
use completion::COMMANDS;
use hints::{CommandHint, DynamicHint, default_hints, dynamic_hints};
use rustyline::{Config, Editor, Helper, Validator, sqlite_history::SQLiteHistory};

use std::collections::{HashMap, HashSet};

pub fn create() -> Editor<DuvaHinter, SQLiteHistory> {
    let editor_conf = Config::builder()
        .auto_add_history(true)
        .completion_type(rustyline::CompletionType::Circular)
        .build();
    let history =
        rustyline::sqlite_history::SQLiteHistory::open(&editor_conf, "duva-cli.hist").unwrap();
    let mut editor = Editor::with_history(editor_conf, history).unwrap();
    editor.set_helper(Some(DuvaHinter {
        default_hints: default_hints(),
        dynamic_hints: dynamic_hints(),
        commands: COMMANDS,
    }));

    editor
}

#[derive(Helper, Validator)]
pub struct DuvaHinter {
    // It's simple example of rustyline, for more efficient, please use ** radix trie **
    pub(crate) default_hints: HashSet<CommandHint>,
    pub(crate) dynamic_hints: HashMap<&'static str, Vec<DynamicHint>>,
    pub(crate) commands: &'static [&'static str],
}

// EXPECT: E0004 — the active `Event` vocabulary is closed and exhaustively
// matched. Dropping one within-vocabulary arm (here every oversize/COPY/notify
// variant) WITHOUT a wildcard `_` is a non-exhaustive match: a wire-legal frame
// would have no handling path. The dispatch must classify every variant, never
// silently swallow one.
use bsql_postgres_proto::engine::Event;

fn classify(ev: Event<'_>) -> u8 {
    match ev {
        Event::NeedMore => 0,
        Event::Idle => 1,
        Event::Deliver => 2,
        Event::Fail(_) => 3,
        Event::Close => 4,
        Event::Notice(_) => 5,
        Event::ParamStatus(_) => 6,
        Event::Row(_) => 7,
        // Notify / RowChunk / RowChunkEnd / CopyData / CopyDone omitted, no
        // wildcard — E0004 non-exhaustive.
    }
}

fn main() {
    let _ = classify(Event::NeedMore);
}

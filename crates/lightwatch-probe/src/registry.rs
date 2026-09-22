//! Interning. A symbol's id is its index in this process's registry, plus one,
//! so zero stays free as the "not yet interned" sentinel inside a `static`.
//!
//! Names go on the wire once, the first time their id appears. Everything the
//! emit thread needs to build those `Register` entries lives here.

use std::sync::{Mutex, OnceLock};

use lightwatch_proto::{FunctionId, Location, PathId, Register, TypeId};

use crate::calls::Site;
use crate::census::TypeSlot;
use crate::hashing::IntMap;

/// How many calling contexts one process may name before the tree stops
/// growing. A context costs a map entry here and a row in every window the
/// daemon retains, and both are paid for by a program's runtime behaviour
/// rather than by its source, so this is the one interning table that needs
/// a ceiling. Past it, a new context reports as its parent: the graph grows
/// shallower rather than unbounded, and `path_overflow` says it happened.
const DEFAULT_MAX_PATHS: usize = 65_536;

/// How many `Register` entries one frame may carry. A reconnecting emitter
/// re-sends every name it ever interned, and without a ceiling a process with
/// a large tree writes one enormous line into a socket that gives it 250ms.
/// The high-water mark simply walks forward over the following frames.
const MAX_REGISTERS_PER_FRAME: usize = 2_048;

static FUNCTIONS: Mutex<Vec<&'static Site>> = Mutex::new(Vec::new());
static TYPES: Mutex<Vec<&'static TypeSlot>> = Mutex::new(Vec::new());
static PATHS: Mutex<PathTable> = Mutex::new(PathTable::new());

/// This process's call tree. `nodes[i]` is the context with id `i + 1`, so a
/// node is always numbered above the parent it hangs off: you cannot name a
/// context without already holding the one it extends.
struct PathTable {
    nodes: Vec<(u32, u32)>,
    index: Option<IntMap<(u32, u32), u32>>,
    overflowed: u64,
}

impl PathTable {
    const fn new() -> Self {
        // `IntMap` cannot be built in a const, and a process that never calls
        // a measured function should not pay for one.
        PathTable { nodes: Vec::new(), index: None, overflowed: 0 }
    }
}

/// Read once. Interning is already the cold half of the hot path, but it has
/// no business consulting the environment every time a program reaches a
/// corner of itself it has not reached before.
fn max_paths() -> usize {
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("LIGHTWATCH_MAX_PATHS")
            .ok()
            .and_then(|raw| raw.parse().ok())
            .filter(|cap| *cap > 0)
            .unwrap_or(DEFAULT_MAX_PATHS)
    })
}

/// A panicking measured function must not leave the probe unusable, so every
/// lock here steps over poisoning rather than propagating it.
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(crate) fn intern_function(site: &'static Site) -> FunctionId {
    let mut functions = lock(&FUNCTIONS);
    // Another thread may have interned this site while we waited for the lock.
    if let Some(existing) = site.interned_id() {
        return existing;
    }
    functions.push(site);
    let id = FunctionId(functions.len() as u32);
    site.publish_id(id);
    id
}

pub(crate) fn intern_type(slot: &'static TypeSlot) -> TypeId {
    let mut types = lock(&TYPES);
    if let Some(existing) = slot.interned_id() {
        return existing;
    }
    types.push(slot);
    let id = TypeId(types.len() as u32);
    slot.publish_id(id);
    id
}

/// Interns the context reached by calling `func` from `parent`, or returns an
/// id already handed out for that pair.
///
/// Returns `0` once the table is full, which callers read as "report this
/// activation against its parent instead".
pub(crate) fn intern_path(parent: u32, func: u32) -> u32 {
    let mut table = lock(&PATHS);
    let table = &mut *table;
    let index = table.index.get_or_insert_with(IntMap::default);
    if let Some(existing) = index.get(&(parent, func)) {
        return *existing;
    }
    if table.nodes.len() >= max_paths() {
        table.overflowed += 1;
        return 0;
    }
    table.nodes.push((parent, func));
    let id = table.nodes.len() as u32;
    index.insert((parent, func), id);
    id
}

/// Every context interned so far, as `(id, parent, func)`.
pub(crate) fn path_nodes() -> Vec<(u32, u32, u32)> {
    lock(&PATHS)
        .nodes
        .iter()
        .enumerate()
        .map(|(index, (parent, func))| (index as u32 + 1, *parent, *func))
        .collect()
}

/// How many contexts were refused because the table was full.
pub(crate) fn paths_overflowed() -> u64 {
    lock(&PATHS).overflowed
}

/// The `Register` entries for symbols interned since `already_sent` of each
/// kind, and the new counts to carry into the next window.
///
/// Functions and types go first, and contexts in id order after them, so a
/// frame never names a context before the function it stands for or the
/// parent it extends.
pub(crate) fn registers_since(
    functions_sent: usize,
    types_sent: usize,
    paths_sent: usize,
) -> (Vec<Register>, usize, usize, usize) {
    let functions = lock(&FUNCTIONS);
    let types = lock(&TYPES);
    let mut out = Vec::new();

    for (index, site) in functions.iter().enumerate().skip(functions_sent) {
        out.push(Register::Function {
            id: FunctionId(index as u32 + 1),
            name: site.name.to_string(),
            module: Some(site.module.to_string()),
            location: Some(Location {
                file: site.file.to_string(),
                line: site.line,
                column: None,
            }),
        });
    }
    for (index, slot) in types.iter().enumerate().skip(types_sent) {
        out.push(Register::Type {
            id: TypeId(index as u32 + 1),
            name: slot.name().to_string(),
            location: Some(Location {
                file: slot.file().to_string(),
                line: slot.line(),
                column: None,
            }),
        });
    }
    let paths = lock(&PATHS);
    let mut paths_now = paths_sent;
    for (offset, (parent, func)) in paths.nodes.iter().enumerate().skip(paths_sent) {
        if out.len() >= MAX_REGISTERS_PER_FRAME {
            break;
        }
        out.push(Register::Path {
            id: PathId(offset as u32 + 1),
            parent: PathId(*parent),
            func: FunctionId(*func),
        });
        paths_now = offset + 1;
    }
    (out, functions.len(), types.len(), paths_now)
}

pub(crate) fn tracked_types() -> Vec<(TypeId, &'static TypeSlot)> {
    lock(&TYPES)
        .iter()
        .enumerate()
        .map(|(index, slot)| (TypeId(index as u32 + 1), *slot))
        .collect()
}

/// Finds an interned function by its registered name, for tests that want to
/// talk about `"decode"` rather than about an id the process chose.
pub(crate) fn function_id_by_name(name: &str) -> Option<FunctionId> {
    lock(&FUNCTIONS)
        .iter()
        .position(|site| site.name == name || site.qualified() == name)
        .map(|index| FunctionId(index as u32 + 1))
}

pub(crate) fn type_slot_by_name(name: &str) -> Option<&'static TypeSlot> {
    lock(&TYPES).iter().find(|slot| slot.name() == name).copied()
}

pub(crate) fn function_name(id: u32) -> Option<&'static str> {
    lock(&FUNCTIONS).get(id.checked_sub(1)? as usize).map(|site| site.name)
}

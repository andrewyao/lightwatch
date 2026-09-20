//! Interning. A symbol's id is its index in this process's registry, plus one,
//! so zero stays free as the "not yet interned" sentinel inside a `static`.
//!
//! Names go on the wire once, the first time their id appears. Everything the
//! emit thread needs to build those `Register` entries lives here.

use std::sync::Mutex;

use lightwatch_proto::{FunctionId, Location, Register, TypeId};

use crate::calls::Site;
use crate::census::TypeSlot;

static FUNCTIONS: Mutex<Vec<&'static Site>> = Mutex::new(Vec::new());
static TYPES: Mutex<Vec<&'static TypeSlot>> = Mutex::new(Vec::new());

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

/// The `Register` entries for symbols interned since `already_sent` of each
/// kind, and the new counts to carry into the next window.
pub(crate) fn registers_since(
    functions_sent: usize,
    types_sent: usize,
) -> (Vec<Register>, usize, usize) {
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
    (out, functions.len(), types.len())
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

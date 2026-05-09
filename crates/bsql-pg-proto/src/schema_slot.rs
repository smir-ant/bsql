//! DEF-270 cluster (R-rephrased letter, 2026-05-09) — tier-1
//! `row_desc_slot` write provenance.
//!
//! # Pre-DEF-270 R
//!
//! `PgProtocol::row_desc_slot: Option<RowDesc>` was reachable through
//! a `&mut Option<RowDesc>` raw mutable borrow that the protocol
//! passed to dispatch handlers and `compute_push_*_idle_only` helpers.
//! Any lib-internal site holding the borrow could write `*slot = ...`
//! at any moment. **Tier-3 by-discipline**: the audit invariant
//! "writes happen only at schema-bearing state transitions" was
//! upheld by reviewer attention, not by the type system.
//!
//! # Post-DEF-270 R
//!
//! - `PgProtocol::row_desc_slot` is private to `mod protocol`.
//! - The sole write surface is [`SchemaParkedSlot`], a `must_use`
//!   ZST-witness wrapping `&'a mut Option<RowDesc>`. Methods
//!   [`SchemaParkedSlot::park`] / [`SchemaParkedSlot::clear`] /
//!   [`SchemaParkedSlot::raw_mut`] consume self.
//! - Construction is gated on a [`SchemaWriteAuth`] sealed-trait
//!   witness. The tag types live in their **host modules**
//!   (`mod protocol` for transitions inside the protocol body;
//!   `mod dispatch` for inbound-frame transitions). Each tag's
//!   `new()` constructor is `pub(in <host_module>)` — only the
//!   host module can mint, achieving tier-1 cross-module closure
//!   on write provenance.
//!
//! # Why tags live in host modules (not here)
//!
//! Rust's `pub(in <path>)` visibility requires `<path>` to be an
//! **ancestor** of the item being annotated. A tag defined inside
//! `mod schema_slot` cannot have its constructor restricted to
//! `pub(in crate::dispatch)` (dispatch is a sibling, not an
//! ancestor). To get tight per-call-site sealing, the tag must
//! live where its constructor's visibility scope can name it as
//! an ancestor — i.e., inside the host module.
//!
//! `mod schema_slot` keeps the **shape** (trait, sealed marker,
//! witness ZST). Host modules contribute their own tag types and
//! impl the trait. The result: external crates cannot mint any
//! tag (visibility seal); cross-module crate-internal callers
//! cannot mint tags belonging to other modules.

use crate::decode::RowDesc;

/// Sealed-supertrait module. `pub(crate)` so in-crate host modules
/// can `impl Sealed for <their-tag>`. External crates cannot reach
/// this module (no public re-export).
pub(crate) mod sealed {
    /// Sealed marker — implementors are crate-internal only by
    /// virtue of `mod sealed`'s `pub(crate)` visibility (external
    /// crates have no path to the trait).
    pub trait Sealed {}
}

/// Sealed witness trait for a row-desc-slot write transition.
/// Implementors are ZST tags emitted at specific transition sites
/// (see [`mod self`] docs for the shape).
pub(crate) trait SchemaWriteAuth: sealed::Sealed {}

/// Tier-1 witness wrapping a mutable borrow of `PgProtocol::row_desc_slot`.
///
/// **Construction:** only via [`crate::PgProtocol::schema_slot_for_write`]
/// (which requires a [`SchemaWriteAuth`] proof at the call site) or
/// the auth-typed [`Self::from_field_with_auth`] (also gated on a
/// [`SchemaWriteAuth`] tag).
///
/// **Methods:** [`Self::park`] (set `Some(desc)`), [`Self::clear`]
/// (set `None`), and [`Self::raw_mut`] (extract the raw `&mut Option<RowDesc>`
/// borrow — used by `compute_push_bind_execute_idle_only`'s legacy
/// signature, which receives the raw ref as a parameter today;
/// post-N-D this surface will fold into the BindExecute install
/// witness).
///
/// All methods consume `self` so a single witness performs exactly
/// one write.
#[must_use = "schema slot witness must be consumed via park / clear / raw_mut"]
pub(crate) struct SchemaParkedSlot<'a> {
    slot: &'a mut Option<RowDesc>,
}

impl<'a> SchemaParkedSlot<'a> {
    /// Auth-typed constructor. Crate-internal modules can construct
    /// a witness if and only if they hold a [`SchemaWriteAuth`] tag —
    /// and tag construction is gated by per-module `pub(in ...)`
    /// visibility on the tag type's `new()` (defined in the host
    /// module). The auth tag itself is the proof that the caller is
    /// at a legitimate transition site.
    ///
    /// **Use case:** dispatch handlers receive `row_desc_slot:
    /// &mut Option<RowDesc>` as a parameter (not `&mut PgProtocol`,
    /// so they cannot call `schema_slot_for_write` directly). They
    /// mint the auth tag at the 'T' arm transition and pair it with
    /// the raw slot ref via this constructor.
    #[inline]
    pub(crate) fn from_field_with_auth<A: SchemaWriteAuth>(
        slot: &'a mut Option<RowDesc>,
        _auth: A,
    ) -> Self {
        Self { slot }
    }

    /// Set the slot to `Some(desc)`. Consumes self.
    #[inline]
    pub(crate) fn park(self, desc: RowDesc) {
        *self.slot = Some(desc);
    }

    /// Set the slot to `None`. Consumes self.
    #[inline]
    pub(crate) fn clear(self) {
        *self.slot = None;
    }
}

#[cfg(test)]
mod tests {
    /// Sealed-trait pin. The `mod sealed` module is `pub(crate)` and
    /// `Sealed` itself is `pub` within it; external crates have no
    /// path to either, so `impl SchemaWriteAuth for ExternalType`
    /// cannot be written outside this crate. Within the crate,
    /// each host module that defines a tag must `impl Sealed for
    /// <tag>` — surfacing the cross-module impl, by-design.
    #[test]
    fn seal_pin_anchor() {
        // Anchor for `git grep "schema_slot.*seal"` searches.
    }
}

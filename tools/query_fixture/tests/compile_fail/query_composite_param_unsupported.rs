// Binding a whole COMPOSITE value as a `$N` parameter (the row-type binary
// ENCODE) is a STAGED follow-up: decode is the high-value half and ships now.
// The precise framing: an ALL-NATIVE composite's field type OIDs are STABLE, so
// its `record` frame COULD be encoded — but a composite with an enum / domain /
// nested-composite field needs SERVER-DYNAMIC OIDs both for the composite's own
// type (the `$N` param OID, to select the binary recv function) AND for that
// field inside the frame (`record_recv` validates each field OID concretely),
// and bsql does NO connect-time OID resolution (the same boundary the enum
// decode rides; the scalar-enum "bind as unspecified OID 0" trick does not
// extend to a `record_recv` frame). Rather than ship only the all-native subset
// — a NON-UNIVERSAL partial — the WHOLE feature stages here.
//
// So a `query!` whose `$N` is inferred as a composite (`a` is the `addr` column
// from 0017_composites.sql) is a LOUD, located compile error — never a
// half-correct encoder.
bsql::query!(BadParam, "SELECT id FROM places WHERE a = $1");

fn main() {}

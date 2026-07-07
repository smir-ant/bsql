// A `copy!` naming MORE than 32 columns is a TAILORED `compile_error!` at the
// macro — naming the 32-column cap and the escape hatch — not a raw, untailored
// E0277 on the `Row<'q>: ParamsWriter` bound. 33 columns trips the arity
// pre-check (which fires before the per-column catalog lookup, so the column
// names need not exist).
fn main() {
    bsql::copy!(
        Bad,
        "copy_bulk",
        (
            x01, x02, x03, x04, x05, x06, x07, x08, x09, x10, x11, x12, x13, x14, x15, x16, x17,
            x18, x19, x20, x21, x22, x23, x24, x25, x26, x27, x28, x29, x30, x31, x32, x33
        )
    );
}

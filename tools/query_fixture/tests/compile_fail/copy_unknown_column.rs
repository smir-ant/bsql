// `copy_bulk` exists in the catalog but has no `nonexistent` column — `copy!`
// validates every column against the SAME build catalog as `query!`, so this is
// a `compile_error!` at the macro call, never a silent pass that would only fail
// at COPY time on the server.
fn main() {
    bsql::copy!(Bad, "copy_bulk", (id, nonexistent));
}

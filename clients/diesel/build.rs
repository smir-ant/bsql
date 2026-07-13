use std::fs;

// Emit the resolved diesel-async version (from Cargo.lock) as a compile-time env
// so the binary prints its true library version, never a hand-written guess.
fn main() {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let lock = format!("{manifest}/Cargo.lock");
    println!("cargo:rerun-if-changed={lock}");
    let mut ver = String::from("unknown");
    if let Ok(text) = fs::read_to_string(&lock) {
        let mut lines = text.lines().peekable();
        while let Some(line) = lines.next() {
            if line.trim() == "name = \"diesel-async\"" {
                if let Some(vline) = lines.peek() {
                    if let Some(rest) = vline.trim().strip_prefix("version = \"") {
                        if let Some(v) = rest.strip_suffix('"') {
                            ver = v.to_string();
                        }
                    }
                }
                break;
            }
        }
    }
    println!("cargo:rustc-env=DIESEL_ASYNC_VER={ver}");
}

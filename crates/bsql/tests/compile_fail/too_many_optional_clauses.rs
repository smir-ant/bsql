fn main() {
    let c0: Option<i32> = None;
    let c1: Option<i32> = None;
    let c2: Option<i32> = None;
    let c3: Option<i32> = None;
    let c4: Option<i32> = None;
    let c5: Option<i32> = None;
    let c6: Option<i32> = None;
    let c7: Option<i32> = None;
    let c8: Option<i32> = None;
    let _ = bsql::query!(
        "SELECT id FROM users WHERE 1=1
         [AND id = $c0: Option<i32>]
         [AND id = $c1: Option<i32>]
         [AND id = $c2: Option<i32>]
         [AND id = $c3: Option<i32>]
         [AND id = $c4: Option<i32>]
         [AND id = $c5: Option<i32>]
         [AND id = $c6: Option<i32>]
         [AND id = $c7: Option<i32>]
         [AND id = $c8: Option<i32>]"
    );
}

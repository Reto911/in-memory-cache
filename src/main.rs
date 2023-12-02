use lockfree_cuckoohash::*;

fn main() {
    let guard = pin();

    let map = LockFreeCuckooHash::new();
    map.insert(1, 1);

    let get = map.get(&1, &guard).unwrap();

    println!("{get}");
}

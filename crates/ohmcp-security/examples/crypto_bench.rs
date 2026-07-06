use std::time::Instant;
fn main() {
    let c = ohmcp_security::SessionCipher::new(&[7u8; 32]);
    let msg = vec![1u8; 64];
    let aad = [0u8; 9];
    let t = Instant::now();
    let mut total = 0usize;
    for _ in 0..100000 {
        let ct = c.encrypt(&msg, &aad);
        let pt = c.decrypt(&ct, &aad).unwrap();
        total += pt.len();
    }
    println!("100k enc+dec 64B: {:?} total={}", t.elapsed(), total);
    let big = vec![2u8; 3000];
    let t = Instant::now();
    for _ in 0..100000 {
        let ct = c.encrypt(&big, &aad);
        let _ = c.decrypt(&ct, &aad).unwrap();
    }
    println!("100k enc+dec 3KB: {:?}", t.elapsed());
}

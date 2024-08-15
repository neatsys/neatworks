pub mod search;
pub mod simulate;

#[cfg(test)]
mod tests {
    use arbtest::arbtest;

    #[test]
    fn pigeonhole() {
        arbtest(|u| {
            let buf = u.arbitrary::<[u8; 257]>()?;
            assert!(buf
                .iter()
                .enumerate()
                .any(|(i, b1)| buf.iter().skip(i + 1).any(|b2| b2 == b1)));
            Ok(())
        });
    }
}

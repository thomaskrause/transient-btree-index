mod btree;
mod errors;

pub use btree::SingleFileBtreeMap;
pub use errors::Error;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}

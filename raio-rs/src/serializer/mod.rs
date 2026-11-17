pub mod bincode;

pub trait Serializer<T> {
    /// The name by which a connection selects this serializer.
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }
    fn serialize(&self, value: &T) -> Vec<u8>;
    fn deserialize(&self, bytes: &[u8]) -> T;
}

#[cfg(test)]
pub(crate) mod tests {
    use super::bincode::BincodeSerializer;
    use super::*;

    /// A second serializer: an integer as decimal text, under its type name.
    pub(crate) struct Decimal;

    impl Serializer<i64> for Decimal {
        fn serialize(&self, value: &i64) -> Vec<u8> {
            value.to_string().into_bytes()
        }

        fn deserialize(&self, bytes: &[u8]) -> i64 {
            std::str::from_utf8(bytes).unwrap().parse().unwrap()
        }
    }

    #[test]
    fn a_serializer_is_named_after_its_type_unless_it_says_otherwise() {
        assert_eq!(Decimal.name(), std::any::type_name::<Decimal>());
        assert_eq!(Serializer::<i64>::name(&BincodeSerializer), "bincode");
    }
}

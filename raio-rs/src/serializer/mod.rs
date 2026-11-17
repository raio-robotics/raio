pub mod bincode;

pub trait Serializer<T> {
    fn serialize(&self, value: &T) -> Vec<u8>;
    fn deserialize(&self, bytes: &[u8]) -> T;
}

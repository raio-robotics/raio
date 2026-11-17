use serde::Serialize;
use serde::de::DeserializeOwned;

pub trait Serializer<T> {
    fn serialize(&self, value: &T) -> Vec<u8>;
    fn deserialize(&self, bytes: &[u8]) -> T;
}

pub struct SerdeSerializer;

impl<T: Serialize + DeserializeOwned> Serializer<T> for SerdeSerializer {
    fn serialize(&self, value: &T) -> Vec<u8> {
        bincode::serde::encode_to_vec(value, bincode::config::standard()).unwrap()
    }
    fn deserialize(&self, bytes: &[u8]) -> T {
        let (value, consumed) =
            bincode::serde::decode_from_slice(bytes, bincode::config::standard()).unwrap();
        assert!(
            consumed == bytes.len(),
            "serialized message has {} trailing bytes",
            bytes.len() - consumed
        );
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let s = SerdeSerializer;
        let value: (i64, String, Vec<f32>) = (-7, "hello".to_string(), vec![1.5, -0.25]);
        let bytes = s.serialize(&value);
        let back: (i64, String, Vec<f32>) = s.deserialize(&bytes);
        assert_eq!(back, value);
    }

    #[test]
    #[should_panic(expected = "trailing bytes")]
    fn trailing_bytes_are_a_bug() {
        let s = SerdeSerializer;
        let mut bytes = s.serialize(&42i64);
        bytes.push(0);
        let _: i64 = s.deserialize(&bytes);
    }

    fn check<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(value: T) {
        let s = SerdeSerializer;
        let bytes = s.serialize(&value);
        let back: T = s.deserialize(&bytes);
        assert_eq!(back, value);
    }

    #[test]
    fn primitives() {
        check(42i64);
        check(i64::MIN);
        check(u64::MAX);
        check(-0.25f64);
        check(true);
        check('é');
    }

    #[test]
    fn strings_and_collections() {
        check(String::from("hello, port"));
        check(String::new());
        check(vec![1.5f32, -0.25, f32::MAX]);
        check(Vec::<u8>::new());
        check(vec![(3u8, String::from("a")), (7u8, String::from("b"))]);
    }

    #[test]
    fn options() {
        check(Some(9usize));
        check(Option::<usize>::None);
    }

    #[test]
    fn large_payload() {
        check((0..100_000u32).collect::<Vec<_>>());
    }

    #[test]
    #[should_panic]
    fn truncated_bytes_are_a_bug() {
        let s = SerdeSerializer;
        let mut bytes = s.serialize(&String::from("hello, port"));
        bytes.truncate(bytes.len() - 3);
        let _: String = s.deserialize(&bytes);
    }
}

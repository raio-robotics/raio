use serde::Serialize;
use serde::de::DeserializeOwned;

use super::Serializer;

/// Serializes values as MessagePack, which nodes written in other languages can read.
pub struct MessagePackSerializer;

impl<T: Serialize + DeserializeOwned> Serializer<T> for MessagePackSerializer {
    fn name(&self) -> &str {
        "msgpack"
    }

    fn serialize(&self, value: &T) -> Vec<u8> {
        // A struct goes as a map of its field names, which other languages read by name.
        rmp_serde::to_vec_named(value).unwrap()
    }

    fn deserialize(&self, bytes: &[u8]) -> T {
        rmp_serde::from_slice(bytes).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Point {
        x: i64,
        y: i64,
    }

    #[test]
    fn round_trip() {
        let value: (i64, String, Vec<f32>) = (-7, "hello".to_string(), vec![1.5, -0.25]);
        let bytes = MessagePackSerializer.serialize(&value);
        let back: (i64, String, Vec<f32>) = MessagePackSerializer.deserialize(&bytes);
        assert_eq!(back, value);
    }

    #[test]
    fn a_struct_is_a_map_of_its_field_names_in_any_order() {
        let point = Point { x: 1, y: 2 };
        let bytes = MessagePackSerializer.serialize(&point);
        // A map of two entries: "x" to 1 and "y" to 2.
        assert_eq!(bytes, [0x82, 0xa1, b'x', 0x01, 0xa1, b'y', 0x02]);

        let reordered = [0x82, 0xa1, b'y', 0x02, 0xa1, b'x', 0x01];
        let back: Point = MessagePackSerializer.deserialize(&reordered);
        assert_eq!(back, point);
    }
}

pub mod action;
pub mod app;
pub mod coordinator;
pub mod driver;
pub mod graph;
pub mod messages;
pub mod node;
pub mod port;
pub mod reaction;
pub mod runtime;
pub mod serializer;
pub mod time;
pub mod transport;

pub use futures;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}

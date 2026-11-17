use std::cell::RefCell;
use std::rc::Rc;

use crate::coordinator::PortId;
use crate::messages::OutputInfo;
use crate::serializer::{Payload, Serializer};
use crate::transport::AnyTransport;
use crate::transport::tcp::TcpTransport;

/// The serializers or the transports that a port has, each with the name that selects it.
///
/// An input has one; an output has one for each that its connections use.
pub(crate) struct PortOptions<V: ?Sized>(RefCell<Vec<(String, Rc<V>)>>);

impl<V: ?Sized> PortOptions<V> {
    pub(crate) fn names(&self) -> Vec<String> {
        let options = self.0.borrow();
        options.iter().map(|(name, _)| name.clone()).collect()
    }

    /// Return the first option and its name; an input has only this one.
    pub(crate) fn first(&self) -> (String, Rc<V>) {
        self.0.borrow()[0].clone()
    }

    /// Return the option with this name; the port must have it.
    pub(crate) fn get(&self, name: &str) -> Rc<V> {
        let options = self.0.borrow();
        let found = options.iter().find(|(other, _)| other == name);
        let (_, option) = found.unwrap_or_else(|| panic!("the port has nothing named {name:?}"));
        option.clone()
    }

    /// Use only this option.
    pub(crate) fn set(&self, name: String, option: Rc<V>) {
        *self.0.borrow_mut() = vec![(name, option)];
    }

    /// Add the option unless the port has one with its name.
    pub(crate) fn add(&self, name: String, option: Rc<V>) {
        let mut options = self.0.borrow_mut();
        if options.iter().all(|(other, _)| *other != name) {
            options.push((name, option));
        }
    }
}

/// A port's id and what it uses to reach other nodes.
pub(crate) struct PortSetting<T> {
    pub(crate) id: PortId,
    pub(crate) serializers: PortOptions<dyn Serializer<T>>,
    pub(crate) transports: PortOptions<dyn AnyTransport>,
}

impl<T: Payload> PortSetting<T> {
    /// Create the setting of a port that uses its payload's default serializer over TCP.
    pub(crate) fn new(id: PortId) -> Self {
        let serializer = T::default_serializer();
        let tcp: Rc<dyn AnyTransport> = Rc::new(TcpTransport);
        let serializers = vec![(serializer.name().to_string(), serializer)];
        let transports = vec![(tcp.name().to_string(), tcp)];
        Self {
            id,
            serializers: PortOptions(RefCell::new(serializers)),
            transports: PortOptions(RefCell::new(transports)),
        }
    }
}

impl<T> PortSetting<T> {
    /// Describe an output: its position and every serializer and transport it has.
    pub(crate) fn describe(&self) -> OutputInfo {
        OutputInfo {
            id: self.id.port_id,
            serializers: self.serializers.names(),
            transports: self.transports.names(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serializer::tests::Decimal;

    fn setting() -> PortSetting<i64> {
        PortSetting::new(PortId {
            node_id: 0,
            port_id: 0,
        })
    }

    #[test]
    fn an_option_is_found_by_its_name_and_added_once() {
        let setting = setting();
        let decimal = Serializer::<i64>::name(&Decimal).to_string();
        setting.serializers.add(decimal.clone(), Rc::new(Decimal));
        setting.serializers.add(decimal.clone(), Rc::new(Decimal));
        assert_eq!(setting.serializers.names(), ["bincode", decimal.as_str()]);
        assert_eq!(setting.serializers.get(&decimal).name(), decimal);

        setting.serializers.set(decimal.clone(), Rc::new(Decimal));
        assert_eq!(setting.serializers.names(), [decimal.as_str()]);
        assert_eq!(setting.serializers.first().0, decimal);
    }

    #[test]
    #[should_panic(expected = "the port has nothing named \"pickle\"")]
    fn an_option_the_port_lacks_is_a_wiring_bug() {
        setting().serializers.get("pickle");
    }
}

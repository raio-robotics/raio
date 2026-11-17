use std::marker::PhantomData;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct PortId {
    pub node_id: usize,
    pub port_id: usize,
}

pub struct InputPort<T> {
    pub(crate) id: PortId,
    _marker: PhantomData<T>,
}

pub struct OutputPort<T> {
    pub(crate) id: PortId,
    _marker: PhantomData<T>,
}

pub struct Builder {
    node_id: usize,
    next_port_id: usize,
}

impl Builder {
    pub(crate) fn new(node_id: usize) -> Self {
        Self {
            node_id,
            next_port_id: 0,
        }
    }

    pub(crate) fn port_count(&self) -> usize {
        self.next_port_id
    }

    pub fn input<T>(&mut self) -> InputPort<T> {
        InputPort {
            id: self.next_port_id(),
            _marker: PhantomData,
        }
    }

    pub fn output<T>(&mut self) -> OutputPort<T> {
        OutputPort {
            id: self.next_port_id(),
            _marker: PhantomData,
        }
    }

    fn next_port_id(&mut self) -> PortId {
        let id = PortId {
            node_id: self.node_id,
            port_id: self.next_port_id,
        };
        self.next_port_id += 1;
        id
    }
}

pub use crate::port::{Builder, InputPort, OutputPort, PortId};
use crate::runtime::Runtime;

type Runner = Box<dyn FnOnce(&Runtime)>;

pub struct App {
    edges: Vec<(PortId, PortId)>,
    nodes: Vec<Runner>,
}

impl App {
    pub fn new() -> Self {
        Self {
            edges: Vec::new(),
            nodes: Vec::new(),
        }
    }

    pub fn connect<T>(&mut self, from: &OutputPort<T>, to: &InputPort<T>) {
        self.edges.push((from.id, to.id));
    }

    pub fn create_node<N, F, Fut>(
        &mut self,
        constructor: impl Fn(&mut Builder) -> N + 'static,
        start: F,
    ) -> N
    where
        N: 'static,
        F: FnOnce(N) -> Fut + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let node_id = self.nodes.len();
        let mut builder = Builder::new(node_id);
        let handle = constructor(&mut builder);
        let ports = builder.port_count();
        self.nodes.push(Box::new(move |runtime| {
            let mut builder = Builder::new(node_id);
            let node = constructor(&mut builder);
            assert!(
                builder.port_count() == ports,
                "node {node_id}: constructor created a different number of ports \
                 than on its first run; constructors must be deterministic"
            );
            runtime.block_on(start(node));
        }));
        handle
    }

    pub fn run(self) {
        let runtime = Runtime::new();
        for runner in self.nodes {
            runner(&runtime);
        }
    }
}

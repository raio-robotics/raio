use raio::app::{Assignment, Builder, InputPort, Node, OutputPort, create_node, run_node_as};
use raio::coordinator::{Connection, Graph, PortId, spawn};
use raio::reaction;
use raio::tasks;
use raio::time::{MS, Time};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

struct Ticker {
    node: Node,
    out: OutputPort<i64>,
}

impl Ticker {
    async fn start(self: Rc<Self>, period: Time, ticks: i64) {
        for i in 0..ticks {
            self.node.wait(period).await;
            self.out.set(i);
        }
    }
}

struct Order {
    node: Node,
    a: InputPort<i64>,
    b: InputPort<i64>,
    /// The timestamp and the name of each reaction that ran.
    ran: Rc<RefCell<Vec<(Time, &'static str)>>>,
}

impl Order {
    async fn first(self: Rc<Self>) {
        reaction!(self, [b], {
            self.ran.borrow_mut().push((self.node.now(), "first"));
        });
    }

    async fn second(self: Rc<Self>) {
        reaction!(self, [a], {
            self.ran.borrow_mut().push((self.node.now(), "second"));
        });
    }

    async fn third(self: Rc<Self>) {
        reaction!(self, [a, b], {
            self.ran.borrow_mut().push((self.node.now(), "third"));
        });
    }
}

#[test]
fn reactions_keep_their_order_when_only_some_ports_have_a_value() {
    // A ticker of 1ms feeds `a` and a ticker of 2ms feeds `b`, so every other timestamp has
    // only `a`.
    let connect = |ticker, input| Connection {
        from: PortId {
            node_id: ticker,
            port_id: 0,
        },
        to: PortId {
            node_id: 2,
            port_id: input,
        },
        delay: Time::ZERO,
    };
    let graph = Graph {
        nodes: 3,
        connections: vec![connect(0, 0), connect(1, 1)],
    };
    let (coordinator_endpoint, coordinator, _) = spawn(graph, HashMap::new());
    let assign = |node_id| Assignment {
        node_id,
        coordinator_endpoint: coordinator_endpoint.clone(),
    };

    let tickers = [(1 * MS, 4), (2 * MS, 2)].into_iter().enumerate();
    let tickers: Vec<_> = tickers
        .map(|(node_id, (period, ticks))| {
            let assignment = assign(node_id);
            std::thread::spawn(move || {
                let constructor = |builder: &mut Builder| Ticker {
                    node: builder.node(),
                    out: builder.output(),
                };
                let tasks = tasks![|ticker: Rc<Ticker>| ticker.start(period, ticks)];
                run_node_as(assignment, create_node(constructor, tasks));
            })
        })
        .collect();
    let assignment = assign(2);
    let order = std::thread::spawn(move || {
        let ran = Rc::new(RefCell::new(Vec::new()));
        let log = ran.clone();
        let constructor = move |builder: &mut Builder| Order {
            node: builder.node(),
            a: builder.input(),
            b: builder.input(),
            ran: log,
        };
        let tasks = tasks![Order::first, Order::second, Order::third];
        run_node_as(assignment, create_node(constructor, tasks));
        ran.take()
    });

    assert_eq!(coordinator.join().unwrap(), Ok(()));
    for ticker in tickers {
        ticker.join().unwrap();
    }
    assert_eq!(
        order.join().unwrap(),
        [
            (1 * MS, "second"),
            (1 * MS, "third"),
            (2 * MS, "first"),
            (2 * MS, "second"),
            (2 * MS, "third"),
            (3 * MS, "second"),
            (3 * MS, "third"),
            (4 * MS, "first"),
            (4 * MS, "second"),
            (4 * MS, "third"),
        ]
    );
}

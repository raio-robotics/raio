use mio::{Events, Poll, Registry, Token};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Waker;

enum Status {
    Waiting(Waker),
    Happened,
}
pub struct Mailroom {
    registry: Registry,
    mailbox: Mutex<HashMap<Token, Status>>,
}

impl Mailroom {
    pub fn deliver(&self, token: Token) {
        let mut mailbox = self.mailbox.lock().unwrap();
        match mailbox.insert(token, Status::Happened) {
            // insert returns the old value
            Some(Status::Waiting(waker)) => waker.wake(),
            _ => (),
        }
    }
}

pub struct Driver {
    poll: Poll,
    events: Events,
    mailroom: Arc<Mailroom>,
}

impl Driver {
    pub fn new() -> Self {
        let poll = Poll::new().unwrap();
        Self {
            mailroom: Arc::new(Mailroom {
                registry: poll.registry().try_clone().unwrap(),
                mailbox: Mutex::new(HashMap::new()),
            }),
            poll,
            events: Events::with_capacity(1024),
        }
    }

    pub fn turn(&mut self) {
        self.poll.poll(&mut self.events, None).unwrap();
        for event in self.events.iter() {
            self.mailroom.deliver(event.token());
        }
    }
}

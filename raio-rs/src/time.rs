use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{Add, Mul, Sub};
use std::time::Duration;

/// A logical timestamp or delay, ordered by nanoseconds and then microsteps.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct Time {
    nanos: i64,
    microstep: u32,
}

impl Time {
    pub const ZERO: Time = Time::new(0);
    pub const NEVER: Time = Time::new(i64::MIN);
    pub const FOREVER: Time = Time::new(i64::MAX);

    pub const fn new(nanos: i64) -> Self {
        Time {
            nanos,
            microstep: 0,
        }
    }

    pub fn nanos(&self) -> i64 {
        self.nanos
    }

    pub fn microstep(&self) -> u32 {
        self.microstep
    }

    pub fn duration(&self) -> Duration {
        Duration::from_nanos(self.nanos as u64)
    }

    /// Advance one microstep within this nanosecond; NEVER and FOREVER remain unchanged.
    pub fn next_microstep(self) -> Time {
        if self == Time::NEVER || self == Time::FOREVER {
            return self;
        }
        Time {
            nanos: self.nanos,
            microstep: self.microstep + 1,
        }
    }
}

/// Adding a nonzero nanosecond delay resets the microstep.
impl Add for Time {
    type Output = Time;
    fn add(self, rhs: Time) -> Time {
        if self == Time::NEVER || rhs == Time::NEVER {
            return Time::NEVER;
        }
        if self == Time::FOREVER || rhs == Time::FOREVER {
            return Time::FOREVER;
        }
        Time {
            nanos: self.nanos + rhs.nanos,
            microstep: if rhs.nanos == 0 {
                self.microstep + rhs.microstep
            } else {
                0
            },
        }
    }
}

impl Sub for Time {
    type Output = Time;
    fn sub(self, rhs: Time) -> Time {
        if rhs == Time::NEVER {
            return Time::FOREVER;
        }
        if self == Time::NEVER || rhs == Time::FOREVER {
            return Time::NEVER;
        }
        if self == Time::FOREVER {
            return Time::FOREVER;
        }
        Time::new(self.nanos - rhs.nanos)
    }
}

impl Mul<i64> for Time {
    type Output = Time;
    fn mul(self, rhs: i64) -> Time {
        if self == Time::NEVER {
            return Time::NEVER;
        }
        if self == Time::FOREVER {
            return Time::FOREVER;
        }
        Time::new(self.nanos * rhs)
    }
}

impl Mul<Time> for i64 {
    type Output = Time;
    fn mul(self, rhs: Time) -> Time {
        rhs * self
    }
}

impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Time::NEVER => return write!(f, "NEVER"),
            Time::FOREVER => return write!(f, "FOREVER"),
            _ => {}
        }
        if self.microstep > 0 {
            return write!(f, "({}, {})", Time::new(self.nanos), self.microstep);
        }
        for (unit, name) in [
            (HOUR, "h"),
            (MINUTE, "min"),
            (S, "s"),
            (MS, "ms"),
            (US, "us"),
        ] {
            if self.nanos != 0 && self.nanos % unit.nanos == 0 {
                return write!(f, "{}{}", self.nanos / unit.nanos, name);
            }
        }
        write!(f, "{}ns", self.nanos)
    }
}

pub const NS: Time = Time::new(1);
pub const US: Time = Time::new(1_000);
pub const MS: Time = Time::new(1_000_000);
pub const SECOND: Time = Time::new(1_000_000_000);
pub const S: Time = SECOND;
pub const MINUTE: Time = Time::new(60 * 1_000_000_000);
pub const HOUR: Time = Time::new(3_600 * 1_000_000_000);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn units_compose() {
        assert_eq!(S, SECOND);
        assert_eq!(NS * 1_000, US);
        assert_eq!(US * 1_000, MS);
        assert_eq!(MS * 1_000, S);
        assert_eq!(S * 60, MINUTE);
        assert_eq!(MINUTE * 60, HOUR);
    }

    #[test]
    fn ordering() {
        assert!(Time::NEVER < Time::ZERO);
        assert!(Time::ZERO < NS);
        assert!(S < Time::FOREVER);
    }

    #[test]
    fn microsteps_order_within_a_nanosecond() {
        let tag = 1 * S;
        assert!(tag < tag.next_microstep());
        assert!(tag.next_microstep() < tag.next_microstep().next_microstep());
        assert!(tag.next_microstep() < 1 * S + NS); // Nanoseconds take precedence over microsteps.
    }

    #[test]
    fn a_delay_resets_the_microstep_and_zero_keeps_it() {
        let tag = (1 * S).next_microstep();
        assert_eq!(tag + Time::ZERO, tag);
        assert_eq!(tag + NS, 1 * S + NS);
        assert_eq!(tag.microstep(), 1);
        assert_eq!((tag + NS).microstep(), 0);
    }

    #[test]
    fn sentinels_have_no_next_microstep() {
        assert_eq!(Time::NEVER.next_microstep(), Time::NEVER);
        assert_eq!(Time::FOREVER.next_microstep(), Time::FOREVER);
    }

    #[test]
    fn finite_arithmetic() {
        assert_eq!(S + MS * 500, MS * 1_500);
        assert_eq!(S - MS, MS * 999);
        assert_eq!(Time::new(5).nanos(), 5);
    }

    #[test]
    fn sentinels_absorb() {
        assert_eq!(Time::NEVER + S, Time::NEVER);
        assert_eq!(S + Time::FOREVER, Time::FOREVER);
        // NEVER takes precedence when both sentinels are added.
        assert_eq!(Time::NEVER + Time::FOREVER, Time::NEVER);
        assert_eq!(Time::NEVER * 0, Time::NEVER);
        assert_eq!(Time::FOREVER * 3, Time::FOREVER);
        assert_eq!(S - Time::FOREVER, Time::NEVER);
        assert_eq!(S - Time::NEVER, Time::FOREVER);
        assert_eq!(Time::FOREVER - S, Time::FOREVER);
    }

    #[test]
    fn display() {
        assert_eq!(Time::ZERO.to_string(), "0ns");
        assert_eq!(Time::new(1_234).to_string(), "1234ns");
        assert_eq!((MS * 1_500).to_string(), "1500ms");
        assert_eq!((S * 90).to_string(), "90s");
        assert_eq!((MINUTE * 2).to_string(), "2min");
        assert_eq!(HOUR.to_string(), "1h");
        assert_eq!((1 * S).next_microstep().to_string(), "(1s, 1)");
        assert_eq!(Time::NEVER.to_string(), "NEVER");
        assert_eq!(Time::FOREVER.to_string(), "FOREVER");
    }
}

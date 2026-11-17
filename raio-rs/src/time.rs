use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{Add, Mul, Sub};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct Time(i64);

impl Time {
    pub const ZERO: Time = Time(0);
    pub const NEVER: Time = Time(i64::MIN);
    pub const FOREVER: Time = Time(i64::MAX);

    pub fn new(nanos: i64) -> Self {
        Time(nanos)
    }

    pub fn nanos(&self) -> i64 {
        self.0
    }

    pub fn after(self, delay: Time) -> Time {
        if self == Time::NEVER || delay == Time::NEVER {
            return Time::NEVER;
        }
        if self == Time::FOREVER || delay == Time::FOREVER {
            return Time::FOREVER;
        }
        Time(self.0 + delay.0)
    }
}

impl Add for Time {
    type Output = Time;
    fn add(self, rhs: Time) -> Time {
        if self == Time::NEVER || rhs == Time::NEVER {
            return Time::NEVER;
        }
        if self == Time::FOREVER || rhs == Time::FOREVER {
            return Time::FOREVER;
        }
        Time(self.0 + rhs.0)
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
        Time(self.0 - rhs.0)
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
        Time(self.0 * rhs)
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
        for (unit, name) in [
            (HOUR, "h"),
            (MINUTE, "min"),
            (S, "s"),
            (MS, "ms"),
            (US, "us"),
        ] {
            if self.0 != 0 && self.0 % unit.0 == 0 {
                return write!(f, "{}{}", self.0 / unit.0, name);
            }
        }
        write!(f, "{}ns", self.0)
    }
}

pub const NS: Time = Time(1);
pub const US: Time = Time(1_000);
pub const MS: Time = Time(1_000_000);
pub const SECOND: Time = Time(1_000_000_000);
pub const S: Time = SECOND;
pub const MINUTE: Time = Time(60 * 1_000_000_000);
pub const HOUR: Time = Time(3_600 * 1_000_000_000);

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
    fn finite_arithmetic() {
        assert_eq!(S + MS * 500, MS * 1_500);
        assert_eq!(S - MS, MS * 999);
        assert_eq!(Time::new(5).nanos(), 5);
    }

    #[test]
    fn sentinels_absorb() {
        assert_eq!(Time::NEVER + S, Time::NEVER);
        assert_eq!(S + Time::FOREVER, Time::FOREVER);
        assert_eq!(Time::NEVER + Time::FOREVER, Time::NEVER); // NEVER wins
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
        assert_eq!(Time::NEVER.to_string(), "NEVER");
        assert_eq!(Time::FOREVER.to_string(), "FOREVER");
    }
}

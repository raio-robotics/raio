#[macro_export]
macro_rules! reaction {
    ($self:ident, [$($port:ident),+ $(,)?], $body:block) => {{
        let mut open = [$({
            let _ = stringify!($port);
            true
        }),+];
        loop {
            let mut triggers = Vec::new();
            let mut indexes = Vec::new();
            let mut index = 0;
            $(
                if open[index] {
                    triggers.push($self.$port.trigger());
                    indexes.push(index);
                }
                index += 1;
            )+
            let _ = index;
            if triggers.is_empty() {
                break;
            }
            let (fired, which, rest) = $crate::futures::future::select_all(triggers).await;
            drop(rest);
            if fired {
                $body
            } else {
                open[indexes[which]] = false;
            }
        }
    }};
}

use crate::action::Action;
use crate::condition::Condition;
use crate::error::Error;
use futures::task::{Context, Poll};
use futures::Future;
use std::pin::Pin;
use tokio::time::{self, Delay, Duration};
use pin_project::pin_project;

#[pin_project(project = RetryStateProj)]
pub enum RetryState<F> {
    Running(#[pin] F),
    Sleeping(Delay),
}

/// Retry is a Future that returns the result of an Action
/// It uses RetryIf to execute the Action possibly multiple times with a retry strategy
#[pin_project]
pub struct Retry<A>
where
    A: Action,
{
    #[pin]
    retry_if: RetryIf<A>,
}

impl<A> Retry<A>
where
    A: Action + 'static,
{
    pub fn new<
        I: Iterator<Item = Duration>,
        T: IntoIterator<IntoIter = I, Item = Duration> + 'static,
    >(
        strategy: T,
        action: A,
    ) -> Retry<A> {
        Retry {
            retry_if: RetryIf::new(
                strategy,
                action,
                (|_| true) as fn(&A::Error) -> bool,
            ),
        }
    }
}

impl<A, O, E> Future for Retry<A>
where
    A: Action<Item = O, Error = E>,
{
    type Output = Result<A::Item, Error<A::Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().retry_if.poll(cx)
    }
}

pub struct RetryIf<A>
where
    A: Action,
{
    inner: Pin<Box<dyn Future<Output = Result<A::Item, Error<A::Error>>>>>,
}

impl<A> RetryIf<A>
where
    A: Action + 'static,
{
    pub fn new<
        I: Iterator<Item = Duration>,
        T: IntoIterator<IntoIter = I, Item = Duration> + 'static,
        C: Condition<A::Error> + 'static,
    >(
        strategy: T,
        mut action: A,
        condition: C,
    ) -> RetryIf<A> {
        RetryIf {
            inner: Box::pin(async move {
                Self::run(strategy, Self::attempt(&mut action), action, condition).await
            }),
        }
    }

    pub fn attempt(action: &mut A) -> Pin<Box<RetryState<A::Future>>> {
        Box::pin(RetryState::Running(action.run()))
    }

    pub async fn run<
        I: Iterator<Item = Duration>,
        T: IntoIterator<IntoIter = I, Item = Duration>,
        C: Condition<A::Error>,
    >(
        strategy: T,
        mut state: Pin<Box<RetryState<A::Future>>>,
        mut action: A,
        mut condition: C,
    ) -> Result<A::Item, Error<A::Error>> {
        let mut strategy = strategy.into_iter();
        loop {
            match state.as_mut().project() {
                RetryStateProj::Running(ref mut f) => match f.await {
                    Ok(ok) => {
                        return Ok(ok);
                    }
                    Err(err) => {
                        if condition.should_retry(&err) {
                            state = Self::retry(&mut strategy, err)?;
                        } else {
                            return Err(Error::OperationError(err));
                        }
                    }
                },
                RetryStateProj::Sleeping(ref mut d) => {
                    d.await;
                    state = Self::attempt(&mut action);
                }
            }
        }
    }

    pub fn retry<I: Iterator<Item = Duration>>(
        strategy: &mut I,
        err: A::Error,
    ) -> Result<Pin<Box<RetryState<A::Future>>>, Error<A::Error>> {
        strategy
            .next()
            .ok_or_else(|| Error::OperationError(err))
            .map(|duration| Box::pin(RetryState::Sleeping(time::delay_for(duration))))
    }
}

impl<A> Future for RetryIf<A>
where
    A: Action,
{
    type Output = Result<A::Item, Error<A::Error>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

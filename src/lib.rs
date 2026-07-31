use std::{ops::Deref, sync::Arc};

use tokio::sync::{Semaphore, SemaphorePermit};

/// A wrapper around a specific value to limit the maximum allowed number of references to it.
#[derive(Clone)]
pub struct Throttle<T> {
    inner: T,
    semaphore: Arc<Semaphore>,
}

impl<T> Throttle<T> {
    /// Create a new `Throttle` with supplied value and maximum allowed concurrent accesses.
    pub fn new(resource: T, parallel: usize) -> Self {
        Self {
            inner: resource,
            semaphore: Arc::new(Semaphore::new(parallel)),
        }
    }

    /// Create a new `Throttle` with supplied value and unlimited concurrent accesses.
    ///
    /// Note that "unlimited" means a maximum number of [`Semaphore::MAX_PERMITS`] concurrent accesses,
    /// which is `usize::MAX >> 3` and unlikely to be reached in practice.
    pub fn new_unlimited(resource: T) -> Self {
        Self {
            inner: resource,
            semaphore: Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
        }
    }

    /// Get a reference to the inner `tokio::sync::Semaphore`.
    pub fn as_semaphore(&self) -> &Semaphore {
        &self.semaphore
    }

    /// Acquire a smart-pointer to access the inner value, waiting until a permit is available if necessary.
    pub async fn get(&self) -> Throttled<'_, T> {
        let permit = self.semaphore.acquire().await.expect("semaphore closed");

        Throttled {
            inner: &self.inner,
            permit,
        }
    }

    /// Bypass the throttling mechanism and directly get a reference to the inner value.
    pub fn unwrap(&self) -> &T {
        &self.inner
    }

    /// Create a new `Throttle` on a different resource that shares the same limitation with the current one.
    pub fn share<U>(&self, new: U) -> Throttle<U> {
        Throttle {
            inner: new,
            semaphore: self.semaphore.clone(),
        }
    }

    /// Create a new `Throttle` by calling the supplied function on the internal value and sharing the same limitation with the current one.
    pub fn derive<U>(&self, f: impl FnOnce(&T) -> U) -> Throttle<U> {
        Throttle {
            inner: f(&self.inner),
            semaphore: self.semaphore.clone(),
        }
    }

    /// Create a new `Throttle` by calling the supplied function on the internal value and sharing the same limitation with the current one.
    pub fn try_derive<U, E>(&self, f: impl FnOnce(&T) -> Result<U, E>) -> Result<Throttle<U>, E> {
        let new = Throttle {
            inner: f(&self.inner)?,
            semaphore: self.semaphore.clone(),
        };

        Ok(new)
    }
}

pub struct Throttled<'a, T> {
    inner: &'a T,

    #[allow(unused)]
    permit: SemaphorePermit<'a>,
}

impl<'a, T> Deref for Throttled<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<'a, T> AsRef<T> for Throttled<'a, T> {
    fn as_ref(&self) -> &T {
        self.deref()
    }
}

impl<'a, T> Throttled<'a, T> {
    /// Forget the throttling mechanism, releasing the permit and return a direct reference to the inner value.
    pub fn forget(self) -> &'a T {
        self.inner
    }
}

pub trait SemaphoreExt {
    fn throttle<T>(self, resource: T) -> Throttle<T>;
}

impl SemaphoreExt for Arc<Semaphore> {
    fn throttle<T>(self, resource: T) -> Throttle<T> {
        Throttle {
            inner: resource,
            semaphore: self,
        }
    }
}

impl SemaphoreExt for Semaphore {
    fn throttle<T>(self, resource: T) -> Throttle<T> {
        Arc::new(self).throttle(resource)
    }
}

pub trait IntoThrottle: Sized {
    fn into_throttle(self, parallel: usize) -> Throttle<Self>;
}

impl<T: Sized> IntoThrottle for T {
    fn into_throttle(self, parallel: usize) -> Throttle<Self> {
        Throttle::new(self, parallel)
    }
}

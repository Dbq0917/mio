//! Reusable connection identifier allocation.
//!
//! A [`ConnectionIdAllocator`] hands out identifiers from a fixed-size space
//! (just over sixty thousand identifiers by default). Without recycling the
//! identifiers are monotonically increasing: a long-running process that
//! repeatedly opens and closes connections eventually reaches the end of the
//! space and has to wrap around. A wrapped identifier can still be referenced
//! by an event that was queued for a previous owner, so the event gets
//! delivered to a connection that never produced it.
//!
//! This module solves that by recycling identifiers while keeping a grace
//! period between the moment a connection closes and the moment its
//! identifier can be handed out again:
//!
//! - [`ConnectionIdAllocator::register`] returns an identifier taken from the
//!   free list first, only growing into never-used space when the free list
//!   is empty.
//! - [`ConnectionIdAllocator::deregister`] does not return the identifier to
//!   the free list immediately. It parks it in a quarantine together with
//!   the current epoch.
//! - [`ConnectionIdAllocator::turn`] advances the epoch and promotes
//!   identifiers whose grace period has elapsed back to the free list.
//!
//! `turn` is called once per event-loop iteration, after the queued events
//! have been drained and before the next wait for events. An identifier freed
//! in epoch `e` is not reused until two later iterations have drained their
//! event queues, so no event still queued for the old connection can carry the
//! identifier to its new owner.

use std::error::Error;
use std::fmt;

/// Identifier handed out by a [`ConnectionIdAllocator`].
///
/// The wrapped value is the raw numeric identifier, ranging from `0` up to the
/// allocator's capacity minus one.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnectionId(pub usize);

impl ConnectionId {
    /// Return the raw numeric identifier.
    pub fn as_usize(self) -> usize {
        self.0
    }
}

impl From<ConnectionId> for usize {
    fn from(id: ConnectionId) -> usize {
        id.0
    }
}

/// Errors returned by [`ConnectionIdAllocator`].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum AllocError {
    /// Every identifier is either in use or still quarantined, so no
    /// identifier can be handed out right now.
    Exhausted,
    /// `deregister` was called with an identifier that is not currently in
    /// use.
    InvalidId,
}

impl fmt::Display for AllocError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AllocError::Exhausted => f.write_str("connection identifier space exhausted"),
            AllocError::InvalidId => f.write_str("connection identifier is not in use"),
        }
    }
}

impl Error for AllocError {}

/// Number of completed event-loop iterations an identifier spends in
/// quarantine before it becomes reusable.
///
/// An identifier deregistered during epoch `e` is first eligible for reuse in
/// epoch `e + GRACE`, meaning two fully drained event queues lie between the
/// old and the new owner.
const GRACE: u64 = 2;

/// Identifier parked in the quarantine.
#[derive(Copy, Clone, Debug)]
struct Retired {
    id: ConnectionId,
    epoch: u64,
}

/// Allocates recyclable [`ConnectionId`]s from a fixed-size identifier space.
///
/// See the [module documentation](self) for the recycling and grace-period
/// rules.
#[derive(Debug)]
pub struct ConnectionIdAllocator {
    /// `true` at index `i` while identifier `i` is handed out.
    in_use: Vec<bool>,
    /// Identifiers available for immediate reuse.
    free: Vec<ConnectionId>,
    /// Recently freed identifiers waiting out the grace period, in FIFO order.
    quarantine: Vec<Retired>,
    /// Number of identifiers that have ever been handed out.
    high_water: usize,
    /// Monotonically increasing event-loop iteration counter.
    epoch: u64,
}

impl ConnectionIdAllocator {
    /// Create an allocator with the default capacity of `u16::MAX + 1`
    /// (65 536) identifiers.
    pub fn new() -> ConnectionIdAllocator {
        ConnectionIdAllocator::with_capacity(usize::from(u16::MAX) + 1)
    }

    /// Create an allocator holding `capacity` identifiers, numbered `0`
    /// through `capacity - 1`.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero.
    pub fn with_capacity(capacity: usize) -> ConnectionIdAllocator {
        assert!(capacity > 0, "capacity must be greater than zero");
        ConnectionIdAllocator {
            in_use: vec![false; capacity],
            free: Vec::new(),
            quarantine: Vec::new(),
            high_water: 0,
            epoch: 0,
        }
    }

    /// Number of distinct identifiers this allocator can ever hand out.
    pub fn capacity(&self) -> usize {
        self.in_use.len()
    }

    /// Number of identifiers currently handed out.
    pub fn active(&self) -> usize {
        self.in_use.iter().filter(|in_use| **in_use).count()
    }

    /// Number of identifiers that have been handed out at least once.
    pub fn high_water(&self) -> usize {
        self.high_water
    }

    /// Allocate an identifier for a newly registered connection.
    ///
    /// Recycled identifiers are preferred over fresh ones, so the identifier
    /// space never wraps into an identifier that still has queued events.
    /// Returns [`AllocError::Exhausted`] when every identifier is in use or
    /// quarantined.
    pub fn register(&mut self) -> Result<ConnectionId, AllocError> {
        if let Some(id) = self.free.pop() {
            self.mark_used(id);
            return Ok(id);
        }

        if self.high_water == self.in_use.len() {
            return Err(AllocError::Exhausted);
        }

        let id = ConnectionId(self.high_water);
        self.high_water += 1;
        self.mark_used(id);
        Ok(id)
    }

    /// Release the identifier of a closed connection.
    ///
    /// The identifier is quarantined rather than reused immediately. It only
    /// becomes reusable after [`ConnectionIdAllocator::turn`] has advanced past
    /// the grace period. Returns [`AllocError::InvalidId`] if the identifier is
    /// not currently in use.
    pub fn deregister(&mut self, id: ConnectionId) -> Result<(), AllocError> {
        if id.0 >= self.in_use.len() || !self.in_use[id.0] {
            return Err(AllocError::InvalidId);
        }

        self.in_use[id.0] = false;
        self.quarantine.push(Retired {
            id,
            epoch: self.epoch,
        });
        Ok(())
    }

    /// Advance the event-loop epoch by one and recycle identifiers whose grace
    /// period has elapsed.
    ///
    /// Call this once per iteration, after all events reported by the latest
    /// poll have been processed. Returns the number of identifiers promoted
    /// back to the free list.
    pub fn turn(&mut self) -> usize {
        self.epoch += 1;

        let mut eligible = 0;
        while eligible < self.quarantine.len()
            && self.epoch >= self.quarantine[eligible].epoch + GRACE
        {
            eligible += 1;
        }

        if eligible == 0 {
            return 0;
        }

        self.free
            .extend(self.quarantine.drain(..eligible).map(|retired| retired.id));
        eligible
    }

    fn mark_used(&mut self, id: ConnectionId) {
        debug_assert!(!self.in_use[id.0], "identifier handed out twice");
        self.in_use[id.0] = true;
    }
}

impl Default for ConnectionIdAllocator {
    fn default() -> ConnectionIdAllocator {
        ConnectionIdAllocator::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{AllocError, ConnectionId, ConnectionIdAllocator, GRACE};

    #[test]
    fn allocates_sequentially_until_reuse() {
        let mut allocator = ConnectionIdAllocator::with_capacity(4);

        let a = allocator.register().unwrap();
        let b = allocator.register().unwrap();
        assert_eq!(a, ConnectionId(0));
        assert_eq!(b, ConnectionId(1));
        assert_eq!(allocator.active(), 2);
        assert_eq!(allocator.high_water(), 2);

        allocator.deregister(a).unwrap();

        // The identifier is quarantined: fresh space is used meanwhile.
        let c = allocator.register().unwrap();
        assert_eq!(c, ConnectionId(2));

        // It remains unavailable until the grace period elapses.
        for _ in 0..GRACE - 1 {
            assert_eq!(allocator.turn(), 0);
            assert_eq!(allocator.register().unwrap(), ConnectionId(3));
            allocator.deregister(ConnectionId(3)).unwrap();
        }
        assert_eq!(allocator.turn(), 1);

        // Now the recycled identifier is preferred over growing the space.
        assert_eq!(allocator.register().unwrap(), a);
        assert_eq!(allocator.high_water(), 4);
    }

    #[test]
    fn never_reuses_an_identifier_with_events_still_queued() {
        let mut allocator = ConnectionIdAllocator::with_capacity(2);

        let first = allocator.register().unwrap();
        assert_eq!(first, ConnectionId(0));
        allocator.deregister(first).unwrap();

        // No turn has happened, so the closed identifier must stay unavailable.
        let other = allocator.register().unwrap();
        assert_eq!(other, ConnectionId(1));

        // All identifiers are either quarantined or in use.
        assert_eq!(allocator.register(), Err(AllocError::Exhausted));

        // One drained iteration is not enough either.
        assert_eq!(allocator.turn(), 0);
        assert_eq!(allocator.register(), Err(AllocError::Exhausted));

        // After the grace period the identifier is safe to hand out again.
        assert_eq!(allocator.turn(), 1);
        allocator.deregister(other).unwrap();
        assert_eq!(allocator.register().unwrap(), first);
    }

    #[test]
    fn rejects_invalid_deregistrations() {
        let mut allocator = ConnectionIdAllocator::with_capacity(2);
        let id = allocator.register().unwrap();

        assert_eq!(
            allocator.deregister(ConnectionId(99)),
            Err(AllocError::InvalidId)
        );

        allocator.deregister(id).unwrap();
        assert_eq!(allocator.deregister(id), Err(AllocError::InvalidId));
    }
}

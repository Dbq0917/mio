use mio::connection::{AllocError, ConnectionId, ConnectionIdAllocator};

#[test]
fn recycled_identifiers_stay_within_the_space() {
    // A handful of long-lived connections plus a constant churn of short-lived
    // ones, exercising both the free list and the quarantine grace period.
    const PINNED: usize = 16;
    const CAPACITY: usize = 1024;
    const ITERATIONS: usize = 10_000_000;

    let mut allocator = ConnectionIdAllocator::with_capacity(CAPACITY);

    let pinned: Vec<ConnectionId> = (0..PINNED).map(|_| allocator.register().unwrap()).collect();

    let mut max_seen = PINNED;
    for _ in 0..ITERATIONS {
        let id = allocator.register().expect("identifier space exhausted");
        assert!(id.as_usize() < CAPACITY);
        max_seen = max_seen.max(id.as_usize() + 1);
        allocator.deregister(id).unwrap();

        allocator.turn();
    }

    // The recycled identifiers keep the high-water mark far below the space:
    // without recycling this would have wrapped after ~1 000 registrations.
    assert!(max_seen <= PINNED + 8, "high-water mark grew to {max_seen}");
    assert_eq!(allocator.active(), PINNED);
    assert!(
        allocator.high_water() <= PINNED + 8,
        "unexpected high-water mark {}",
        allocator.high_water()
    );

    // Pinned connections kept their identifiers for the whole run.
    for (index, id) in pinned.iter().enumerate() {
        assert_eq!(*id, ConnectionId(index));
    }
}

#[test]
fn quarantine_delays_reuse_for_two_drained_iterations() {
    let mut allocator = ConnectionIdAllocator::with_capacity(3);

    let id = allocator.register().unwrap();
    allocator.deregister(id).unwrap();

    // Fresh identifiers are used while the freed one is quarantined.
    assert_eq!(allocator.register().unwrap(), ConnectionId(1));
    assert_eq!(allocator.register().unwrap(), ConnectionId(2));

    // The space is full; nothing can be handed out during the grace period.
    assert_eq!(allocator.register(), Err(AllocError::Exhausted));
    allocator.turn();
    assert_eq!(allocator.register(), Err(AllocError::Exhausted));

    // Two drained iterations later the identifier is reusable again.
    allocator.turn();
    assert_eq!(allocator.register().unwrap(), id);
}

#[test]
fn reuse_never_collides_with_a_live_connection() {
    let mut allocator = ConnectionIdAllocator::with_capacity(4);

    let keep = allocator.register().unwrap();
    let drop_first = allocator.register().unwrap();
    let drop_second = allocator.register().unwrap();

    allocator.deregister(drop_first).unwrap();
    allocator.turn();
    allocator.deregister(drop_second).unwrap();
    allocator.turn();
    allocator.turn();

    // Both freed identifiers come back, never the one still in use.
    let reused_one = allocator.register().unwrap();
    let reused_two = allocator.register().unwrap();
    let mut reused = [reused_one, reused_two];
    reused.sort();
    assert_eq!(reused, [drop_first, drop_second]);
    assert_ne!(reused_one, keep);
    assert_ne!(reused_two, keep);
    assert_eq!(allocator.active(), 3);
}

#[test]
fn full_space_is_reported_not_wrapped() {
    let mut allocator = ConnectionIdAllocator::with_capacity(2);

    let first = allocator.register().unwrap();
    let second = allocator.register().unwrap();
    assert_eq!(allocator.register(), Err(AllocError::Exhausted));

    allocator.deregister(first).unwrap();
    // Still exhausted while the identifier sits in quarantine.
    assert_eq!(allocator.register(), Err(AllocError::Exhausted));

    allocator.turn();
    allocator.turn();
    assert_eq!(allocator.register().unwrap(), first);
    assert_eq!(allocator.register(), Err(AllocError::Exhausted));

    allocator.deregister(second).unwrap();
    assert_eq!(allocator.deregister(second), Err(AllocError::InvalidId));
}

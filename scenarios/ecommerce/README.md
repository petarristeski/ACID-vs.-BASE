# E-commerce Scenarios

- Steady: baseline concurrent checkouts with mixed payment successes/failures.
- Payments: synchronized bursts focusing on payment failures and compensation vs rollback.
- Concurrent Orders: many buyers competing for the same hot SKU at once; emphasizes lock/LWT/atomic-guard behavior under contention.

Consistency goals
- Avoid overselling under contention.
- Highlight differences between ACID transactions (Postgres) and BASE approaches (MongoDB/Cassandra) during failures.

See schemas in `databases/*/ecommerce.*`.

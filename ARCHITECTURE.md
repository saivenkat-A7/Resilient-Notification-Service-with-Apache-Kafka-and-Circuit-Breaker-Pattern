# Architecture

## Components

| Component | Responsibility |
|---|---|
| `NotificationController` | `POST /api/notifications/send`: validates input, delegates to the producer, returns `202` |
| `NotificationProducerService` | Builds `NotificationEvent` (UUID, timestamp), publishes to `notification_events` keyed by `userId`, waits for broker ack |
| `NotificationConsumerService` | `@KafkaListener`; idempotency check, calls the client, records `SENT` / `DEFERRED` |
| `UserProfileServiceClient` | `RestTemplate` call protected by `@CircuitBreaker`; fallback returns an empty profile |
| `MockUserProfileServiceController` | Simulated external service with `fail` / `delay` controls |
| `NotificationStore` | Bounded in-memory store of records and processed event ids |
| `KafkaConfig` | Topic declaration and consumer error handler (exponential back-off) |
| `CircuitBreakerEventLogger` | Logs breaker state transitions |

## Sequence: happy path

```mermaid
sequenceDiagram
    participant C as Client
    participant API as NotificationController
    participant P as ProducerService
    participant K as Kafka
    participant L as ConsumerService
    participant CB as UserProfileServiceClient (CB)
    participant U as User Profile Service (mock)

    C->>API: POST /api/notifications/send
    API->>P: publishNotificationEvent
    P->>K: send(key=userId, NotificationEvent)
    K-->>P: ack
    API-->>C: 202 Accepted
    K->>L: poll NotificationEvent
    L->>CB: getUserProfile(userId)
    CB->>U: GET /mock-users/{userId}
    U-->>CB: 200 UserProfile
    CB-->>L: UserProfile
    L->>L: log "NOTIFICATION SENT" + store SENT
```

## Sequence: User Profile Service down

```mermaid
sequenceDiagram
    participant L as ConsumerService
    participant CB as UserProfileServiceClient (CB)
    participant U as User Profile Service (mock)

    Note over CB: CLOSED - failures counted
    L->>CB: getUserProfile
    CB->>U: GET (HTTP 500 / timeout)
    U--xCB: error
    CB-->>L: fallback (empty profile) -> DEFERRED
    Note over CB: failure rate >= 50% -> OPEN
    L->>CB: getUserProfile
    CB-->>L: CallNotPermittedException -> fallback, no network call
    Note over CB: after wait duration -> HALF_OPEN
    L->>CB: probe calls
    CB->>U: GET
    U-->>CB: 200
    Note over CB: probes succeed -> CLOSED
```

## Circuit breaker state machine

```mermaid
stateDiagram-v2
    [*] --> CLOSED
    CLOSED --> OPEN: failure rate >= threshold (after min. calls)
    OPEN --> HALF_OPEN: wait duration elapsed (automatic)
    HALF_OPEN --> CLOSED: probe calls succeed
    HALF_OPEN --> OPEN: probe calls fail
```

## Delivery guarantees and failure handling

| Situation | Behaviour |
|---|---|
| Kafka down while publishing | Producer retries (`acks=all`, idempotent). After the delivery timeout the API answers `503`; nothing is accepted silently |
| Kafka down while consuming | Client reconnects with back-off; the listener keeps polling and resumes automatically; offsets are only committed after processing |
| Consumer redelivery | Processed `eventId`s are remembered → duplicates are skipped (idempotent consumer) |
| Poison message / listener exception | Retried with exponential back-off (3 attempts by default), then logged and skipped |
| Undeserialisable payload | `ErrorHandlingDeserializer` turns it into a handled error instead of an endless poll loop |
| User Profile Service failing/slow | Timeout + breaker + fallback → `DEFERRED` record; REST API unaffected |

## Known limitations / next steps

* The notification store is in memory (as allowed by the task). A persistent outbox/DB would survive restarts.
* `DEFERRED` notifications are recorded but not re-driven. A scheduled job or a retry topic (`notification_events.retry`)
  with a dead-letter topic would complete the pattern.
* The consumer idempotency cache is per instance; with several instances use a shared store (e.g. Redis/DB unique key).
* Single broker / single partition defaults are for local development only. Increase partitions and replication for production.

# TaskForge: Data Model & Schema Specification

## 1. Relational Entity Diagram

```mermaid
erDiagram
    TASKS ||--o{ TASK_ATTEMPTS : "has attempts"
    TASKS ||--o| TASK_RESULTS : "produces result"
    TASKS ||--o{ TASK_EVENTS : "records audit events"
    TASKS ||--o{ OUTBOX_EVENTS : "emits outbox events"

    TASKS {
        string id PK
        string task_name
        string task_type
        json payload
        string status
        int priority
        string idempotency_key UK
        int timeout_seconds
        int max_retries
        int retry_count
        datetime scheduled_at
        datetime started_at
        datetime completed_at
        string lease_token
        datetime lease_expires_at
        datetime created_at
        datetime updated_at
    }

    TASK_ATTEMPTS {
        int id PK
        string task_id FK
        int attempt_number
        string worker_id
        string status
        text error_message
        text error_traceback
        datetime started_at
        datetime completed_at
        float duration_ms
    }

    TASK_RESULTS {
        int id PK
        string task_id FK
        json return_value
        json execution_metadata
        datetime created_at
    }

    TASK_EVENTS {
        int id PK
        string task_id FK
        string event_type
        string from_state
        string to_state
        json metadata_json
        datetime created_at
    }

    OUTBOX_EVENTS {
        string id PK
        string aggregate_type
        string aggregate_id
        string event_type
        json payload
        string status
        int retry_count
        text error_message
        datetime created_at
        datetime published_at
    }
```

## 2. Indexing Strategy

1. **`tasks`**:
   - `ix_tasks_status`: Fast state filtering.
   - `ix_tasks_priority`: Priority queue ordering.
   - `ix_tasks_idempotency_key`: Unique lookup during deduplication.
   - `ix_tasks_status_priority`: Composite index for pending work queries.
   - `ix_tasks_lease_recovery`: Composite index on `(status, lease_expires_at)` for sub-millisecond reaper scans.

2. **`outbox_events`**:
   - `ix_outbox_status_created`: Composite index on `(status, created_at)` for the outbox publisher polling query.

3. **`task_attempts`**:
   - `ix_task_attempts_task_attempt`: Composite index on `(task_id, attempt_number)` for audit chronology.


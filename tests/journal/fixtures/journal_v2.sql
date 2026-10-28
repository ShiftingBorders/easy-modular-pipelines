-- Fixed pre-compaction storage format; independent of future schema constants.
CREATE TABLE events (
    cursor INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    producer_instance_id TEXT NOT NULL,
    sequence_number INTEGER NOT NULL CHECK (sequence_number > 0),
    event_json TEXT NOT NULL,
    UNIQUE (producer_instance_id, sequence_number)
);
CREATE TABLE journal_info (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    journal_id TEXT NOT NULL,
    generation TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE command_results (
    request_id TEXT PRIMARY KEY NOT NULL,
    identity_json TEXT NOT NULL,
    runner_event_id TEXT REFERENCES events(event_id),
    participant_event_id TEXT REFERENCES events(event_id),
    runner_observation_json TEXT,
    participant_observation_json TEXT,
    effective_event_id TEXT NOT NULL REFERENCES events(event_id),
    effective_author TEXT NOT NULL CHECK (effective_author IN ('runner', 'participant')),
    CHECK (runner_event_id IS NOT NULL OR participant_event_id IS NOT NULL)
);
CREATE TABLE journal_changes (
    change_cursor INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL REFERENCES events(event_id),
    change_json TEXT NOT NULL
);
CREATE TABLE journal_restorations (
    restoration_id TEXT PRIMARY KEY NOT NULL,
    parameters_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);
PRAGMA application_id=1162694732;
PRAGMA user_version=2;
INSERT INTO journal_info VALUES (1, '11111111111111111111111111111111', '22222222222222222222222222222222', '2026-10-16T00:00:00.000000+00:00');

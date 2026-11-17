CREATE TABLE matrix_db.notification_subscriptions (
    id uuid PRIMARY KEY,
    owner text NOT NULL,
    credential_id text NOT NULL,
    delegated_owner text,
    dataset_id text NOT NULL REFERENCES matrix_db.datasets(id),
    revision integer NOT NULL DEFAULT 1,
    definition jsonb NOT NULL,
    active boolean NOT NULL,
    deleted boolean NOT NULL DEFAULT false,
    last_sequence bigint NOT NULL,
    last_polled_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX notification_subscriptions_due_idx ON matrix_db.notification_subscriptions(last_polled_at,id) WHERE active AND NOT deleted;
CREATE INDEX notification_subscriptions_owner_idx ON matrix_db.notification_subscriptions(owner,id);
CREATE TABLE matrix_db.notification_subscription_versions (
    subscription_id uuid NOT NULL REFERENCES matrix_db.notification_subscriptions(id),
    revision integer NOT NULL,
    definition jsonb NOT NULL,
    action text NOT NULL CHECK(action IN ('created','updated','deleted','authorization_revoked')),
    at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(subscription_id,revision)
);
CREATE TABLE matrix_db.notification_deliveries (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    owner text NOT NULL,
    dataset_id text NOT NULL REFERENCES matrix_db.datasets(id),
    event_sequence bigint NOT NULL REFERENCES matrix_db.quality_events(sequence),
    payload jsonb NOT NULL,
    read_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(owner,event_sequence)
);
CREATE INDEX notification_deliveries_owner_idx ON matrix_db.notification_deliveries(owner,id DESC);
CREATE TABLE matrix_db.notification_matches (
    delivery_id bigint NOT NULL REFERENCES matrix_db.notification_deliveries(id),
    subscription_id uuid NOT NULL REFERENCES matrix_db.notification_subscriptions(id),
    PRIMARY KEY(delivery_id,subscription_id)
);
CREATE TRIGGER immutable_subscription_versions BEFORE UPDATE OR DELETE ON matrix_db.notification_subscription_versions FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();
CREATE TRIGGER immutable_notification_matches BEFORE UPDATE OR DELETE ON matrix_db.notification_matches FOR EACH ROW EXECUTE FUNCTION matrix_db.reject_revision_edit();

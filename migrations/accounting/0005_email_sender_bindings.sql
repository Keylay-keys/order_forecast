-- Owner-approved, exact forwarding identities. Never authorize a provider domain.
CREATE UNIQUE INDEX IF NOT EXISTS accounting_email_originals_address_message_idx
    ON accounting_email_originals (address, message_id);

CREATE TABLE IF NOT EXISTS accounting_email_sender_bindings (
    address text NOT NULL REFERENCES accounting_email_addresses(address),
    verified_forwarder_email text NOT NULL CHECK (
        length(verified_forwarder_email) BETWEEN 3 AND 254
        AND verified_forwarder_email = lower(verified_forwarder_email)
    ),
    sender_email text NOT NULL CHECK (
        length(sender_email) BETWEEN 3 AND 254
        AND sender_email = lower(sender_email)
        AND sender_email ~ '^[^@[:space:]]+@[^@[:space:]]+[.][^@[:space:]]+$'
        AND sender_email !~ '@gruma[.]com$'
    ),
    provider text NOT NULL CHECK (provider IN ('outlook', 'icloud', 'forwarding')),
    source_message_id uuid NOT NULL UNIQUE,
    confirmed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (address, verified_forwarder_email, sender_email),
    FOREIGN KEY (address, source_message_id)
        REFERENCES accounting_email_originals(address, message_id),
    CHECK (provider <> 'outlook' OR sender_email ~ '^outlook_[0-9a-f]{16,64}@outlook[.]com$')
);

REVOKE ALL ON accounting_email_sender_bindings FROM PUBLIC;

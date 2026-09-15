-- Personal Web Portal SAP-row ordering preference.
--
-- This additive migration creates no order rows, performs no Firebase access,
-- and never deletes or rewrites legacy order sapOrder fields.

CREATE TABLE IF NOT EXISTS portal_sap_order_preferences (
    firebase_uid TEXT NOT NULL,
    route_number TEXT NOT NULL,
    sap_order JSONB NOT NULL DEFAULT '[]'::jsonb,
    revision BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (firebase_uid, route_number),
    CONSTRAINT portal_sap_order_uid_present
        CHECK (btrim(firebase_uid) <> '' AND length(firebase_uid) <= 128),
    CONSTRAINT portal_sap_order_route_valid
        CHECK (route_number ~ '^[0-9]{1,10}$'),
    CONSTRAINT portal_sap_order_array
        CHECK (jsonb_typeof(sap_order) = 'array'),
    CONSTRAINT portal_sap_order_string_elements
        CHECK (NOT jsonb_path_exists(
            sap_order,
            '$[*] ? (@.type() != "string")'
        )),
    CONSTRAINT portal_sap_order_bounded
        CHECK (jsonb_array_length(sap_order) <= 2000),
    CONSTRAINT portal_sap_order_revision_positive
        CHECK (revision > 0)
);

REVOKE ALL ON TABLE portal_sap_order_preferences FROM PUBLIC;

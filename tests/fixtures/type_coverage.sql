-- type_coverage.sql
-- Creates a table with every PostgreSQL type that omnihedron handles,
-- inserts sample data, and verifies the serialization round-trip.

-- Use the same schema as the test fixture DB.
SET search_path TO app;

-- Custom enum for testing the dynamic-OID fallback.
DO $$ BEGIN
    CREATE TYPE app.test_status AS ENUM ('active', 'inactive', 'pending');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS app.type_coverage (
    -- Primary key (text, required by SubQuery convention)
    id              TEXT PRIMARY KEY,

    -- Boolean
    col_bool        BOOLEAN,

    -- Integer types
    col_int2        SMALLINT,
    col_int4        INTEGER,
    col_int8        BIGINT,

    -- Float types
    col_float4      REAL,
    col_float8      DOUBLE PRECISION,

    -- Numeric / decimal
    col_numeric     NUMERIC,

    -- String types
    col_text        TEXT,
    col_varchar     VARCHAR(255),
    col_bpchar      CHAR(10),

    -- Binary
    col_bytea       BYTEA,

    -- JSON
    col_json        JSON,
    col_jsonb       JSONB,

    -- Date / time
    col_timestamp   TIMESTAMP WITHOUT TIME ZONE,
    col_timestamptz TIMESTAMP WITH TIME ZONE,
    col_date        DATE,
    col_time        TIME WITHOUT TIME ZONE,

    -- Interval (text-mode fallback)
    col_interval    INTERVAL,

    -- UUID
    col_uuid        UUID,

    -- Bit strings
    col_bit         BIT(8),
    col_varbit      BIT VARYING(16),

    -- Network types
    col_inet        INET,
    col_cidr        CIDR,
    col_macaddr     MACADDR,

    -- Geometric types
    col_point       POINT,
    col_box         BOX,

    -- Custom enum
    col_enum        app.test_status,

    -- OID
    col_oid         OID,

    -- Array types
    col_bool_arr    BOOLEAN[],
    col_int2_arr    SMALLINT[],
    col_int4_arr    INTEGER[],
    col_int8_arr    BIGINT[],
    col_float4_arr  REAL[],
    col_float8_arr  DOUBLE PRECISION[],
    col_text_arr    TEXT[],
    col_numeric_arr NUMERIC[],
    col_uuid_arr    UUID[],
    col_timestamp_arr TIMESTAMP WITHOUT TIME ZONE[],
    col_timestamptz_arr TIMESTAMP WITH TIME ZONE[],
    col_date_arr    DATE[],
    col_time_arr    TIME WITHOUT TIME ZONE[],
    col_jsonb_arr   JSONB[],
    col_bytea_arr   BYTEA[],
    col_inet_arr    INET[],
    col_macaddr_arr MACADDR[],

    -- SubQuery internal columns
    _id             UUID NOT NULL DEFAULT gen_random_uuid(),
    _block_range    INT8RANGE NOT NULL DEFAULT '[1,)'
);

-- Insert test rows with known values.
INSERT INTO app.type_coverage (
    id,
    col_bool, col_int2, col_int4, col_int8,
    col_float4, col_float8, col_numeric,
    col_text, col_varchar, col_bpchar,
    col_bytea,
    col_json, col_jsonb,
    col_timestamp, col_timestamptz, col_date, col_time,
    col_interval,
    col_uuid,
    col_bit, col_varbit,
    col_inet, col_cidr, col_macaddr,
    col_point, col_box,
    col_enum, col_oid,
    col_bool_arr, col_int2_arr, col_int4_arr, col_int8_arr,
    col_float4_arr, col_float8_arr,
    col_text_arr, col_numeric_arr, col_uuid_arr,
    col_timestamp_arr, col_timestamptz_arr,
    col_date_arr, col_time_arr,
    col_jsonb_arr, col_bytea_arr,
    col_inet_arr, col_macaddr_arr
) VALUES (
    'type-test-1',
    true, 32767, 2147483647, 9223372036854775807,
    3.14, 2.718281828459045, 99999999999999.123456,
    'hello world', 'varchar val', 'bpchar    ',
    E'\\xDEADBEEF',
    '{"key": "value"}', '{"nested": {"num": 42}}',
    '2024-01-15 10:30:00', '2024-01-15 10:30:00+00', '2024-01-15', '14:30:00',
    '1 year 2 months 3 days 04:05:06',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    B'10101010', B'1100110011',
    '192.168.1.1', '10.0.0.0/8', '08:00:2b:01:02:03',
    '(1.5, 2.5)', '((0,0),(3,4))',
    'active', 12345,
    ARRAY[true, false, true], ARRAY[1::smallint, 2::smallint, 3::smallint],
    ARRAY[10, 20, 30], ARRAY[100::bigint, 200::bigint],
    ARRAY[1.1::real, 2.2::real], ARRAY[3.3::double precision, 4.4::double precision],
    ARRAY['foo', 'bar', 'baz'], ARRAY[1.23::numeric, 4.56::numeric],
    ARRAY['a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid],
    ARRAY['2024-01-15 10:30:00'::timestamp, '2024-06-15 12:00:00'::timestamp],
    ARRAY['2024-01-15 10:30:00+00'::timestamptz],
    ARRAY['2024-01-15'::date, '2024-06-15'::date],
    ARRAY['14:30:00'::time, '08:00:00'::time],
    ARRAY['{"a":1}'::jsonb, '{"b":2}'::jsonb],
    ARRAY[E'\\xCAFE'::bytea, E'\\xBABE'::bytea],
    ARRAY['192.168.1.1'::inet, '10.0.0.1'::inet],
    ARRAY['08:00:2b:01:02:03'::macaddr]
),
(
    'type-test-null',
    NULL, NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL,
    NULL, NULL,
    NULL, NULL, NULL, NULL,
    NULL,
    NULL,
    NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL,
    NULL, NULL,
    NULL, NULL, NULL, NULL,
    NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL,
    NULL, NULL,
    NULL, NULL,
    NULL, NULL
)
ON CONFLICT (id) DO NOTHING;

-- Migration: Create a new guardian table in the ClickHouse database
CREATE TABLE IF NOT EXISTS clickhouse.guardian (
    -- Primary Key
    guardianId UUID,

    -- General Information
    schoolId UUID,
    firstName String,
    lastName String,
    firstNameNative Nullable(String),
    lastNameNative Nullable(String),
    gender Nullable(String),
    dob Nullable(Date),
    phone Nullable(String),
    email Nullable(String),
    address Nullable(String),
    photo Nullable(String),
    createdAt DateTime,
    updatedAt DateTime,
    archiveStatus Int8 DEFAULT 0,
    userName Nullable(String)

) ENGINE = MergeTree()
PARTITION BY schoolId      -- Partition by schoolId
ORDER BY (schoolId, guardianId);   -- Primary key is schoolId and guardianId

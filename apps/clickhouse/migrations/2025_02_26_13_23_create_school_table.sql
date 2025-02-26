-- Migration: Create a new school table in the ClickHouse database
CREATE TABLE IF NOT EXISTS clickhouse.school (
    schoolId UUID,
    name String,
    code Nullable(String),
    url String,
    email Nullable(String),
    address Nullable(String),
    logo Nullable(String),
    status Nullable(String),
    province Nullable(String),
    country Nullable(String),
    createdAt DateTime,
    updatedAt DateTime
) ENGINE = MergeTree()
ORDER BY schoolId;

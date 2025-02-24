-- Migration: Create a new month_score table in the clickhouse database
CREATE TABLE IF NOT EXISTS clickhouse.month_score (
    -- Score
    score Nullable(Float32),
    maxScore Nullable(Float32),
    gpa Nullable(String),
    rank Nullable(UInt32),

    month String,
    year String,
    updatedAt DateTime,
    createdAt DateTime,

    -- Foreign keys
    schoolId UUID,
    studentUniqueKey String,
    campusId Nullable(UUID),
    groupStructureId Nullable(UUID),
    structureId Nullable(UUID),
    structureRecordId Nullable(UUID),

) ENGINE = MergeTree()
PARTITION BY (schoolId, studentUniqueKey)      -- Partition by schoolId and student's uniqueKey
ORDER BY (schoolId, studentUniqueKey, month, year);   -- Primary key

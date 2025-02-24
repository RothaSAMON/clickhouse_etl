-- Migration: Create a new academic table in the clickhouse database
CREATE TABLE IF NOT EXISTS clickhouse.academic (
    -- Score
    score Nullable(Float32),
    maxScore Nullable(Float32),
    gpa Nullable(String),
    rank Nullable(UInt32),

    -- Attendance
    P UInt32 DEFAULT 0,
    AP UInt32 DEFAULT 0,
    A UInt32 DEFAULT 0,
    L UInt32 DEFAULT 0,
    other UInt32 DEFAULT 0,

    -- Academic information
    name String,
    nameNative Nullable(String),
    remark Nullable(String),
    idCard Nullable(String),
    status String DEFAULT 'start',
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
PARTITION BY schoolId      -- Partition by schoolId
ORDER BY (schoolId, studentUniqueKey);   -- Primary key

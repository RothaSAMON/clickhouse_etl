-- Migration: Create a new date_attendance table in the clickhouse database
CREATE TABLE IF NOT EXISTS clickhouse.date_attendance (
    -- Attendance
    P UInt32 DEFAULT 0,
    AP UInt32 DEFAULT 0,
    A UInt32 DEFAULT 0,
    L UInt32 DEFAULT 0,
    other UInt32 DEFAULT 0,

    date Date,
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
PARTITION BY (schoolId, studentUniqueKey)       -- Partition by schoolId
ORDER BY (schoolId, studentUniqueKey, date);    -- Primary key

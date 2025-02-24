-- Migration: Create a new student table in the clickhouse database
CREATE TABLE IF NOT EXISTS clickhouse.student (
    -- Primary Key
    uniqueKey String,

    -- General Information
    idCard Nullable(String),
    firstName String,
    lastName String,
    firstNameNative Nullable(String),
    lastNameNative Nullable(String),
    gender Nullable(String),
    dob Nullable(Date),
    program Nullable(String),
    remark Nullable(String),
    phone Nullable(String),
    email Nullable(String),
    profile Nullable(String),       -- to keep addition dynamic fields
    noAttendance Bool DEFAULT false,
    status String DEFAULT 'start',
    finalAcademicStatus String DEFAULT 'start',
    enrolledAt Nullable(DateTime),
    archiveStatus Int8 DEFAULT 0,
    position Nullable(String),
    finishDate Nullable(DateTime),
    finishReason Nullable(String),
    updatedAt DateTime,
    createdAt DateTime,
    
    -- Foreign keys
    schoolId UUID,

) ENGINE = MergeTree()
PARTITION BY schoolId      -- Partition by schoolId
ORDER BY (schoolId, uniqueKey);   -- Primary key is schoolId and id

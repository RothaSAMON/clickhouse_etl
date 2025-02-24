-- Migration: Create a new subject table in the clickhouse database
CREATE TABLE IF NOT EXISTS clickhouse.subject (
    -- Link to class
    schoolId UUID,
    campusId Nullable(UUID),
    groupStructureId Nullable(UUID),
    structureRecordId Nullable(UUID),
    -- Subject information
    subjectId UUID,
    curriculumId Nullable(UUID),
    name String,
    nameNative Nullable(String),
    description Nullable(String),
    credit Float32 DEFAULT 1.0,
    code Nullable(String),
    practiceHour Int8 DEFAULT 0,
    theoryHour Int8 DEFAULT 0,
    fieldHour Int8 DEFAULT 0,
    totalHour Int8 DEFAULT 0,
    photo Nullable(String),
    createdAt DateTime,
    updatedAt DateTime,
    archiveStatus Int8 DEFAULT 0,
    lmsCourseId Nullable(UUID),
    -- Teacher information
    teachers Nullable(String)
) ENGINE = MergeTree()
PARTITION BY schoolId               -- Partition by schoolId
ORDER BY (schoolId, subjectId);     -- Primary key is schoolId and subjectId

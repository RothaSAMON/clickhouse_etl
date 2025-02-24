-- Migration: Create a new academic table in the clickhouse database
CREATE TABLE IF NOT EXISTS clickhouse.subject_score (
    -- Score
    score Nullable(Float32),
    maxScore Nullable(Float32),
    gpa Nullable(String),
    rank Nullable(UInt32),

    date Nullable(Date),
    month Nullable(String),
    year Nullable(String),
    createdAt DateTime,

    -- Foreign keys
    schoolId UUID,
    evaluationId UUID,
    campusId Nullable(UUID),
    groupStructureId Nullable(UUID),
    structurePath Nullable(UUID),
    templateId Nullable(UUID),
    configGroupId Nullable(UUID),
    parentId Nullable(UUID),
    studentId UUID,
    subjectId UUID,

) ENGINE = MergeTree()
PARTITION BY (schoolId, subjectId)      -- Partition by schoolId
ORDER BY (schoolId, subjectId, studentId);   -- Primary key

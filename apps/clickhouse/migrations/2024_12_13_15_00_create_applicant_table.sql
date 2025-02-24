CREATE TABLE IF NOT EXISTS clickhouse.applicant (
    applicantId UUID,
    userKey Nullable(String),
    idCard Nullable(String),
    status String DEFAULT 'pending',
    enrollToSubject String,
    enrollToDetail String,
    lastProfile String,
    applicantStatus String DEFAULT 'pending',
    source Nullable(String),
    admissionFlow String DEFAULT 'default',
    confirmTarget Nullable(String),
    waitApplicantConfirm Nullable(String),
    updatedAt DateTime,
    createdAt DateTime,
    toNotifyApplicant Bool DEFAULT false,

    -- Foreign keys
    schoolId UUID,
    userId Nullable(UUID),
    enrollToId UUID
) ENGINE = MergeTree()
PARTITION BY schoolId               -- Partition by schoolId
ORDER BY (schoolId, applicantId);   -- Primary key is schoolId and applicantId

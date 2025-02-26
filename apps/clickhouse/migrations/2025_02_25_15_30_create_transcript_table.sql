CREATE TABLE IF NOT EXISTS student_transcript_staging (
    -- School & Campus
    schoolId UUID,
    campusId UUID,

    -- Structure / Class Info
    structureRecordId UUID,
    structureRecordName String,
    groupStructureId UUID,
    structurePath String,

    -- Student Info
    studentId UUID,
    studentFirstName String,
    studentLastName String,
    studentFirstNameNative String,
    studentLastNameNative String,
    idCard String,
    dob Nullable(Date),
    gender String,

    -- Aggregated Subject Data
    subjectDetails Array(
        Tuple(
            subjectEvaluationId UUID,
            subjectName String,
            subjectNameNative String,
            code String,
            credit Decimal(5,2),
            score Float64,
            percentage Float64,
            grade String,
            meaning String,
            gpa Float64
        )
    ),
    
    -- Totals
    totalCredits Decimal(7,2),
    totalGPA Float64,
    subjectCount UInt32,
    
    -- Additional Info
    scorerId UUID,
    markedAt Nullable(DateTime),
    
    -- Timestamps
    createdAt DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (groupStructureId, structureRecordId, studentId);
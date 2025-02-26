CREATE TABLE IF NOT EXISTS student_month_subject_score_staging (
    -- School & Campus
    schoolId UUID,
    campusId UUID,

    -- Structure / Class Info
    structureRecordId UUID,
    structureRecordName      String,
    groupStructureId  UUID,
    structurePath      String,

    -- Student Info
    studentId          UUID,
    studentFirstName  String,
    studentLastName   String,
    studentFirstNameNative String,
    studentLastNameNative String,
    idCard             String,
    dob         Nullable(Date),
    gender String,
    -- if needed
    -- ... any other student fields you want

    -- Month Evaluation (Parent)
    monthEvaluationId UUID,
    monthName          String,
    monthStartDate    Nullable(DateTime), -- from attendanceColumn.startDate
    monthEndDate      Nullable(DateTime), -- from attendanceColumn.endDate

    -- Subject Evaluation (Child)
    subjectEvaluationId UUID,
    subjectName          String,
    subjectNameNative   String,
    subjectMaxScore     Float64,  -- e.g. "maxScore"
    coe     Int32,
    code    String,
    credit  Decimal(5,2),

    -- ... any other subject fields like "coe", "gradingMode" if needed

    -- Score Info
    score       Float64,    -- or Decimal(5,2)
    percentage       Float64,
    grade   String,
    meaning String,
    gpa     Float64,
    scoreSource String,
    customEvaluationCount Int32,
    customEvaluations String,
    scorerId   UUID,
    markedAt   Nullable(DateTime),   -- from "markedAt"
    description String,     -- if you need the "description"

    -- Timestamps
    createdAt DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (monthEvaluationId, subjectEvaluationId, studentId);

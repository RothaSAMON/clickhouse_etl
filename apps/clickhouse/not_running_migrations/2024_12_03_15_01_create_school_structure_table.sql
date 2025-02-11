-- Migration: Create a new school table in the clickhouse database
CREATE TABLE IF NOT EXISTS clickhouse.school (
    schoolId UUID,
    name String,
    nameNative Nullable(String),
    code Nullable(String),
    url String,
    email Nullable(String),
    phone Nullable(String),
    schoolType String DEFAULT 'university',
    address Nullable(String),
    logo Nullable(String),
    status Nullable(String),
    mission Nullable(String),
    vision Nullable(String),
    standOutMessages Nullable(String),
    facilities Nullable(String),
    google_map Nullable(String),
    facebook Nullable(String),
    instagram Nullable(String),
    twitter Nullable(String),
    images Nullable(String),
    categories Nullable(String),
    videos Nullable(String),
    tutionFees Nullable(String),
    programs Nullable(String),
    scholarshipOverview Nullable(String),
    exchange Nullable(String),
    website Nullable(String),
    onlineAdmission Bool DEFAULT true,
    requirement Nullable(String),
    field Nullable(String),
    overview Nullable(String),
    overviewKhmer Nullable(String),
    tuition_fee Nullable(String),
    tuition_fee_khmer Nullable(String),
    program_english Nullable(String),
    program_khmer Nullable(String),
    scholarshipsKhmer Nullable(String),
    exchangePrograms Nullable(String),
    exchangeProgramsKhmer Nullable(String),
    archiveStatus Int8 DEFAULT 0,
    verified Bool DEFAULT false,
    featureImage Nullable(String),
    province Nullable(String),
    country Nullable(String),
    minPrice Nullable(String),
    maxPrice Nullable(String),
    isPublic String DEFAULT 'no',
    gallery Nullable(String),
    scholarships Nullable(String),
    missionKhmer Nullable(String),
    visionKhmer Nullable(String),
    standOutMessagesKhmer Nullable(String),
    createdAt DateTime,
    updatedAt DateTime
) ENGINE = MergeTree()
ORDER BY schoolId;

CREATE TABLE IF NOT EXISTS clickhouse.campus (
    schoolId UUID,
    campusId UUID,
    name String,
    nameNative Nullable(String),
    code Nullable(String),
    phone Nullable(String),
    email Nullable(String),
    address Nullable(String),
    isHq Bool DEFAULT false,
    rooomsBuildings Nullable(String),
    archiveStatus Int8 DEFAULT 0,
    status String DEFAULT 'progress',
    photo Nullable(String),
    map Nullable(String),
    responsibleBy Nullable(String),
    structureType Nullable(String),
    createdAt DateTime,
    updatedAt DateTime
) ENGINE = MergeTree()
PARTITION BY schoolId
ORDER BY campusId;

CREATE TABLE IF NOT EXISTS clickhouse.group_structure (
    schoolId UUID,
    campusId UUID,
    groupStructureId UUID,
    name String,
    nameNative Nullable(String),
    code Nullable(String),
    description Nullable(String),
    archiveStatus Int8 DEFAULT 0,
    status String DEFAULT 'progress',
    responsibleBy Nullable(String),
    structureType Nullable(String),
    createdAt DateTime,
    updatedAt DateTime
) ENGINE = MergeTree()
PARTITION BY schoolId
ORDER BY groupStructureId;

CREATE TABLE IF NOT EXISTS clickhouse.structure_record (
    schoolId UUID,
    campusId UUID,
    groupStructureId UUID,
    structureRecordId UUID,
    name String,
    nameNative Nullable(String),
    code Nullable(String),
    description Nullable(String),
    enrollableCategory Nullable(String),
    photo Nullable(String),
    recordType Nullable(String),
    qty Nullable(String),
    tags Nullable(String),
    statistic Nullable(String),
    countStructure Nullable(String),
    isPromoted Bool DEFAULT false,
    isFeatured Bool DEFAULT false,
    isPublic Bool DEFAULT false,
    isOpen Bool DEFAULT false,
    startDate Nullable(Date),
    endDate Nullable(Date),
    extra Nullable(String),
    phone Nullable(String),
    structurePath Nullable(String),
    archiveStatus Int8 DEFAULT 0,
    status String DEFAULT 'progress',
    share Nullable(String),
    responsibleBy Nullable(String),
    structure String,                   -- Structure's name, coming from table structure
    structureType Nullable(String),     -- Structure's structureType, coming from table structure
    createdAt DateTime,
    updatedAt DateTime
) ENGINE = MergeTree()
PARTITION BY schoolId
ORDER BY structureRecordId;
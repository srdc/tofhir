CREATE TABLE patients
(
    pid              varchar(4) NOT NULL,
    gender           varchar(8) NULL,
    birthDate        date       NULL,
    deceasedDateTime timestamp  NULL,
    homePostalCode   varchar(8) NULL
);

CREATE TABLE otherobservations
(
    pid         varchar(255) NOT NULL,
    "time"      timestamp    NULL,
    encounterId varchar(255) NULL,
    code        varchar(255) NULL,
    "value"     varchar(255) NULL
);

CREATE TABLE care_site
(
    care_site_id                  int4         NOT NULL,
    care_site_name                varchar(255) NULL,
    place_of_service_concept_id   int4         NULL,
    location_id                   int4         NULL,
    care_site_source_value        varchar(50)  NULL,
    place_of_service_source_value varchar(50)  NULL
);

CREATE TABLE location
(
    location_id           int4        NOT NULL,
    address_1             varchar(50) NULL,
    address_2             varchar(50) NULL,
    city                  varchar(50) NULL,
    state                 varchar(2)  NULL,
    zip                   varchar(9)  NULL,
    county                varchar(20) NULL,
    location_source_value varchar(50) NULL,
    country_concept_id    int4        NULL,
    country_source_value  varchar(80) NULL,
    latitude              numeric     NULL,
    longitude             numeric     NULL
);

CREATE TABLE procedure_occurrence
(
    procedure_occurrence_id     int4        NOT NULL,
    person_id                   int4        NOT NULL,
    procedure_concept_id        int4        NOT NULL,
    procedure_date              date        NOT NULL,
    procedure_datetime          timestamp   NULL,
    procedure_end_date          date        NULL,
    procedure_end_datetime      timestamp   NULL,
    procedure_type_concept_id   int4        NOT NULL,
    modifier_concept_id         int4        NULL,
    quantity                    int4        NULL,
    provider_id                 int4        NULL,
    visit_occurrence_id         int4        NULL,
    visit_detail_id             int4        NULL,
    procedure_source_value      varchar(50) NULL,
    procedure_source_concept_id int4        NULL,
    modifier_source_value       varchar(50) NULL
);


CREATE TABLE concept
(
    concept_id       int4         NOT NULL,
    concept_name     varchar(255) NOT NULL,
    domain_id        varchar(20)  NOT NULL,
    vocabulary_id    varchar(20)  NOT NULL,
    concept_class_id varchar(20)  NOT NULL,
    standard_concept varchar(1)   NULL,
    concept_code     varchar(50)  NOT NULL,
    valid_start_date date         NOT NULL,
    valid_end_date   date         NOT NULL,
    invalid_reason   varchar(1)   NULL
);


INSERT INTO patients
(pid, gender, birthDate, deceasedDateTime, homePostalCode)
VALUES ('p1', 'male', TO_TIMESTAMP('2000-05-10', 'YYYY-MM-DD'),
        TO_TIMESTAMP('2017-03-10 15:11:23+02:00', 'YYYY-MM-DD HH24:MI:SS+TZH:TZM'), 'G02547'),
       ('p2', 'male', TO_TIMESTAMP('1985-05-08', 'YYYY-MM-DD'), TO_TIMESTAMP('2017-03-10', 'YYYY-MM-DD'), 'G02547'),
       ('p5', 'male', TO_TIMESTAMP('1965-10-01', 'YYYY-MM-DD'), TO_TIMESTAMP('2019-04-21', 'YYYY-MM-DD'), 'G02547'),
       ('p4', 'male', TO_TIMESTAMP('1999-06-05', 'YYYY-MM-DD'), TO_TIMESTAMP('2017-03-10', 'YYYY-MM-DD'), 'H10564'),
       ('p7', 'female', TO_TIMESTAMP('1972-10-25', 'YYYY-MM-DD'), TO_TIMESTAMP('2017-03-10', 'YYYY-MM-DD'), 'V13135'),
       ('p8', 'female', TO_TIMESTAMP('2010-01-10', 'YYYY-MM-DD'), NULL, 'Z54564'),
       ('p6', 'female', TO_TIMESTAMP('1991-03', 'YYYY-MM'), TO_TIMESTAMP('2017-03-10', 'YYYY-MM-DD'), 'G02547'),
       ('p9', 'female', TO_TIMESTAMP('1999-05-12', 'YYYY-MM-DD'), TO_TIMESTAMP('2017-03-10', 'YYYY-MM-DD'), NULL),
       ('p10', 'female', TO_TIMESTAMP('2003-11', 'YYYY-MM'), TO_TIMESTAMP('2017-03-10', 'YYYY-MM-DD'), 'G02547'),
       ('p3', 'male', TO_TIMESTAMP('1997-02', 'YYYY-MM'), NULL, 'G02547');

INSERT INTO otherobservations
(pid, "time", encounterId, code, "value")
VALUES ('p1', TO_TIMESTAMP('2007-10-11T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e1', '9110-8', '450'),
       ('p2', TO_TIMESTAMP('2007-11-12T09:00:00+00:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e2', '1335-9', '420'),
       ('p3', TO_TIMESTAMP('2007-10-13T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e3', '1299-7', '30.5'),
       ('p1', TO_TIMESTAMP('2007-10-13T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e1', '1035-5', '45'),
       ('p3', TO_TIMESTAMP('2007-10-13T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e3', '1298-9', '43.2'),
       ('p4', TO_TIMESTAMP('2007-10-13T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e4', '38214-3', '10'),
       ('p5', TO_TIMESTAMP('2007-10-13T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e5', '9187-6', '4'),
       ('p6', TO_TIMESTAMP('2007-10-13T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e6', '445619006', '3'),
       ('p7', TO_TIMESTAMP('2007-10-13T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e7', '445597002', '5'),
       ('p8', TO_TIMESTAMP('2007-10-13T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e8', '9269-2', '5'),
       ('p4', TO_TIMESTAMP('2007-10-13T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e4', '313002', '25'),
       ('p1', TO_TIMESTAMP('2007-10-13T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e1', '35629', '10'),
       ('p2', TO_TIMESTAMP('2007-10-13T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e2', '33834', '35'),
       ('p4', TO_TIMESTAMP('2007-10-13T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e4', '9269-2', '4-3-5-4');


INSERT INTO care_site
(care_site_id, care_site_name, place_of_service_concept_id, location_id, care_site_source_value,
 place_of_service_source_value)
VALUES (1, 'Example care site name', 8717, 1, '2600GD', 'Inpatient Facility'),
       (2, NULL, 8756, 2, '2600RA', 'Outpatient Facility'),
       (3, NULL, 8940, NULL, '815501822', ' '),
       (4, NULL, 8940, NULL, '272401260', ' '),
       (5, NULL, 8940, NULL, '017191654', ' '),
       (6, NULL, 8940, NULL, '396635013', ' '),
       (7, NULL, 8940, NULL, '631138902', ' '),
       (8, NULL, 8717, NULL, '3913XU', 'Inpatient Facility'),
       (9, NULL, 8717, NULL, '3900MB', 'Inpatient Facility'),
       (10, NULL, 8717, NULL, '3900HM', 'Inpatient Facility'),
       (11, NULL, 8756, NULL, '3901GS', 'Outpatient Facility'),
       (12, NULL, 8756, NULL, '3939PG', 'Outpatient Facility'),
       (13, NULL, 8940, NULL, '673314266', ' '),
       (14, NULL, 8940, NULL, '028262926', ' '),
       (15, NULL, 8940, NULL, '027730834', ' '),
       (16, NULL, 8940, NULL, '335324178', ' '),
       (17, NULL, 8940, NULL, '602380861', ' '),
       (18, NULL, 8940, NULL, '958903309', ' '),
       (19, NULL, 8940, NULL, '802066794', ' '),
       (20, NULL, 8940, NULL, '392434615', ' ');


INSERT INTO location
(location_id, address_1, address_2, city, state, zip, county, location_source_value, country_concept_id,
 country_source_value, latitude, longitude)
VALUES (1, '19 Farragut', 'Oxford Street', NULL, 'MO', NULL, '26950', '26-950', 4330424, NULL, NULL, NULL),
       (2, NULL, NULL, NULL, 'PA', NULL, '39230', '39-230', NULL, NULL, NULL, NULL),
       (3, NULL, NULL, NULL, 'PA', NULL, '39280', '39-280', NULL, NULL, NULL, NULL),
       (4, NULL, NULL, NULL, 'CO', NULL, '06290', '06-290', NULL, NULL, NULL, NULL),
       (5, NULL, NULL, NULL, 'WI', NULL, '52590', '52-590', NULL, NULL, NULL, NULL);


INSERT INTO procedure_occurrence
(procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, procedure_datetime, procedure_end_date,
 procedure_end_datetime, procedure_type_concept_id, modifier_concept_id, quantity, provider_id, visit_occurrence_id,
 visit_detail_id, procedure_source_value, procedure_source_concept_id, modifier_source_value)
VALUES (1, 906440, 2008238, '2010-04-25', NULL, NULL, NULL, 38000251, NULL, NULL, 48878, 43483680, NULL, '9904',
        2008238, NULL),
       (2, 956309, 2000064, '2010-05-13', NULL, NULL, NULL, 38000251, NULL, NULL, 12731, 45887852, NULL, '0066',
        2000064, NULL),
       (3, 2296927, 2005199, '2008-03-29', NULL, NULL, NULL, 38000251, NULL, NULL, 202664, 110204292, NULL,
        '7806', 2005199, NULL),
       (4, 1121148, 2005595, '2009-08-20', NULL, NULL, NULL, 38000251, NULL, NULL, 14272, 53771451, NULL,
        '7994', 2005595, NULL),
       (5, 2218124, 2001220, '2008-05-08', NULL, NULL, NULL, 38000251, NULL, NULL, 47376, 106415668, NULL,
        '2761', 2001220, NULL);

INSERT INTO concept
(concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept,
 concept_code, valid_start_date, valid_end_date, invalid_reason)
VALUES (8756, 'Outpatient Hospital', 'Place of Service', 'Place of Service', 'Place of Service', 'S', '22',
        '1970-01-01', '2099-12-31', NULL),
       (8940, 'Office', 'Place of Service', 'Place of Service', 'Place of Service', 'S', '11', '1970-01-01',
        '2099-12-31', NULL),
       (8717, 'Inpatient Hospital', 'Place of Service', 'Place of Service', 'Place of Service', 'S', '21', '1970-01-01',
        '2099-12-31', NULL),
       (2000064, 'Percutaneous transluminal coronary angioplasty [PTCA]', 'Procedure', 'ICD9Proc', '4-dig billing code',
        'S', '00.66', '2005-10-01', '2099-12-31', NULL),
       (2001220, 'Suture of laceration of palate', 'Procedure', 'ICD9Proc', '4-dig billing code', 'S', '27.61',
        '1970-01-01', '2099-12-31', NULL),
       (2008238, 'Transfusion of packed cells', 'Procedure', 'ICD9Proc', '4-dig billing code', 'S', '99.04',
        '1970-01-01', '2099-12-31', NULL),
       (2005595, 'Unspecified operation on bone injury, phalanges of hand', 'Procedure', 'ICD9Proc',
        '4-dig billing code', 'S', '79.94', '1970-01-01', '2099-12-31', NULL),
       (2005199, 'Bone graft, patella', 'Procedure', 'ICD9Proc', '4-dig billing code', 'S', '78.06', '1970-01-01',
        '2099-12-31', NULL),
       (4011566, 'Health visitor', 'Provider Specialty', 'SNOMED', 'Social Context', NULL, '159000000', '1970-01-01',
        '2016-07-30', 'D'),
       (4330153, 'Nurse practitioner', 'Provider Specialty', 'SNOMED', 'Social Context', NULL, '224571005',
        '1970-01-01', '2099-12-31', NULL);
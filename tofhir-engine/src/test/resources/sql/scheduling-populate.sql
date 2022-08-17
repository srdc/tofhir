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


INSERT INTO patients
(pid, gender, birthDate, deceasedDateTime, homePostalCode)
VALUES ('p1', 'male', TO_TIMESTAMP('2000-05-10', 'YYYY-MM-DD'), NULL, NULL),
       ('p2', 'male', TO_TIMESTAMP('1985-05-08', 'YYYY-MM-DD'), TO_TIMESTAMP('2017-03-10', 'YYYY-MM-DD'), 'G02547'),
       ('p3', 'male', TO_TIMESTAMP('1997-02', 'YYYY-MM'), NULL, 'G02547'),
       ('p4', 'male', TO_TIMESTAMP('1999-06-05', 'YYYY-MM-DD'), TO_TIMESTAMP('2017-03-10', 'YYYY-MM-DD'), 'H10564'),
       ('p5', 'male', TO_TIMESTAMP('1965-10-01', 'YYYY-MM-DD'), TO_TIMESTAMP('2019-04-21', 'YYYY-MM-DD'), 'G02547'),
       ('p6', 'female', TO_TIMESTAMP('1991-03', 'YYYY-MM'), TO_TIMESTAMP('2017-03-10', 'YYYY-MM-DD'), 'G02547'),
       ('p7', 'female', TO_TIMESTAMP('1972-10-25', 'YYYY-MM-DD'), TO_TIMESTAMP('2017-03-10', 'YYYY-MM-DD'), 'V13135'),
       ('p8', 'female', TO_TIMESTAMP('2010-01-10', 'YYYY-MM-DD'), NULL, 'Z54564'),
       ('p9', 'female', TO_TIMESTAMP('1999-05-12', 'YYYY-MM-DD'), TO_TIMESTAMP('2017-03-10', 'YYYY-MM-DD'), NULL),
       ('p10', 'female', TO_TIMESTAMP('2003-11', 'YYYY-MM'), TO_TIMESTAMP('2017-03-10', 'YYYY-MM-DD'), 'G02547');


INSERT INTO otherobservations
(pid, "time", encounterId, code, "value")
VALUES ('p1', TO_TIMESTAMP('2007-10-11T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e1', '9110-8', '450'),
       ('p2', TO_TIMESTAMP('2007-10-11T09:00:00+00:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e2', '1335-9', '420'),
       ('p3', TO_TIMESTAMP('2007-10-11T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e3', '1299-7', '30.5'),
       ('p1', TO_TIMESTAMP('2007-10-11T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e4', '1035-5', '45'),
       ('p3', TO_TIMESTAMP('2007-10-11T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e5', '1298-9', '43.2'),
       ('p4', TO_TIMESTAMP('2007-10-11T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e6', '38214-3', '10'),
       ('p5', TO_TIMESTAMP('2007-10-11T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e7', '9187-6', '4'),
       ('p6', TO_TIMESTAMP('2007-10-11T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e8', '445619006', '3'),
       ('p7', TO_TIMESTAMP('2007-10-11T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e9', '445597002', '5'),
       ('p8', TO_TIMESTAMP('2007-10-11T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e10', '9270-2', '5'),
       ('p4', TO_TIMESTAMP('2007-10-11T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e11', '313002', '25'),
       ('p1', TO_TIMESTAMP('2007-10-11T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e12', '35629', '10'),
       ('p2', TO_TIMESTAMP('2007-10-11T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e13', '33834', '35'),
       ('p4', TO_TIMESTAMP('2007-10-13T10:00:00+01:00', 'YYYY-MM-DDTHH24:MI:SS+TZH:TZM'), 'e14', '9269-2', '4-3-5-4');

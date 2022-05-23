CREATE TABLE patients (
                                 pid varchar(4) NULL,
                                 gender varchar(8) NULL,
                                 birthDate varchar(16) NULL,
                                 deceasedDateTime varchar(16) NULL,
                                 homePostalCode varchar(8) NULL
);

CREATE TABLE otherobservations (
                                   pid varchar(255) NULL,
                                   "time" timestamp NULL,
                                   encounterId varchar(255) NULL,
                                   code varchar(255) NULL,
                                   "value" varchar(255) NULL
);

INSERT INTO patients
(pid, gender, birthDate, deceasedDateTime, homePostalCode)
VALUES('p1', 'male', '2000-05-10', '2017-03-10', 'G02547');
INSERT INTO patients
(pid, gender, birthDate, deceasedDateTime, homePostalCode)
VALUES('p2', 'male', '1985-05-08', '2017-03-10', 'G02547');
INSERT INTO patients
(pid, gender, birthDate, deceasedDateTime, homePostalCode)
VALUES('p5', 'male', '1965-10-01', '2019-04-21', 'G02547');
INSERT INTO patients
(pid, gender, birthDate, deceasedDateTime, homePostalCode)
VALUES('p4', 'male', '1999-06-05', '2017-03-10', 'H10564');
INSERT INTO patients
(pid, gender, birthDate, deceasedDateTime, homePostalCode)
VALUES('p7', 'female', '1972-10-25', '2017-03-10', 'V13135');
INSERT INTO patients
(pid, gender, birthDate, deceasedDateTime, homePostalCode)
VALUES('p8', 'female', '2010-01-10', NULL, 'Z54564');
INSERT INTO patients
(pid, gender, birthDate, deceasedDateTime, homePostalCode)
VALUES('p6', 'female', '1991-03', '2017-03-10', 'G02547');
INSERT INTO patients
(pid, gender, birthDate, deceasedDateTime, homePostalCode)
VALUES('p9', 'female', '1999-05-12', '2017-03-10', NULL);
INSERT INTO patients
(pid, gender, birthDate, deceasedDateTime, homePostalCode)
VALUES('p10', 'female', '2003-11', '2017-03-10', 'G02547');
INSERT INTO patients
(pid, gender, birthDate, deceasedDateTime, homePostalCode)
VALUES('p3', 'male', '1997-02', NULL, 'G02547');



INSERT INTO otherobservations
(pid, "time", encounterId, code, "value")
VALUES('p1', '2007-10-11T10:00:00+01:00', 'e1', '9110-8', '450');
INSERT INTO otherobservations
(pid, "time", encounterId, code, "value")
VALUES('p2', '2007-11-12T09:00:00+00:00', 'e2', '1335-9', '420');
INSERT INTO otherobservations
(pid, "time", encounterId, code, "value")
VALUES('p3', '2007-10-13T10:00:00+01:00', 'e3', '1299-7', '30.5');
INSERT INTO otherobservations
(pid, "time", encounterId, code, "value")
VALUES('p1', '2007-10-13T10:00:00+01:00', 'e1', '1035-5', '45');
INSERT INTO otherobservations
(pid, "time", encounterId, code, "value")
VALUES('p3', '2007-10-13T10:00:00+01:00', 'e3', '1298-9', '43.2');
INSERT INTO otherobservations
(pid, "time", encounterId, code, "value")
VALUES('p4', '2007-10-13T10:00:00+01:00', 'e4', '38214-3', '10');
INSERT INTO otherobservations
(pid, "time", encounterId, code, "value")
VALUES('p5', '2007-10-13T10:00:00+01:00', 'e5', '9187-6', '4');
INSERT INTO otherobservations
(pid, "time", encounterId, code, "value")
VALUES('p6', '2007-10-13T10:00:00+01:00', 'e6', '445619006', '3');
INSERT INTO otherobservations
(pid, "time", encounterId, code, "value")
VALUES('p7', '2007-10-13T10:00:00+01:00', 'e7', '445597002', '5');
INSERT INTO otherobservations
(pid, "time", encounterId, code, "value")
VALUES('p8', '2007-10-13T10:00:00+01:00', 'e8', '9269-2', '5');
INSERT INTO otherobservations
(pid, "time", encounterId, code, "value")
VALUES('p4', '2007-10-13T10:00:00+01:00', 'e4', '313002', '25');
INSERT INTO otherobservations
(pid, "time", encounterId, code, "value")
VALUES('p1', '2007-10-13T10:00:00+01:00', 'e1', '35629', '10');
INSERT INTO otherobservations
(pid, "time", encounterId, code, "value")
VALUES('p2', '2007-10-13T10:00:00+01:00', 'e2', '33834', '35');
INSERT INTO otherobservations
(pid, "time", encounterId, code, "value")
VALUES('p4', '2007-10-13T10:00:00+01:00', 'e4', '9269-2', '4-3-5-4');

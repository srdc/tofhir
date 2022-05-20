CREATE TABLE patients (
                                 pid varchar(4) NULL,
                                 gender varchar(8) NULL,
                                 birthDate varchar(16) NULL,
                                 deceasedDateTime varchar(16) NULL,
                                 homePostalCode varchar(8) NULL
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

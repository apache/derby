--
-- this test shows the current supported select functionality
--


create table t(i int, s smallint);
insert into t (i,s) values (1956,475);

-- select a subset of the columns
select i from t;

-- select all columns in order
select i,s from t;

-- select columns out of order
select s,i from t;

-- select with repeating columns
select i,i,s,s,i,i from t;

-- select with constants
select 10 from t;

-- select with table name
select t.i from t;

-- select with correlation name
select b.i from t b;

-- select *
select * from t;

-- select * and constants and columns 
select *, 10, i from t;

-- select correlation name dot star
select b.* from t b;

-- select table name dot star
select t.* from t;

-- believe it or not, the next query is valid
(select * from t);

-- negative testing
-- non-boolean where clause
-- (only put here due to small size of this test)
select * from t where i;

-- invalid correlation name for "*"
select asdf.* from t; 

-- cleanup
drop table t;

-- Beetle 5019.  We had a failure in V5.1.20.
CREATE SCHEMA CONTENT;
CREATE TABLE CONTENT.CONTENT (ID INTEGER NOT NULL, CREATOR VARCHAR(128) NOT NULL, CREATION_DATE DATE NOT NULL, URL VARCHAR(256) NOT NULL, TITLE VARCHAR(128) NOT NULL, DESCRIPTION VARCHAR(512) NOT NULL, HEIGHT INTEGER NOT NULL, WIDTH INTEGER NOT NULL);
ALTER TABLE CONTENT.CONTENT ADD CONSTRAINT CONTENT_ID PRIMARY KEY (ID);
CREATE TABLE CONTENT.STYLE (ID INTEGER NOT NULL,DESCRIPTION VARCHAR(128) NOT NULL);
ALTER TABLE CONTENT.STYLE ADD CONSTRAINT STYLE_ID PRIMARY KEY (ID);
CREATE TABLE CONTENT.CONTENT_STYLE  (CONTENT_ID INTEGER NOT NULL, STYLE_ID INTEGER NOT NULL);
ALTER TABLE CONTENT.CONTENT_STYLE ADD CONSTRAINT CONTENTSTYLEID PRIMARY KEY (CONTENT_ID, STYLE_ID);
CREATE TABLE CONTENT.KEYGEN (KEYVAL INTEGER NOT NULL, KEYNAME VARCHAR(256) NOT NULL);
ALTER TABLE CONTENT.KEYGEN  ADD CONSTRAINT PK_KEYGEN PRIMARY KEY (KEYNAME);
CREATE TABLE CONTENT.RATING  (ID INTEGER NOT NULL,RATING DOUBLE PRECISION NOT NULL,ENTRIES DOUBLE PRECISION NOT NULL);
ALTER TABLE CONTENT.RATING ADD CONSTRAINT PK_RATING PRIMARY KEY (ID);

INSERT INTO CONTENT.STYLE VALUES (1, 'BIRD');
INSERT INTO CONTENT.STYLE VALUES (2, 'CAR');
INSERT INTO CONTENT.STYLE VALUES (3, 'BUILDING');
INSERT INTO CONTENT.STYLE VALUES (4, 'PERSON');

INSERT INTO CONTENT.CONTENT values(1, 'djd', CURRENT DATE, 'http://url.1', 'title1', 'desc1', 100, 100);
INSERT INTO CONTENT.CONTENT values(2, 'djd', CURRENT DATE, 'http://url.2', 'title2', 'desc2', 100, 100);
INSERT INTO CONTENT.CONTENT values(3, 'djd', CURRENT DATE, 'http://url.3', 'title3', 'desc3', 100, 100);
INSERT INTO CONTENT.CONTENT values(4, 'djd', CURRENT DATE, 'http://url.4', 'title4', 'desc4', 100, 100);
INSERT INTO CONTENT.CONTENT values(5, 'djd', CURRENT DATE, 'http://url.5', 'title5', 'desc5', 100, 100);

INSERT INTO CONTENT.CONTENT_STYLE VALUES(1,1);
INSERT INTO CONTENT.CONTENT_STYLE VALUES(1,2);
INSERT INTO CONTENT.CONTENT_STYLE VALUES(2,1);
INSERT INTO CONTENT.CONTENT_STYLE VALUES(2,4);
INSERT INTO CONTENT.CONTENT_STYLE VALUES(3,3);
INSERT INTO CONTENT.CONTENT_STYLE VALUES(3,4);
INSERT INTO CONTENT.CONTENT_STYLE VALUES(3,1);
INSERT INTO CONTENT.CONTENT_STYLE VALUES(4,4);
INSERT INTO CONTENT.CONTENT_STYLE VALUES(5,1);


INSERT INTO CONTENT.RATING VALUES(1, 4.5, 1);
INSERT INTO CONTENT.RATING VALUES(2, 4.0, 1);
INSERT INTO CONTENT.RATING VALUES(3, 3.9, 1);
INSERT INTO CONTENT.RATING VALUES(4, 4.1, 1);
INSERT INTO CONTENT.RATING VALUES(5, 4.0, 1);

select S.DESCRIPTION, FAV.MAXRATE, C.TITLE, C.URL FROM CONTENT.RATING R, CONTENT.CONTENT C, CONTENT.STYLE S, CONTENT.CONTENT_STYLE CS, (select S.ID, max(rating) from CONTENT.RATING R, CONTENT.CONTENT C, CONTENT.STYLE S, CONTENT.CONTENT_STYLE CS group by S.ID) AS FAV(FID,MAXRATE) where R.ID = C.ID AND C.ID = CS.CONTENT_ID AND CS.STYLE_ID = FAV.FID AND FAV.FID = S.ID AND FAV.MAXRATE = R.RATING;

drop table content.rating;
drop table content.content_style;
drop table content.content;
drop table content.style;
drop table content.keygen;
drop schema content restrict;


drop function PADSTRING;
drop function RANDOM;
CREATE FUNCTION  PADSTRING (DATA VARCHAR(32000), LENGTH INTEGER) RETURNS VARCHAR(32000) EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.Formatters.padString' LANGUAGE JAVA PARAMETER STYLE JAVA;

CREATE FUNCTION RANDOM() RETURNS DOUBLE EXTERNAL NAME 'java.lang.Math.random' LANGUAGE JAVA PARAMETER STYLE JAVA;


drop table main;
drop table main2;

create table main(x int not null primary key, y varchar(2000));
insert into main values(1, PADSTRING('aaaa',2000));
insert into main values(2, PADSTRING('aaaa',2000));
insert into main values(3, PADSTRING('aaaa',2000));
insert into main values(4, PADSTRING('aaaa',2000));
insert into main values(5, PADSTRING('aaaa',2000));
insert into main values(6, PADSTRING('aaaa',2000));
insert into main values(7, PADSTRING('aaaa',2000));
insert into main values(8, PADSTRING('aaaa',2000));
insert into main values(9, PADSTRING('aaaa',2000));
insert into main values(10, PADSTRING('aaaa',2000));
insert into main values(12, PADSTRING('aaaa',2000));
insert into main values(13, PADSTRING('aaaa',2000));

create table main2(x int not null primary key, y varchar(2000));
insert into main2 values(1, PADSTRING('aaaa',2000));
insert into main2 values(2, PADSTRING('aaaa',2000));
insert into main2 values(3, PADSTRING('aaaa',2000));
insert into main2 values(4, PADSTRING('aaaa',2000));
insert into main2 values(5, PADSTRING('aaaa',2000));
insert into main2 values(6, PADSTRING('aaaa',2000));
insert into main2 values(7, PADSTRING('aaaa',2000));
insert into main2 values(8, PADSTRING('aaaa',2000));
insert into main2 values(9, PADSTRING('aaaa',2000));
insert into main2 values(10, PADSTRING('aaaa',2000));
insert into main2 values(12, PADSTRING('aaaa',2000));
insert into main2 values(13, PADSTRING('aaaa',2000));
disconnect;

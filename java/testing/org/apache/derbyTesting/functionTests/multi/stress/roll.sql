autocommit off;
insert into main values (666, '666');
rollback;

disconnect;

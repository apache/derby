
-- this test shows the ij commands in use,
-- and what happens when invalid stuff is entered.

-- no driver loaded yet, detected off of the url
-- this one is a bad url:
connect 'cloudscape:wombat';
-- this one will work.
connect 'jdbc:derby:wombat';

-- no connection yet, this will fail
create table t (i int);

-- no table yet, this will fail
select i from t;

-- invalid syntax ... incomplete statements
driver;
connect;
prepare;
execute;
run;
remove;

-- should fail because procedure is an illegal statement name
prepare procedure as 'select * from bar';

-- should fail because text is passed on to derby, which
-- barfs on the unknown statement name. execute procedure is
-- a foundation 2000 concept
execute procedure sqlj.install_jar( 'file:c:/p4c/systest/out/DigIt.jar', 'SourceWUs', 1 );


-- and, the help output:
help;


driver 'org.apache.derby.jdbc.EmbeddedDriver';
connect 'jdbc:derby:wombat;create=true';

-- can we run a simple query?
values 1;

-- can we disconnect?
disconnect;

-- can we reconnect?
connect 'jdbc:derby:wombat;create=true';

-- can we run a simple query?
values 1;
disconnect;

-- do we get a non-internal error when we try to create
-- over an existing directory? (T#674)
connect 'jdbc:derby:wombat/seg0;create=true';

-- check to ensure an empty database name is taken
-- as the name, over any connection attribute.
-- this should fail.
connect 'jdbc:derby: ;databaseName=wombat';

-- and this should succeed (no database name in URL)
connect 'jdbc:derby:;databaseName=wombat';
disconnect;

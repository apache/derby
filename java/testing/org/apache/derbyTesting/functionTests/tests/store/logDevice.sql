
-- note: weird is spelled wrong here.  So you too don't spend 15 minutes figuring that out.
values SUBSTR(SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice'), LOCATE('wierdlog',SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice')),8);

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('logDevice', null);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('logDevice', 'foobar');

values SUBSTR(SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice'), LOCATE('wierdlog',SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice')),8);

call SYSCS_UTIL.SYSCS_BACKUP_DATABASE('ext/mybackup');

disconnect;
connect 'wombat;shutdown=true';

connect 'wombat';

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('logDevice', 'foobar');

values SUBSTR(SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice'), LOCATE('wierdlog',SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice')),8);


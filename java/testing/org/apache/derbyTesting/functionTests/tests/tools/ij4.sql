connect 'jdbc:derby:wombat;create=true';

-- test maximum display width
values cast('1' as varchar(512));
maximumdisplaywidth 40;
values cast('1' as varchar(512));
maximumdisplaywidth 400;
values cast('1' as varchar(512));

-- and, the exit command:
exit;




-- this file contains any ejbql tests which produce an error which
-- includes a column number. The column number will change for SPS tests
-- so these tests can't be included in allAsSPS

-- absolute is not a reserved word any more
create table absolute( a int );

values{ fn abs( NULL ) };
values{ fn abs( null ) };
values{ fn abs( ) };

-- Error
values{ fn concat( ) };
values{ fn concat( 'error0' ) };
values  fn concat( 'syntax', ' error1' );
values{ fn concat { 'syntax', ' error2' }};
values{ fn concat( 'syntax', ' error3' });
values{ fn concat( fn concat( 'nested', ' not ' ), ' allowed!' ) };
values{ fn concat( values{ fn concat( 'nested', ' not ' ) }, ' allowed!' ) };

values{ fn locate( null, null ) };
values{ fn locate( null, ''   ) };
values{ fn locate( '', null   ) };

values{ fn locate( null, null,1) };
values{ fn locate( null, ''  ,1) };
values{ fn locate( '', null  ,1) };

values{ fn sqrt( NULL ) };
values{ fn sqrt( null ) };
values{ fn sqrt( ) };

-- Substring tests. Valid only as escaped function
values substring('asdf', 1);
values substring(X'101101', 3);
values substring('asdf', 1, 2);

values {fn substring('asdf', 1)};
values {fn substring(X'101101', 3)};
values {fn substring('asdf', 1, 2)};

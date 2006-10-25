--
--   Licensed to the Apache Software Foundation (ASF) under one or more
--   contributor license agreements.  See the NOTICE file distributed with
--   this work for additional information regarding copyright ownership.
--   The ASF licenses this file to You under the Apache License, Version 2.0
--   (the "License"); you may not use this file except in compliance with
--   the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.
--
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

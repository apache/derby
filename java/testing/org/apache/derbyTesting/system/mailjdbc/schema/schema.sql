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
--Drop table inbox;

CREATE TABLE inbox (id bigint generated always as identity (start with 1,increment by 1),
					from_name varchar(64),
					to_name varchar(64),
					message clob(3M),
					date timestamp,
					folder_id Integer,
					to_delete smallint default 0,
					exp_date timestamp,
					size_problem varchar(32672),
					CONSTRAINT inbox__pk PRIMARY KEY (id));
					
CREATE TABLE attach (id bigint not null,
				attach_id bigint generated always as identity (start with 1, increment by 1),
		      		attachment blob(5M),
		      		CONSTRAINT attach__pk PRIMARY KEY (id,attach_id),
		      		constraint attach_fk foreign key (id) references inbox(id)
		      		ON DELETE CASCADE );

--Drop table folders;
		      		
CREATE TABLE folders (folder_id integer generated always as identity (start with 1,increment by 1),
					 foldername varchar(16));
					 

insert into folders (foldername) values('folder1');
insert into folders (foldername) values('folder2');
insert into folders (foldername) values('folder3');
insert into folders (foldername) values('folder4');
insert into folders (foldername) values('folder5');


CREATE INDEX to_delete_Index ON inbox(to_delete); 
CREATE INDEX to_name_Index ON inbox(to_name); 
CREATE INDEX date_Index ON inbox(date); 
CREATE INDEX attach_at_Index ON attach(attach_id); 

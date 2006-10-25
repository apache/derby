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
drop table localtab;
create table localtab(	ndec decimal(5,3), 
							ndatec date , 
							ntimec time , 
							ncharc varchar(500));
insert into localtab values(12.345, date('2000-05-25'),'15:30:15','\u30a4\u30d9\u30f3\u30c8\u30a2\u30e9\u30fc\u30e0\u304c\u6709\u52b9\u3067\u3059\u3002');
insert into localtab values(32.432, date('2000-05-18'),'15:32:10','DR:DRAUTO\u306f0 (Off)\u3067\u3059\u3002');
insert into localtab values(54.846, date('2000-01-28'),'15:32:28','INFORMIX-OnLine\u304c\u521d\u671f\u5316\u3055\u308c\u3001\u30c7\u30a3\u30b9\u30af\u306e\u521d\u671f\u5316\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f\u3002');
insert into localtab values(39.003, date('2000-11-11'),'15:32:29','\u30c1\u30a7\u30c3\u30af\u30dd\u30a4\u30f3\u30c8\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f:\u7d99\u7d9a\u6642\u9593\u306f 0\u79d2\u3067\u3057\u305f');
insert into localtab values(79.406, date('2000-06-05'),'15:32:29','\u3059\u3079\u3066\u306eDB\u9818\u57df\u306e\u30c7\u30fc\u30bf\u30b9\u30ad\u30c3\u30d7\u306f\u73fe\u5728\u30aa\u30d5\u306b\u306a\u3063\u3066\u3044\u307e\u3059\u3002');
insert into localtab values(94.999, date('2000-07-30'),'15:32:30','On-Line\u30e2\u30fc\u30c9');
insert into localtab values(18.849, date('2000-10-22'),'15:32:31','sysmaster\u30c7\u30fc\u30bf\u30d9\u30fc\u30b9\u3092\u4f5c\u6210\u4e2d\u3067\u3059...');
insert into localtab values(35.444, date('2000-02-29'),'15:33:22','\u8ad6\u7406\u30ed\u30b0 1\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f\u3002');
insert into localtab values(84.391, date('2000-01-21'),'15:33:23','\u30ea\u30bf\u30fc\u30f3\u30b3\u30fc\u30c9 1\u3092\u623b\u3057\u3066\u30d7\u30ed\u30bb\u30b9\u304c\u7d42\u4e86\u3057\u307e\u3057\u305f:/bin/sh /bin/sh -c /work1/MOSES_7.22.UC1A5_27/sqldist/etc/log_full.sh 2 23 \u8ad6\u7406\u30ed\u30b0 1\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f\u3002 \u8ad6\u7406');
insert into localtab values(56.664, date('2000-04-16'),'15:33:40','\u8ad6\u7406\u30ed\u30b0 2\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f\u3002');
insert into localtab values(22.393, date('2000-03-01'),'15:33;42','\u30ea\u30bf\u30fc\u30f3\u30b3\u30fc\u30c9 1\u3092\u623b\u3057\u3066\u30d7\u30ed\u30bb\u30b9\u304c\u7d42\u4e86\u3057\u307e\u3057\u305f:/bin/sh /bin/sh -c /work1/MOSES_7.22.UC1A5_27/sqldist/etc/log_full.sh 2 23 \u8ad6\u7406\u30ed\u30b0 2\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f\u3002 \u8ad6\u7406');
insert into localtab values(90.007, date('2000-11-27'),'15:33:43','\u30c1\u30a7\u30c3\u30af\u30dd\u30a4\u30f3\u30c8\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f:\u7d99\u7d9a\u6642\u9593\u306f 2\u79d2\u3067\u3057\u305f');
insert into localtab values(30.496, date('2000-04-03'),'15:34:29','\u8ad6\u7406\u30ed\u30b0 3\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f\u3002');
insert into localtab values(66.295, date('2000-10-15'),'15:34:30','\u30ea\u30bf\u30fc\u30f3\u30b3\u30fc\u30c9 1\u3092\u623b\u3057\u3066\u30d7\u30ed\u30bb\u30b9\u304c\u7d42\u4e86\u3057\u307e\u3057\u305f:/bin/sh /bin/sh -c /work1/MOSES_7.22.UC1A5_27/sqldist/etc/log_full.sh 2 23 \u8ad6\u7406\u30ed\u30b0 3\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f\u3002 \u8ad6\u7406');
insert into localtab values(54.332, date('2000-09-01'),'15:35:35','sysmaster\u30c7\u30fc\u30bf\u30d9\u30fc\u30b9\u306e\u4f5c\u6210\u306f\u5b8c\u4e86\u3057\u307e\u3057\u305f\u3002');
insert into localtab values(11.105, date('2000-07-09'),'15:39:10','\u30c1\u30a7\u30c3\u30af\u30dd\u30a4\u30f3\u30c8\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f:\u7d99\u7d9a\u6642\u9593\u306f 8\u79d2\u3067\u3057\u305f');

-- display in non localized format
select * from localtab;

-- display in localized format
LOCALIZEDDISPLAY ON;
select * from localtab;

-- test the pushing of predicates into unflattened views
-- and derived tables

set isolation to rr;

-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

-- set up
create table t1(c1 int, c2 int, c3 varchar(2000));

-- simple views
create view sv1 (c1, c2, c3) as select c1, c2, c3 || '$' from t1;
create view sv2 (x1, x2, x3) as select c1, c2, c3  || '$' from t1;
create view sv3 (y1, y2, y3) as select x1, x2, x3  || '$' from sv2;
create view sv4 (z1, z2, z3, z4, z5, z6) as
select a.c1, a.c2, a.c3 || '$', b.c1, b.c2, b.c3 || '$' from t1 a, t1 b;

-- more complex views
create view av1 (c1, c2) as select c1, max(c2) from t1 group by c1;
create view av2 (x1, x2) as select c1, max(c2) from av1 group by c1;
create view av3 (y1, y2, y3, y4) as
select a.c1, b.c1, max(a.c2), max(b.c2) from t1 a, t1 b group by a.c1, b.c1;

-- non-flattenable derived table in a non-flattenable view
create view cv1 (c1, c2) as
select c1, max(c2) from (select c1, c2 + 1 from t1) t(c1, c2) group by c1;

-- populate the tables
insert into t1 values (1, 1, ''), (1, 1, ''), (1, 2, ''), (1, 2, ''),
		      (2, 2, ''), (2, 2, ''), (2, 3, ''), (2, 3, '');


call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 2000;

-- predicate should get pushed into scan
select c1, c2 from sv1 where c1 = 1 order by c1, c2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select c1, c2 from sv1 where c1 = 1  + 1 order by c1, c2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select x1, x2 from sv2 where x1 = 1 order by x1, x2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select y1, y2 from sv3 where y1 = 1 order by y1, y2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select z1, z2, z4, z5 from sv4 where z1 = z4 and z2 = z5
order by z1, z2, z4, z5;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from av1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from av2 where x1 = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from av3;
select y1, y2, y3, y4 + 0 from av3 where y1 = y2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from cv1;
select * from cv1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
create index t1_c1 on t1(c1);
select c1, c2 from sv1 where c1 = 1 order by c1, c2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select x1, x2 from sv2 where x1 = 1 order by x1, x2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select y1, y2 from sv3 where y1 = 1 order by y1, y2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select z1, z2, z4, z5 from sv4 where z1 = z4 and z2 = z5
order by z1, z2, z4, z5;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from av1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from av2 where x1 = 2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select y1, y2, y3, y4 + 0 from av3 where y1 = y2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from cv1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();


-- cleanup
drop view cv1;
drop view av3;
drop view av2;
drop view av1;
drop view sv4;
drop view sv3;
drop view sv2;
drop view sv1;
drop table t1;

-- Beetle 4983, customer case, complex query involving views, wrong column remapping
-- after view flattening, NullPointerException, and didn't get predicate pushed down
-- all the way to base table.

autocommit off;

create schema xr;

create table xr.businessentity 
			 ( 	ID    		varchar(48) not null primary key ,
				type		varchar(24) not null,
                           	name    	varchar(128) not null ,
			   	uname		varchar(128) ,
--GENERATED ALWAYS AS (ucase(name)),
			   	description	varchar(256),
                           	createdate   	timestamp  not null,
                           	changedate   	timestamp  not null,
                           	deletedate   	timestamp,
			   	-- for type=BUSINESS this is the delegate owner
			   	-- for type=USER this is their userID
				AuthID	    	varchar(48) not null,  
			       	ownerUserUUID 	varchar(48),
				-- for type=BUSINESS -- in subclass 
			       	businessTypeID  varchar(48)
                         );

create trigger xr.be_uname_i after insert on xr.businessentity
referencing new as n
for each row mode db2sql
update xr.businessentity set uname = upper( n.name ) where name = n.name;

create trigger xr.be_unane_u after update of name, uname on xr.businessentity
referencing new as n
for each row mode db2sql
update xr.businessentity set uname = upper( n.name )
  where name = n.name and uname <> upper( n.name );

create unique index xr.bus1 on xr.businessentity (ownerUserUUID, uname, deletedate);

create table xr.BEMembers(
	beid			varchar(48) not null,
	memberid		varchar(48) not null
	);


create unique index xr.bem1 on xr.BEMembers (beid, memberid);

alter table xr.BEMembers add constraint bem_beid
	foreign key (beid)
	references xr.businessentity(id)
	on delete cascade;

alter table xr.BEMembers add constraint bem_memberid
	foreign key (memberid)
	references xr.businessentity(id)
	on delete cascade;

create table xr.xruser 
			 ( 	businessentityID   	varchar(48) not null primary key ,
				userid			varchar(48) not null, 
				deletedate   		timestamp
                    	);


create unique index xr.user1 on xr.xruser (userID, deletedate);


alter table xr.xruser add constraint u_busent foreign key (businessentityID)
                            references xr.businessentity(ID)
                            on delete cascade;


create table xr.locator 
			(ownerID		varchar(48) not null,
			type	      		varchar(48) not null,
			useTypeID	      	varchar(48) ,
			value			varchar(256),
			street1			varchar(48),
			street2			varchar(48),
			city			varchar(48),
			state			varchar(48),
			country			varchar(48),
			postalcode		varchar(48)	
			);

create unique index xr.loc1 on xr.locator ( ownerID,type,usetypeID ); 

alter table xr.locator add constraint l_busent foreign key (ownerID)
                            references xr.businessentity(ID)
                            on delete cascade;


create table xr.BusinessEntityAssociation
			(ID			varchar(48) not null primary key ,
			sourceID		varchar(48) not null,
			targetID	      	varchar(48) not null,
			ownerID		 	varchar(48) not null,
			assoctypeID		varchar(48) not null,
                  	createdate   		timestamp  not null,
                    	changedate   		timestamp  not null,
			deletedate   		timestamp, 
			description		varchar(256)
			);

alter table xr.BusinessEntityAssociation 
add constraint bea_sourceID foreign key (sourceID)
                            references xr.businessentity(ID)
                            on delete restrict;

alter table xr.BusinessEntityAssociation 
add constraint bea_targetID foreign key (targetID)
                            references xr.businessentity(ID)
                            on delete restrict;

alter table xr.BusinessEntityAssociation 
add constraint bea_ownerID foreign key (ownerID)
                            references xr.businessentity(ID)
                            on delete restrict;


create unique index xr.BEA1 
on xr.BusinessEntityAssociation( sourceid, targetid, ownerID, assoctypeID, deletedate); 


create table xr.repositoryobjectresource (
	id			varchar(48)	not null primary key,
	type			varchar(48)	not null,
	subtype		varchar(48),
	creatorid		varchar(48)	not null,
	createdate		timestamp	not null,
	currentVersion	varchar(48),
	versionControlled smallint	not null with default 0,
	checkedOut		smallint	not null with default 0,
	checkForLock	smallint	not null with default 0
	);

alter table xr.repositoryobjectresource add constraint ror_creatorid
	foreign key (creatorid)
	references xr.xruser(businessentityid)
	on delete restrict;

create table xr.repositoryobjectversion (
	id			varchar(48)		not null primary key,
	resourceid		varchar(48)		not null,
	name			varchar(128)	not null,
	uname			varchar(128),
--	GENERATED ALWAYS AS (ucase(name)),
	folderid		varchar(48),
	versionName		varchar(128)	not null,
	uri			varchar(255)	not null,
	versionuri		varchar(255)	not null,
	description		varchar(256),
	versionComment	varchar(256),
	ownerid		varchar(48)	not null,
	creatorid		varchar(48)	not null,
	versiondate		timestamp	not null,
	changedate		timestamp	not null,
	deletedate		timestamp, 
	previousversion	varchar(48)
	);
	
create trigger xr.rov_uname_i after insert on xr.repositoryobjectversion
referencing new as n
for each row mode db2sql
update xr.repositoryobjectversion set uname = upper( n.name ) where name = n.name;

create trigger xr.rov_unane_u after update of name, uname on xr.repositoryobjectversion
referencing new as n
for each row mode db2sql
update xr.repositoryobjectversion set uname = upper( n.name )
  where name = n.name and uname <> upper( n.name );
create unique index xr.versionname on xr.repositoryobjectversion (resourceid, versionName);

-- Don't think I want this constraint with versioning.
-- Object could have been deleted in a later version.
-- create unique index xr.versionuri on xr.repositoryobjectversion (versionuri, deletedate);

alter table xr.repositoryobjectversion add constraint rov_previousvers
	foreign key (previousversion)
	references xr.repositoryobjectversion(id)
	on delete set null;

alter table xr.repositoryobjectversion add constraint rov_folderid
	foreign key (folderid)
	references xr.repositoryobjectresource(id)
	on delete restrict;

alter table xr.repositoryobjectversion add constraint rov_ownerid
	foreign key (ownerid)
	references xr.businessentity(id)
	on delete restrict;

alter table xr.repositoryobjectversion add constraint rov_creatorid
	foreign key (creatorid)
	references xr.xruser(businessentityid)
	on delete restrict;

alter table xr.repositoryobjectresource add constraint ror_currentVersion
	foreign key (currentVersion)
	references xr.repositoryobjectversion(id)
	on delete restrict;

create table xr.lock (
	locktoken		varchar(48)	not null,
	resourceid		varchar(48)	not null,
	ownerid		varchar(48) not null,
	exclusive		smallint 	not null,
	timeoutSeconds	bigint	not null,
	expirationDate	timestamp	not null
	);

alter table xr.lock add primary key (locktoken, resourceid);

alter table xr.lock add constraint l_resourceid
	foreign key (resourceid)
	references xr.repositoryobjectresource(id)
	on delete cascade;

alter table xr.lock add constraint l_ownerid
	foreign key (ownerid)
	references xr.xruser(businessentityid)
	on delete cascade;

create table xr.keyword (
	versionid			varchar(48)		not null,
	keyword			varchar(128)	not null
	);

alter table xr.keyword add constraint k_versionid
	foreign key (versionid)
	references xr.repositoryobjectversion(id)
	on delete cascade;

create table xr.slot (
	versionid			varchar(48)		not null,
	name				varchar(128)	not null,
	value				varchar(256)
	);

alter table xr.slot add constraint s_versionid
	foreign key (versionid)
	references xr.repositoryobjectversion(id)
	on delete cascade;
	
create table xr.versionlabel (
	versionid			varchar(48)		not null,
	label				varchar(128)	not null
	);

alter table xr.versionlabel add constraint vl_versionid
	foreign key (versionid)
	references xr.repositoryobjectversion(id)
	on delete cascade;

create table xr.repositoryentry (
	versionid		varchar(48)		not null primary key,
	versioncontentid	varchar(48),
	mimetype		varchar(48),
	stability		varchar(48),
	status		varchar(48),
	startdate		timestamp,
	expirationdate	timestamp,
	isopaque		smallint		not null with default 0
	);

alter table xr.repositoryentry add constraint re_versionid
	foreign key (versionid)
	references xr.repositoryobjectversion(id)
	on delete cascade;

	
create table xr.repositoryentrycontent (
 	versionid			varchar(48)		not null primary key,
	contentchangedate	timestamp,
	content				long varchar
--blob(1M)
	);

alter table xr.repositoryentry add constraint re_versioncontent
	foreign key (versioncontentid)
	references xr.repositoryentrycontent(versionid)
	on delete set null;


create table xr.objectgroup_content (
	versionid		varchar(48) not null,
	memberid		varchar(48) not null
	);

alter table xr.objectgroup_content add constraint ogc_versionid
	foreign key (versionid)
	references xr.repositoryobjectversion(id)
	on delete cascade;

alter table xr.objectgroup_content add constraint ogc_memberid
	foreign key (memberid)
	references xr.repositoryobjectresource(id)
	on delete cascade;

create table xr.externaldependency_content (
	versionid		varchar(48) not null,
	objectid		varchar(48) not null
	);

alter table xr.externaldependency_content add constraint edc_objectid
	foreign key (objectid)
	references xr.repositoryobjectresource(id)
	on delete cascade;

create table xr.objectassociation (
	id			varchar(48) not null primary key,
	sourceid		varchar(48) not null,
	targetid		varchar(48) not null
	);

alter table xr.objectassociation add constraint oa_id
	foreign key (id)
	references xr.repositoryobjectresource(id)
	on delete cascade;

alter table xr.objectassociation add constraint oa_sourceid
	foreign key (sourceid)
	references xr.repositoryobjectresource(id)
	on delete cascade;

alter table xr.objectassociation add constraint oa_targetid
	foreign key (targetid)
	references xr.repositoryobjectresource(id)
	on delete cascade;

create table xr.classificationscheme (
	id			varchar(48) not null primary key,
	structuretype	varchar(48) not null
	);

alter table xr.classificationscheme add constraint cs_id
	foreign key (id)
	references xr.repositoryobjectresource(id)
	on delete cascade;

create table xr.classification_values (
	versionid		varchar(48) not null,
	valueid		varchar(48) not null,
	value			varchar(128) not null,
	description		varchar(256),
	parentvalueid	varchar(48)
	);

alter table xr.classification_values add primary key (versionid, valueid);

alter table xr.classification_values add constraint cv_versionid
	foreign key (versionid)
	references xr.repositoryobjectversion(id)
	on delete cascade;

alter table xr.classification_values add constraint cv_parentvalueid
	foreign key (versionid, parentvalueid)
	references xr.classification_values(versionid, valueid)
	on delete cascade;

create table xr.classification_value_ancestors (
	versionid		varchar(48) not null,
	valueid		varchar(48) not null,
	ancestorid		varchar(48) not null
	);

alter table xr.classification_value_ancestors add constraint cva_versionid
	foreign key (versionid)
	references xr.repositoryobjectversion(id)
	on delete cascade;

alter table xr.classification_value_ancestors add constraint cva_valueid
	foreign key (versionid, valueid)
	references xr.classification_values(versionid, valueid)
	on delete cascade;

alter table xr.classification_value_ancestors add constraint cva_ancestorid
	foreign key (versionid, ancestorid)
	references xr.classification_values(versionid, valueid)
	on delete cascade;

create table xr.classifications (
	objectversionid	varchar(48) not null,
	valueid		varchar(48) not null
	);

create view  xr.classificationcurrentvalueview (
	valueid,
	value
) as select 
	v.valueid,
	v.value
from  xr.classification_values v, xr.repositoryobjectresource ror
where v.versionid = ror.currentversion;

create view  xr.classificationschemecurrentversionview (
	id,
	versionid,
	name,
	uname,
	versionName,
	uri,
	versionuri,
	folderid,
	ownerid,
	ownerdeletedate,
	ownername,
	uownername,
	ownerauthid,
	creatorname,
	description,
	versionComment,
	objecttype,
	subtypeid,
	subtype,
	structuretype,
	createdate,
	versiondate,
	changedate,
	deletedate,
	versioncontrolled,
	currentversion,
	previousversion,	
	checkedout
) as select 
	ror.id,
	rov.id,
	rov.name,
	rov.uname,
	rov.versionName,
	rov.uri,
	rov.versionuri,
	rov.folderid,
	rov.ownerid,
	beo.deletedate,
	beo.name,
	beo.uname,
	beo.authid,
	bec.name,
	rov.description,
	rov.versionComment,
	ror.type,
	ror.subtype,
	cvtype.value,
	cls.structuretype,
	ror.createdate,
	rov.versiondate,
	rov.changedate,
	rov.deletedate,
	ror.versioncontrolled,
	ror.currentversion,
	rov.previousversion,	
	ror.checkedout
from  xr.repositoryobjectresource ror
	inner join xr.classificationscheme cls on (ror.id = cls.id)
	inner join xr.repositoryobjectversion rov on (ror.currentversion = rov.id)
	left outer join xr.businessentity beo on (rov.ownerid = beo.id)
	left outer join xr.businessentity bec on (rov.creatorid = bec.id)
	left outer join xr.classificationcurrentvalueview cvtype on (ror.subtype = cvtype.valueid)
	where rov.deletedate is null;

create view  xr.classificationschemeallversionview (
	id,
	versionid,
	name,
	uname,
	versionName,
	uri,
	versionuri,
	folderid,
	ownerid,
	ownerdeletedate,
	ownername,
	uownername,
	ownerauthid,
	creatorname,
	description,
	versionComment,
	objecttype,
	subtypeid,
	subtype,
	structuretype,
	createdate,
	versiondate,
	changedate,
	deletedate,
	versioncontrolled,
	currentversion,
	previousversion,	
	checkedout
) as select 
	ror.id,
	rov.id,
	rov.name,
	rov.uname,
	rov.versionName,
	rov.uri,
	rov.versionuri,
	rov.folderid,
	rov.ownerid,
	beo.deletedate,
	beo.name,
	beo.uname,
	beo.authid,
	bec.name,
	rov.description,
	rov.versionComment,
	ror.type,
	ror.subtype,
	cvtype.value,
	cls.structuretype,
	ror.createdate,
	rov.versiondate,
	rov.changedate,
	rov.deletedate,
	ror.versioncontrolled,
	ror.currentversion,
	rov.previousversion,	
	ror.checkedout
from  xr.repositoryobjectresource ror
	inner join xr.classificationscheme cls on (ror.id = cls.id)
	inner join xr.repositoryobjectversion rov on (ror.id = rov.resourceid)
	left outer join xr.businessentity beo on (rov.ownerid = beo.id)
	left outer join xr.businessentity bec on (rov.creatorid = bec.id)
	left outer join xr.classificationcurrentvalueview cvtype on (ror.subtype = cvtype.valueid)
	where rov.deletedate is null;

create view  xr.classificationschemelifecycleview (
	id,
	versionid,
	name,
	versionName,
	uri,
	versionuri,
	folderid,
	ownerid,
	ownerdeletedate,
	ownerauthid,
	ownername,
	description,
	objecttype,
	subtypeid,
	subtype,
	structuretype,
	checkforlock,
	createdate,
	versiondate,
	changedate,
	deletedate,
	versioncontrolled,
	currentversion,
	previousversion,
	checkedout
) as select 
	ror.id,
	rov.id,
	rov.name,
	rov.versionName,
	rov.uri,
	rov.versionuri,
	rov.folderid,
	rov.ownerid,
	beo.deletedate,
	beo.authid,
	beo.name,
	rov.description,
	ror.type,
	ror.subtype,
	cvtype.value,
	cls.structuretype,
	ror.checkforlock,
	ror.createdate,
	rov.versiondate,
	rov.changedate,
	rov.deletedate,
	ror.versioncontrolled,
	ror.currentversion,
	rov.previousversion,
	ror.checkedout
from  xr.repositoryobjectresource ror
	inner join xr.classificationscheme cls on (ror.id = cls.id)
	inner join xr.repositoryobjectversion rov on (ror.currentversion = rov.id)
	left outer join xr.businessentity beo on (rov.ownerid = beo.id)
	left outer join xr.classificationcurrentvalueview cvtype on (ror.subtype = cvtype.valueid);

create view  xr.classificationvalueview (
	id,
	value,
	description,
	parentid,
	parentvalue,
	schemeid,
	schemeversionid,
	schemename
) as select 
	v.valueid,
	v.value,
	v.description,
	p.valueid,
	p.value,
	rov.resourceid,
	rov.id,
	rov.name	
from  xr.classification_values v 
	inner join xr.repositoryobjectversion rov on (v.versionid = rov.id)
	left outer join xr.classification_values p on (v.parentvalueid = p.valueid)
where rov.deletedate is null;

create view  xr.classification_direct_view (
	objectversionid,
	valueid,
	schemeversionid,
	value
) as select 
	c.objectversionid,
	cv.valueid,
	cv.versionid,
	cv.value
from  xr.classifications c, xr.classification_values cv, 
	xr.repositoryobjectresource ror
	where c.valueid = cv.valueid and cv.versionid = ror.currentversion;

create view  xr.classification_indirect_view (
	objectversionid,
	valueid,
	schemeversionid,
	value
) as select 
	c.objectversionid,
	cv.valueid,
	cv.versionid,
	cv.value
from  xr.classifications c, xr.classification_values cv, 
	xr.classification_value_ancestors cva, xr.repositoryobjectresource ror
	where c.valueid = cva.valueid and 
	cva.ancestorid = cv.valueid and cva.versionid = cv.versionid and
	cv.versionid = ror.currentversion;

create view  xr.businessentityqueryview (
	id,
	name,
	uname,
	type,
	createdate,
	changedate,
	description,
	authID,	
	ownerid,
	ownername,
	uownername,
	businessTypeID,
	businessType
) as select 
	be.id, 
	be.name,
	be.uname,
	be.type,
	be.createdate,
	be.changedate,
	be.description,  
	be.authID , 
	o.id,  
	o.name, 
	o.uname,
	be.businessTypeID, 
	cv.value 
from  xr.businessentity be left outer join xr.businessentity o on be.owneruserUUID = o.id  
left outer join xr.classificationcurrentvalueview cv on cv.valueid = be.businessTypeID 
where be.deletedate is null and o.deletedate is null;

create view  xr.businessassociationqueryview (
	id,
	sourceid,
	sourcename,
	usourcename,
	sourcetype,
	targetid,
	targetname,
	utargetname,
	targettype,
	createdate,
	changedate,
	description,
	ownerid,
	ownername,
	uownername,
	associationTypeID,
	associationType
) as select 
	bea.id,
	bea.sourceid,
	s.name,
	s.uname,
	s.type,
	bea.targetid, 
	t.name,
	t.uname,
	t.type,
	bea.createdate,
	bea.changedate,
	bea.description,  
	o.id,  
	o.name, 
	o.uname,
	bea.assoctypeID, 
	cv.value 	
from  xr.businessentityassociation bea 
left outer join xr.businessentity s on bea.sourceID = s.ID 
left outer join xr.businessentity t on bea.targetID = t.ID 
left outer join xr.businessentity o on bea.ownerID = o.ID  
left outer join xr.classificationcurrentvalueview cv on cv.valueid = bea.assoctypeID 
where bea.deletedate is null and s.deletedate is null and t.deletedate is null and o.deletedate is null;

create view  xr.repositoryobjectcurrentversionview (
	id, versionid, name, uname, versionName, 
	uri, versionuri, folderid, 
	ownerid, ownerdeletedate, ownername, uownername, ownerauthid, creatorname,
	description, versionComment, objecttype, subtypeid, subtype, 
	checkforlock, createdate, versiondate, changedate, deletedate,
	versioncontrolled, currentversion, previousversion, checkedout,
	-- from RepositoryEntry
	stability, statusid, status, isopaque,
	startdate, expirationdate, contentchangedate, versioncontentid,
	-- from ObjectAssociation
	sourceid, targetid,
	-- from ClassificationScheme
	structuretype
) as select 
	ror.id, rov.id, rov.name, rov.uname, rov.versionName, 
	rov.uri, rov.versionuri, rov.folderid,
	rov.ownerid, beo.deletedate, beo.name, beo.uname, beo.authid, bec.name,
	rov.description, rov.versionComment, ror.type, ror.subtype, cvsubt.value,  
	ror.checkforlock, ror.createdate, rov.versiondate, rov.changedate, rov.deletedate,
	ror.versioncontrolled, ror.currentversion, rov.previousversion, ror.checkedout,
	-- from RepositoryEntry
	re.stability, re.status, cvstatus.value, re.isopaque,
	re.startdate, re.expirationdate, rec.contentchangedate, re.versioncontentid,
	-- from ObjectAssociation
	oa.sourceid, oa.targetid,
	-- from ClassificationScheme
	cs.structuretype
from  xr.repositoryobjectresource ror
	inner join xr.repositoryobjectversion rov on (ror.currentversion = rov.id)
	left outer join xr.classificationcurrentvalueview cvsubt on (ror.subtype = cvsubt.valueid)
	left outer join xr.businessentity beo on (rov.ownerid = beo.id)
	left outer join xr.businessentity bec on (rov.creatorid = bec.id)
	left outer join xr.repositoryentry re on (ror.currentversion = re.versionid)
	left outer join xr.repositoryentrycontent rec on (re.versioncontentid = rec.versionid)
	left outer join xr.classificationcurrentvalueview cvstatus on (re.status = cvstatus.valueid)
	left outer join xr.objectassociation oa on (ror.id = oa.id)
	left outer join xr.classificationscheme cs on (ror.id = cs.id)
	where rov.deletedate is null;

create view  xr.repositoryobjectallversionview (
	id, versionid, name, uname, versionName, 
	uri, versionuri, folderid, 
	ownerid, ownerdeletedate, ownername, uownername, ownerauthid, creatorname,
	description, versionComment, objecttype, subtypeid, subtype, 
	checkforlock, createdate, versiondate, changedate, deletedate,
	versioncontrolled, currentversion, previousversion, checkedout, 
	-- from RepositoryEntry
	stability, statusid, status, isopaque,
	startdate, expirationdate, contentchangedate, versioncontentid,
	-- from ObjectAssociation
	sourceid, targetid,
	-- from ClassificationScheme
	structuretype
) as select 
	ror.id, rov.id, rov.name, rov.uname, rov.versionName, 
	rov.uri, rov.versionuri, rov.folderid,
	rov.ownerid, beo.deletedate, beo.name, beo.uname, beo.authid, bec.name,
	rov.description, rov.versionComment, ror.type, ror.subtype, cvsubt.value,  
	ror.checkforlock, ror.createdate, rov.versiondate, rov.changedate, rov.deletedate,
	ror.versioncontrolled, ror.currentversion, rov.previousversion, ror.checkedout, 
	-- from RepositoryEntry
	re.stability, re.status, cvstatus.value, re.isopaque,
	re.startdate, re.expirationdate, rec.contentchangedate, re.versioncontentid,
	-- from ObjectAssociation
	oa.sourceid, oa.targetid,
	-- from ClassificationScheme
	cs.structuretype
from  xr.repositoryobjectresource ror
	inner join xr.repositoryobjectversion rov on (ror.id = rov.resourceid)
	left outer join xr.classificationcurrentvalueview cvsubt on (ror.subtype = cvsubt.valueid)
	left outer join xr.businessentity beo on (rov.ownerid = beo.id)
	left outer join xr.businessentity bec on (rov.creatorid = bec.id)
	left outer join xr.repositoryentry re on (ror.currentversion = re.versionid)
	left outer join xr.repositoryentrycontent rec on (re.versioncontentid = rec.versionid)
	left outer join xr.classificationcurrentvalueview cvstatus on (re.status = cvstatus.valueid)
	left outer join xr.objectassociation oa on (ror.id = oa.id)
	left outer join xr.classificationscheme cs on (ror.id = cs.id)
	where rov.deletedate is null;

create view  xr.repositoryobjectlifecycleview (
	id,
	versionid,
	name,
	versionName,
	uri,
	versionuri,
	folderid,
	ownerid,
	ownerauthid,
	ownername,
	ownerdeletedate,
	description,
	objecttype,
	subtypeid,
	checkforlock,
	createdate,
	versiondate,
	changedate,
	deletedate,
	versioncontrolled,
	checkedout,
	currentversion,
	previousversion
) as select 
	ror.id,
	rov.id,
	rov.name,
	rov.versionName,
	rov.uri,
	rov.versionuri,
	rov.folderid,
	rov.ownerid,
	beo.authid,
	beo.name,
	beo.deletedate,
	rov.description,
	ror.type,
	ror.subtype,
	ror.checkforlock,
	ror.createdate,
	rov.versiondate,
	rov.changedate,
	rov.deletedate,
	ror.versioncontrolled,
	ror.checkedout,
	ror.currentversion,
	rov.previousversion
from  xr.repositoryobjectresource ror
	inner join xr.repositoryobjectversion rov on (ror.currentversion = rov.id)
	left outer join xr.businessentity beo on (rov.ownerid = beo.id);
	
create view  xr.repositoryobjectlabelview (
	id,
	versionid,
	name,
	versionName,
	uri,
	versionuri,
	folderid,
	ownerid,
	ownerauthid,
	ownername,
	ownerdeletedate,
	description,
	objecttype,
	subtypeid,
	checkforlock,
	createdate,
	versiondate,
	changedate,
	deletedate,
	versioncontrolled,
	checkedout,
	currentversion,
	previousversion
) as select 
	ror.id,
	rov.id,
	rov.name,
	rov.versionName,
	rov.uri,
	rov.versionuri,
	rov.folderid,
	rov.ownerid,
	beo.authid,
	beo.name,
	beo.deletedate,
	rov.description,
	ror.type,
	ror.subtype,
	ror.checkforlock,
	ror.createdate,
	rov.versiondate,
	rov.changedate,
	rov.deletedate,
	ror.versioncontrolled,
	ror.checkedout,
	ror.currentversion,
	rov.previousversion
from  xr.repositoryobjectresource ror
	inner join xr.repositoryobjectversion rov on (ror.id = rov.resourceid)
	left outer join xr.businessentity beo on (rov.ownerid = beo.id);

create view  xr.repositoryentrycurrentversionview (
	id,
	versionid,
	name,
	uname,
	versionName,
	uri,
	versionuri,
	versioncontentid,
	folderid,
	ownerid,
	ownerdeletedate,
	ownername,
	uownername,
	ownerauthid,
	creatorname,
	description,
	versionComment,
	objecttype,
	createdate,
	versiondate,
	changedate,
	contentchangedate,
	deletedate,
	versioncontrolled,
	currentversion,
	previousversion,
	checkedout,
	subtypeid,
	subtype,
	stability,
	statusid,
	status,
	startdate,
	expirationdate,
	isopaque
) as select 
	ror.id,
	rov.id,
	rov.name,
	rov.uname,
	rov.versionName,
	rov.uri,
	rov.versionuri,
	re.versioncontentid,
	rov.folderid,
	rov.ownerid,
	beo.deletedate,
	beo.name,
	beo.uname,
	beo.authid,
	bec.name,
	rov.description,
	rov.versionComment,
	ror.type,
	ror.createdate,
	rov.versiondate,
	rov.changedate,
	rec.contentchangedate,
	rov.deletedate,
	ror.versioncontrolled,
	ror.currentversion,
	rov.previousversion,
	ror.checkedout,
	re.mimetype,
	cvmime.value,
	re.stability,
	re.status,
	cvstatus.value,
	re.startdate,
	re.expirationdate,
	re.isopaque
from  xr.repositoryobjectresource ror
	inner join xr.repositoryobjectversion rov on (ror.currentversion = rov.id)
	inner join xr.repositoryentry re on (rov.id = re.versionid)
	left outer join xr.repositoryentrycontent rec on (re.versioncontentid = rec.versionid)
	left outer join xr.businessentity beo on (rov.ownerid = beo.id)
	left outer join xr.businessentity bec on (rov.creatorid = bec.id)
	left outer join xr.classificationcurrentvalueview cvmime on (re.mimetype = cvmime.valueid)
	left outer join xr.classificationcurrentvalueview cvstatus on (re.status = cvstatus.valueid)
	where rov.deletedate is null;

create view  xr.repositoryentryallversionview (
	id,
	versionid,
	name,
	uname,
	versionName,
	uri,
	versionuri,
	versioncontentid,
	folderid,
	ownerid,
	ownerdeletedate,
	ownername,
	uownername,
	ownerauthid,
	creatorname,
	description,
	versionComment,
	objecttype,
	createdate,
	versiondate,
	changedate,
	contentchangedate,
	deletedate,
	versioncontrolled,
	currentversion,
	previousversion,
	checkedout,
	subtypeid,
	subtype,
	stability,
	statusid,
	status,
	startdate,
	expirationdate,
	isopaque
) as select 
	ror.id,
	rov.id,
	rov.name,
	rov.uname,
	rov.versionName,
	rov.uri,
	rov.versionuri,
	re.versioncontentid,
	rov.folderid,
	rov.ownerid,
	beo.deletedate,
	beo.name,
	beo.uname,
	beo.authid,
	bec.name,
	rov.description,
	rov.versionComment,
	ror.type,
	ror.createdate,
	rov.versiondate,
	rov.changedate,
	rec.contentchangedate,
	rov.deletedate,
	ror.versioncontrolled,
	ror.currentversion,		
	rov.previousversion,
	ror.checkedout,
	re.mimetype,
	cvmime.value,
	re.stability,
	re.status,
	cvstatus.value,
	re.startdate,
	re.expirationdate,
	re.isopaque
from  xr.repositoryobjectresource ror
	inner join xr.repositoryobjectversion rov on (ror.id = rov.resourceid)
	inner join xr.repositoryentry re on (rov.id = re.versionid)
	left outer join xr.repositoryentrycontent rec on (re.versioncontentid = rec.versionid)	
	left outer join xr.businessentity beo on (rov.ownerid = beo.id)
	left outer join xr.businessentity bec on (rov.creatorid = bec.id)
	left outer join xr.classificationcurrentvalueview cvmime on (re.mimetype = cvmime.valueid)
	left outer join xr.classificationcurrentvalueview cvstatus on (re.status = cvstatus.valueid)
	where rov.deletedate is null;

create view  xr.repositoryentrylifecycleview (
	id,
	versionid,
	name,
	versionName,
	uri,
	versionuri,
	versioncontentid,
	folderid,
	ownerid,
	ownerdeletedate,
	ownerauthid,
	ownername,
	description,
	objecttype,
	subtypeid,
	subtype,
	checkforlock,
	createdate,
	versiondate,
	changedate,
	contentchangedate,
	deletedate,
	versioncontrolled,
	checkedout,
	currentversion,
	previousversion	
) as select 
	ror.id,
	rov.id,
	rov.name,
	rov.versionName,
	rov.uri,
	rov.versionuri,
	re.versioncontentid,
	rov.folderid,
	rov.ownerid,
	beo.deletedate,
	beo.authid,
	beo.name,
	rov.description,
	ror.type,
	re.mimetype,
	cvmime.value,
	ror.checkforlock,
	ror.createdate,
	rov.versiondate,
	rov.changedate,
	rec.contentchangedate,
	rov.deletedate,
	ror.versioncontrolled,
	ror.checkedout,
	ror.currentversion,
	rov.previousversion
from  xr.repositoryobjectresource ror
	inner join xr.repositoryobjectversion rov on (ror.currentversion = rov.id)
	inner join xr.repositoryentry re on (rov.id = re.versionid)
	left outer join xr.repositoryentrycontent rec on (re.versioncontentid = rec.versionid)
	left outer join xr.businessentity beo on (rov.ownerid = beo.id)
	left outer join xr.classificationcurrentvalueview cvmime on (re.mimetype = cvmime.valueid);

create view  xr.repositoryentrycontentview (
	id,
	versionid,
	versioncontentid,
	uri,
	versionuri,
	content,
	contentchangedate,
	currentversion,
	mimetypeid,
	mimetype
) as select 
	ror.id,
	rov.id,
	re.versioncontentid,
	rov.uri,
	rov.versionuri,
	rec.content,
	rec.contentchangedate,
	ror.currentversion,
	re.mimetype,
	cvmime.value
from  xr.repositoryobjectresource ror
	inner join xr.repositoryobjectversion rov on (ror.id = rov.resourceid)
	inner join xr.repositoryentry re on (rov.id = re.versionid)
	left outer join xr.repositoryentrycontent rec on (re.versioncontentid = rec.versionid)	
	left outer join xr.classificationcurrentvalueview cvmime on (re.mimetype = cvmime.valueid)
	left outer join xr.classificationcurrentvalueview cvstatus on (re.status = cvstatus.valueid)
	where rov.deletedate is null;

create view  xr.objectgroupcurrentversionview (
	id,
	versionid,
	name,
	uname,
	versionName,
	uri,
	versionuri,
	folderid,
	ownerid,
	ownerdeletedate,
	ownername,
	uownername,
	ownerauthid,
	creatorname,
	description,
	versionComment,
	objecttype,
	subtypeid,
	subtype,
	createdate,
	versiondate,
	changedate,
	deletedate,
	versioncontrolled,
	currentversion,
	previousversion,	
	checkedout
) as select 
	ror.id,
	rov.id,
	rov.name,
	rov.uname,
	rov.versionName,
	rov.uri,
	rov.versionuri,
	rov.folderid,
	rov.ownerid,
	beo.deletedate,
	beo.name,
	beo.uname,
	beo.authid,
	bec.name,
	rov.description,
	rov.versionComment,
	ror.type,
	ror.subtype,
	cvtype.value,
	ror.createdate,
	rov.versiondate,
	rov.changedate,
	rov.deletedate,
	ror.versioncontrolled,
	ror.currentversion,
	rov.previousversion,	
	ror.checkedout
from  xr.repositoryobjectresource ror
	inner join xr.repositoryobjectversion rov on (ror.currentversion = rov.id)
	left outer join xr.businessentity beo on (rov.ownerid = beo.id)
	left outer join xr.businessentity bec on (rov.creatorid = bec.id)
	left outer join xr.classificationcurrentvalueview cvtype on (ror.subtype = cvtype.valueid)
	where ror.type = 'COLLECTION' and rov.deletedate is null;

create view  xr.objectgroupallversionview (
	id,
	versionid,
	name,
	uname,
	versionName,
	uri,
	versionuri,
	folderid,
	ownerid,
	ownerdeletedate,
	ownername,
	uownername,
	ownerauthid,
	creatorname,
	description,
	versionComment,
	objecttype,
	subtypeid,
	subtype,
	createdate,
	versiondate,
	changedate,
	deletedate,
	versioncontrolled,
	currentversion,
	previousversion,	
	checkedout
) as select 
	ror.id,
	rov.id,
	rov.name,
	rov.uname,
	rov.versionName,
	rov.uri,
	rov.versionuri,
	rov.folderid,
	rov.ownerid,
	beo.deletedate,
	beo.name,
	beo.uname,
	beo.authid,
	bec.name,
	rov.description,
	rov.versionComment,
	ror.type,
	ror.subtype,
	cvtype.value,
	ror.createdate,
	rov.versiondate,
	rov.changedate,
	rov.deletedate,
	ror.versioncontrolled,
	ror.currentversion,
	rov.previousversion,	
	ror.checkedout
from  xr.repositoryobjectresource ror
	inner join xr.repositoryobjectversion rov on (ror.id = rov.resourceid)
	left outer join xr.businessentity beo on (rov.ownerid = beo.id)
	left outer join xr.businessentity bec on (rov.creatorid = bec.id)
	left outer join xr.classificationcurrentvalueview cvtype on (ror.subtype = cvtype.valueid)
	where ror.type = 'COLLECTION' and rov.deletedate is null;

create view  xr.objectgrouplifecycleview (
	id,
	versionid,
	name,
	versionName,
	uri,
	versionuri,
	folderid,
	ownerid,
	ownerdeletedate,
	ownerauthid,
	ownername,
	description,
	objecttype,
	subtypeid,
	subtype,
	checkforlock,
	createdate,
	versiondate,
	changedate,
	deletedate,
	versioncontrolled,
	checkedout,
	currentversion,
	previousversion
) as select 
	ror.id,
	rov.id,
	rov.name,
	rov.versionName,
	rov.uri,
	rov.versionuri,
	rov.folderid,
	rov.ownerid,
	beo.deletedate,
	beo.authid,
	beo.name,
	rov.description,
	ror.type,
	ror.subtype,
	cvtype.value,
	ror.checkforlock,
	ror.createdate,
	rov.versiondate,
	rov.changedate,
	rov.deletedate,
	ror.versioncontrolled,
	ror.checkedout,
	ror.currentversion,
	rov.previousversion
from  xr.repositoryobjectresource ror
	inner join xr.repositoryobjectversion rov on (ror.currentversion = rov.id)
	left outer join xr.businessentity beo on (rov.ownerid = beo.id)
	left outer join xr.classificationcurrentvalueview cvtype on (ror.subtype = cvtype.valueid)
	where ror.type = 'COLLECTION';

create view  xr.externaldependencycurrentversionview (
	id,
	versionid,
	name,
	uname,
	versionName,
	uri,
	versionuri,
	folderid,
	ownerid,
	ownerdeletedate,
	ownername,
	uownername,
	ownerauthid,
	creatorname,
	description,
	versionComment,
	objecttype,
	subtypeid,
	subtype,
	createdate,
	versiondate,
	changedate,
	deletedate,
	versioncontrolled,
	checkedout,
	currentversion,
	previousversion
) as select 
	ror.id,
	rov.id,
	rov.name,
	rov.uname,
	rov.versionName,
	rov.uri,
	rov.versionuri,
	rov.folderid,
	rov.ownerid,
	beo.deletedate,
	beo.name,
	beo.uname,
	beo.authid,
	bec.name,
	rov.description,
	rov.versionComment,
	ror.type,
	ror.subtype,
	cvtype.value,
	ror.createdate,
	rov.versiondate,
	rov.changedate,
	rov.deletedate,
	ror.versioncontrolled,
	ror.checkedout,
	ror.currentversion,
	rov.previousversion
from  xr.repositoryobjectresource ror
	inner join xr.repositoryobjectversion rov on (ror.currentversion = rov.id)
	left outer join xr.businessentity beo on (rov.ownerid = beo.id)
	left outer join xr.businessentity bec on (rov.creatorid = bec.id)
	left outer join xr.classificationcurrentvalueview cvtype on (ror.subtype = cvtype.valueid)
	where ror.type = 'EXTERNAL_DEPENDENCY' and rov.deletedate is null;

create view  xr.externaldependencyallversionview (
	id,
	versionid,
	name,
	uname,
	versionName,
	uri,
	versionuri,	
	folderid,
	ownerid,
	ownerdeletedate,
	ownername,
	uownername,
	ownerauthid,
	creatorname,
	description,
	versionComment,
	objecttype,
	dependencytypeid,
	dependencytype,
	createdate,
	versiondate,
	changedate,
	deletedate,
	versioncontrolled,	
	checkedout,
	currentversion,
	previousversion	
) as select 
	ror.id,
	rov.id,
	rov.name,
	rov.uname,
	rov.versionName,
	rov.uri,
	rov.versionuri,
	rov.folderid,
	rov.ownerid,
	beo.deletedate,
	beo.name,
	beo.uname,
	beo.authid,
	bec.name,
	rov.description,
	rov.versionComment,
	ror.type,
	ror.subtype,
	cvtype.value,
	ror.createdate,
	rov.versiondate,
	rov.changedate,
	rov.deletedate,
	ror.versioncontrolled,
	ror.checkedout,
	ror.currentversion,
	rov.previousversion	
from  xr.repositoryobjectresource ror
	inner join xr.repositoryobjectversion rov on (ror.id = rov.resourceid)
	left outer join xr.businessentity beo on (rov.ownerid = beo.id)
	left outer join xr.businessentity bec on (rov.creatorid = bec.id)
	left outer join xr.classificationcurrentvalueview cvtype on (ror.subtype = cvtype.valueid)
	where ror.type = 'EXTERNAL_DEPENDENCY' and rov.deletedate is null;

create view  xr.objectassociationcurrentversionview (
	id,
	versionid,
	name,
	uname,
	versionName,
	uri,
	versionuri,
	folderid,
	ownerid,
	ownerdeletedate,
	ownername,
	uownername,
	ownerauthid,
	creatorname,
	description,
	versionComment,
	objecttype,
	subtypeid,
	subtype,
	createdate,
	versiondate,
	changedate,
	deletedate,
	versioncontrolled,
	currentversion,
	previousversion,	
	checkedout,
	sourceid,
	targetid
) as select 
	ror.id,
	rov.id,
	rov.name,
	rov.uname,
	rov.versionName,
	rov.uri,
	rov.versionuri,
	rov.folderid,
	rov.ownerid,
	beo.deletedate,
	beo.name,
	beo.uname,
	beo.authid,
	bec.name,
	rov.description,
	rov.versionComment,
	ror.type,
	ror.subtype,
	cvtype.value,
	ror.createdate,
	rov.versiondate,
	rov.changedate,
	rov.deletedate,
	ror.versioncontrolled,
	ror.currentversion,
	rov.previousversion,	
	ror.checkedout,
	oa.sourceid,
	oa.targetid
from  xr.repositoryobjectresource ror
	inner join xr.repositoryobjectversion rov on (ror.currentversion = rov.id)
	inner join xr.objectassociation oa on (ror.id = oa.id)
	left outer join xr.businessentity beo on (rov.ownerid = beo.id)
	left outer join xr.businessentity bec on (rov.creatorid = bec.id)
	left outer join xr.classificationcurrentvalueview cvtype on (ror.subtype = cvtype.valueid)
	where rov.deletedate is null;

create view  xr.objectassociationallversionview (
	id,
	versionid,
	name,
	uname,
	versionName,
	uri,
	versionuri,
	folderid,
	ownerid,
	ownerdeletedate,
	ownername,
	uownername,
	ownerauthid,
	creatorname,
	description,
	versionComment,
	objecttype,
	subtypeid,
	subtype,
	createdate,
	versiondate,
	changedate,
	deletedate,
	versioncontrolled,
	currentversion,
	previousversion,	
	checkedout,
	sourceid,
	targetid
) as select 
	ror.id,
	rov.id,
	rov.name,
	rov.uname,
	rov.versionName,
	rov.uri,
	rov.versionuri,
	rov.folderid,
	rov.ownerid,
	beo.deletedate,
	beo.name,
	beo.uname,
	beo.authid,
	bec.name,
	rov.description,
	rov.versionComment,
	ror.type,
	ror.subtype,
	cvtype.valueid,
	ror.createdate,
	rov.versiondate,
	rov.changedate,
	rov.deletedate,
	ror.versioncontrolled,
	ror.currentversion,
	rov.previousversion,	
	ror.checkedout,
	oa.sourceid,
	oa.targetid
from  xr.repositoryobjectresource ror
	inner join xr.repositoryobjectversion rov on (ror.id = rov.resourceid)
	inner join xr.objectassociation oa on (ror.id = oa.id)
	left outer join xr.businessentity beo on (rov.ownerid = beo.id)
	left outer join xr.businessentity bec on (rov.creatorid = bec.id)
	left outer join xr.classificationcurrentvalueview cvtype on (ror.subtype = cvtype.valueid)
	where rov.deletedate is null;

create view  xr.objectassociationlifecycleview (
	id,
	versionid,
	name,
	versionName,
	uri,
	versionuri,
	folderid,
	ownerid,
	ownerdeletedate,
	ownerauthid,
	ownername,
	description,
	objecttype,
	subtypeid,
	subtype,
	checkforlock,
	createdate,
	versiondate,
	changedate,
	deletedate,
	versioncontrolled,
	checkedout,
	currentversion,
	previousversion,
	sourceid,
	targetid
) as select 
	ror.id,
	rov.id,
	rov.name,
	rov.versionName,
	rov.uri,
	rov.versionuri,
	rov.folderid,
	rov.ownerid,
	beo.deletedate,
	beo.authid,
	beo.name,
	rov.description,
	ror.type,
	ror.subtype,
	cvtype.value,
	ror.checkforlock,
	ror.createdate,
	rov.versiondate,
	rov.changedate,
	rov.deletedate,
	ror.versioncontrolled,
	ror.checkedout,
	ror.currentversion,
	rov.previousversion,
	oa.sourceid,
	oa.targetid
from  xr.repositoryobjectresource ror
	inner join xr.repositoryobjectversion rov on (ror.currentversion = rov.id)
	inner join xr.objectassociation oa on (ror.id = oa.id)
	left outer join xr.businessentity beo on (rov.ownerid = beo.id)
	left outer join xr.classificationcurrentvalueview cvtype on (ror.subtype = cvtype.valueid);

create view  xr.objectassociationcurrentversionqueryview (
	id,
	versionid,
	name,
	uname,
	versionName,
	uri,
	versionuri,
	folderid,
	ownerid,
	ownerdeletedate,
	ownername,
	uownername,
	ownerauthid,
	creatorname,
	description,
	versionComment,
	objecttype,
	subtypeid,
	subtype,
	createdate,
	versiondate,
	changedate,
	deletedate,
	versioncontrolled,
	currentversion,
	previousversion,	
	checkedout,
	sourceid,
	sourcename,
	usourcename,
	sourcetype,
	targetid,
	targetname,
	utargetname,
	targettype
) as select 
	ror.id,
	rov.id,
	rov.name,
	rov.uname,
	rov.versionName,
	rov.uri,
	rov.versionuri,
	rov.folderid,
	rov.ownerid,
	beo.deletedate,
	beo.name,
	beo.uname,
	beo.authid,
	bec.name,
	rov.description,
	rov.versionComment,
	ror.type,
	ror.subtype,
	cvtype.value,
	ror.createdate,
	rov.versiondate,
	rov.changedate,
	rov.deletedate,
	ror.versioncontrolled,
	ror.currentversion,
	rov.previousversion,	
	ror.checkedout,
	oa.sourceid,
	s.name,
	s.uname,
	s.objecttype,
	oa.targetid,
	t.name,
	t.uname,
	t.objecttype	
from  xr.repositoryobjectresource ror
	inner join xr.repositoryobjectversion rov on (ror.currentversion = rov.id)
	inner join xr.objectassociation oa on (ror.id = oa.id)
	left outer join xr.repositoryobjectcurrentversionview s on (oa.sourceid = s.id)
	left outer join xr.repositoryobjectcurrentversionview t on (oa.targetid = t.id)		
	left outer join xr.businessentity beo on (rov.ownerid = beo.id)
	left outer join xr.businessentity bec on (rov.creatorid = bec.id)
	left outer join xr.classificationcurrentvalueview cvtype on (ror.subtype = cvtype.valueid)
	where rov.deletedate is null;

create view  xr.lockview (
	locktoken,
	resourceid,
	resourcename,
	userid,
	username,
	exclusive,
	timeoutseconds,
	expirationdate
) as select 
	l.locktoken,
	l.resourceid,
	rov.name,
	l.ownerid,
	be.name,
	l.exclusive,
	l.timeoutseconds,
	l.expirationdate
from  xr.lock l, xr.repositoryobjectresource ror, xr.repositoryobjectversion rov, 
	xr.businessentity be 	
where l.resourceid = ror.id and ror.currentversion = rov.id and l.ownerid = be.id;

--echo === Create Users ================================================;

-- if we don't commit, the following insert will block due to fix of beetle 4821
commit;

-- The following insert statement shouldn't block for 2 minutes!! The compile of the trigger
-- shouldn't wait for timeout!!
insert into xr.businessentity ( ID, type, name, authID, createdate, changedate )
	values ('26747154-0dfc-46af-a85d-1dc30c230c4e', 
		'USER', 
		'Administrator', 		
		'xradmin', 
		CURRENT TIMESTAMP, CURRENT TIMESTAMP);
insert into xr.xruser 
		(businessentityid, 
		userid)
	values ('26747154-0dfc-46af-a85d-1dc30c230c4e',	
		'xradmin');


insert into xr.businessentity ( ID, type, name, authID, createdate, changedate )
	values ('013026c8-1b22-487a-a189-1c7b16811035', 
		'USER', 
		'Sample XR User', 
		'xrguest', CURRENT TIMESTAMP, CURRENT TIMESTAMP);
insert into xr.xruser (businessentityid, userid)
	values ('013026c8-1b22-487a-a189-1c7b16811035',	
		'xrguest');

--echo == Create locators ==============================================;

insert into xr.locator ( 
	ownerID,
	type, 
	usetypeID, 
	value, 
	street1, 
	street2, 
	city, 
	state, 
	country, 
	postalcode
) values (
	'26747154-0dfc-46af-a85d-1dc30c230c4e', 
	'EMAIL',
	'67c249a4-d160-11d6-bb9c-646533376c37',
	'xradmin@xr.com',
	'',
	'',
	'',
	'',
	'',
	'');


insert into xr.locator ( 
	ownerID,
	type, 
	usetypeID, 
	value, 
	street1, 
	street2, 
	city, 
	state, 
	country, 
	postalcode
) values (
	'013026c8-1b22-487a-a189-1c7b16811035', 
	'EMAIL',
	'67c249a4-d160-11d6-bb9c-646533376c37',
	'xrguest@yourmail.com',
	'',
	'',
	'',
	'',
	'',
	'');

--echo =============================================================================;
--echo xr database insert script
--echo =============================================================================;

--XRADMIN ID '26747154-0dfc-46af-a85d-1dc30c230c4e' 
-- repositoryobjectversion.folderid references xr.repositoryobjectresource(id)
--ROOT FolderID foreign '225924f8-1a72-42c9-a58d-05b41d8415ce' 


--echo == ROOT Folder ====================================;
--echo == ROOT Folder ====================================;



insert into xr.repositoryobjectversion (
	id, resourceid, name, versionName, uri, versionuri, description,
	ownerid, creatorid, versiondate, changedate, previousversion
	) values (
	'077bb8c4-0894-4d99-937a-356c315d26e2',
	'225924f8-1a72-42c9-a58d-05b41d8415ce', 
	'', '1.0', '/',
	'1.0/',
	'Root XR folder',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP, CURRENT TIMESTAMP,
	null
	);

-- Root folder
insert into xr.repositoryobjectresource (
	id, type, subtype, creatorid, createdate, currentVersion, versionControlled
	) values (
	'225924f8-1a72-42c9-a58d-05b41d8415ce', 
	'COLLECTION', 
	'11a832a5-0bb1-46db-a000-147906f70021',
	'013026c8-1b22-487a-a189-1c7b16811035',
	CURRENT TIMESTAMP, 
	'077bb8c4-0894-4d99-937a-356c315d26e2', 
	0);

-- This ObjectGroup classified as INTERNAL_USE
insert into xr.classifications ( objectversionid, valueid )
	values ('077bb8c4-0894-4d99-937a-356c315d26e2', '196b15da-136f-4e19-933f-036f01481f5f');






--echo ==================================================;

--echo =============================================================================;
--echo xr database insert script
--echo =============================================================================;

--XRADMIN ID '26747154-0dfc-46af-a85d-1dc30c230c4e' 
-- repositoryobjectversion.folderid references xr.repositoryobjectresource(id)
--ROOT FolderID foreign '225924f8-1a72-42c9-a58d-05b41d8415ce' 



--echo == Business types =======================================;
--echo =========================================================;



insert into xr.repositoryobjectversion (
	id, 
	resourceid, 
	name, 
	versionName,
	folderid, 
	uri, 
	versionuri, 
	description,
	ownerid, 
	creatorid, 
	versiondate, 
	changedate, 
	previousversion
) values (
	'6e7fb600-d184-11d6-85ee-646533376c37',
	'717a3150-d184-11d6-85ee-646533376c37', 
	'Business Types', '1.0',  
	'225924f8-1a72-42c9-a58d-05b41d8415ce',
	'Business+Types',
	'1.0/Business+Types/1.0',
	'Valid values for the Business TYpe property of a Business',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP, 
	CURRENT TIMESTAMP,
	null
	);


insert into xr.repositoryobjectresource (
	id, 
	type, 
	subtype, 
	creatorid, 
	createdate, 
	currentVersion, 
	versionControlled
) values (
	'717a3150-d184-11d6-85ee-646533376c37', 
	'CLASSIFICATION_SCHEME', 
	'19adf2cf-0a2e-4d98-8f68-221708370b4c',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP,
	'6e7fb600-d184-11d6-85ee-646533376c37', 
	0);

-- This scheme goes into root folder
insert into xr.objectgroup_content (
	versionid,
	memberid
) values (
	'077bb8c4-0894-4d99-937a-356c315d26e2',
	'717a3150-d184-11d6-85ee-646533376c37'
);


insert into xr.classificationscheme (
	id, 
	structuretype
) values (
	'717a3150-d184-11d6-85ee-646533376c37', 
	'LIST');



-- This scheme classified as INTERNAL_USE
insert into xr.classifications ( 
	objectversionid, 
	valueid 
) values (
	'6e7fb600-d184-11d6-85ee-646533376c37',
	'196b15da-136f-4e19-933f-036f01481f5f');


--echo =======================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
) values ( 
	'6e7fb600-d184-11d6-85ee-646533376c37', 
	'717a3151-d184-11d6-85ee-646533376c37', 
	'CORPORATION',
	'CORPORATION');

--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'6e7fb600-d184-11d6-85ee-646533376c37', 
	'717bb7f0-d184-11d6-85ee-646533376c37', 
	'ORGANIZATION',
	'ORGANIZATION');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid
)values ( 
	'6e7fb600-d184-11d6-85ee-646533376c37', 
	'717bb7f0-d184-11d6-85ee-646533376c37', 
	'717bb7f0-d184-11d6-85ee-646533376c37');

--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'6e7fb600-d184-11d6-85ee-646533376c37', 
	'717bb7f3-d184-11d6-85ee-646533376c37', 
	'DIVISION',
	'DIVISION');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid 
) values ( 
	'6e7fb600-d184-11d6-85ee-646533376c37', 
	'717bb7f3-d184-11d6-85ee-646533376c37', 
	'717bb7f3-d184-11d6-85ee-646533376c37');

--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
) values ( 
	'6e7fb600-d184-11d6-85ee-646533376c37', 
	'717bb7f1-d184-11d6-85ee-646533376c37', 
	'GROUP',
	'GROUP');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid 
) values ( 
	'6e7fb600-d184-11d6-85ee-646533376c37', 
	'717bb7f1-d184-11d6-85ee-646533376c37', 
	'717bb7f1-d184-11d6-85ee-646533376c37');

--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
) values ( 
	'6e7fb600-d184-11d6-85ee-646533376c37', 
	'717bb7f2-d184-11d6-85ee-646533376c37', 
	'PARTNERSHIP',
	'PARTNERSHIP');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid 
) values ( 
	'6e7fb600-d184-11d6-85ee-646533376c37', 
	'717bb7f2-d184-11d6-85ee-646533376c37', 
	'717bb7f2-d184-11d6-85ee-646533376c37');

--echo ==========================================================;
--echo == Locator Use types ====================================;
--echo =========================================================;


insert into xr.repositoryobjectversion (
	id, 
	resourceid, 
	name, 
	versionName,
	folderid, 
	uri, 
	versionuri, 
	description,
	ownerid, 
	creatorid, 
	versiondate, 
	changedate, 
	previousversion
) values (
	'67c249a1-d160-11d6-bb9c-646533376c37',
	'67c249a2-d160-11d6-bb9c-646533376c37', 
	'Locator Use Types', '1.0',  
	'225924f8-1a72-42c9-a58d-05b41d8415ce',
	'Locator+Use+Types',
	'1.0/Locator+Use+Types/1.0',
	'Valid values for the Use Type property of a Locator',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP, 
	CURRENT TIMESTAMP,
	null
	);

insert into xr.repositoryobjectresource (
	id, 
	type, 
	subtype, 
	creatorid, 
	createdate, 
	currentVersion, 
	versionControlled
) values (
	'67c249a2-d160-11d6-bb9c-646533376c37', 
	'CLASSIFICATION_SCHEME', 
	'19adf2cf-0a2e-4d98-8f68-221708370b4c',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP,
	'67c249a1-d160-11d6-bb9c-646533376c37', 
	0);

-- This scheme goes into root folder
insert into xr.objectgroup_content (
	versionid,
	memberid
) values (
	'077bb8c4-0894-4d99-937a-356c315d26e2',
	'67c249a2-d160-11d6-bb9c-646533376c37'
);

insert into xr.classificationscheme (
	id, 
	structuretype
) values (
	'67c249a2-d160-11d6-bb9c-646533376c37', 
	'LIST');


-- This scheme classified as INTERNAL_USE
insert into xr.classifications ( 
	objectversionid, 
	valueid 
) values (
	'67c249a1-d160-11d6-bb9c-646533376c37',
	'196b15da-136f-4e19-933f-036f01481f5f');




--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
) values ( 
	'67c249a1-d160-11d6-bb9c-646533376c37', 
	'67c249a3-d160-11d6-bb9c-646533376c37', 
	'HOME',
	'HOME');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid 
) values ( 
	'67c249a1-d160-11d6-bb9c-646533376c37', 
	'67c249a3-d160-11d6-bb9c-646533376c37', 
	'67c249a3-d160-11d6-bb9c-646533376c37');

--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'67c249a1-d160-11d6-bb9c-646533376c37', 
	'67c249a4-d160-11d6-bb9c-646533376c37', 
	'OFFICE',
	'OFFICE');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid
)values ( 
	'67c249a1-d160-11d6-bb9c-646533376c37', 
	'67c249a4-d160-11d6-bb9c-646533376c37', 
	'67c249a4-d160-11d6-bb9c-646533376c37');

--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'67c249a1-d160-11d6-bb9c-646533376c37', 
	'67c249a5-d160-11d6-bb9c-646533376c37', 
	'MOBILE',
	'MOBILE');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid 
) values ( 
	'67c249a1-d160-11d6-bb9c-646533376c37', 
	'67c249a5-d160-11d6-bb9c-646533376c37', 
	'67c249a5-d160-11d6-bb9c-646533376c37');

--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
) values ( 
	'67c249a1-d160-11d6-bb9c-646533376c37', 
	'67c9eac0-d160-11d6-bb9c-646533376c37', 
	'PAGER',
	'PAGER');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid 
) values ( 
	'67c249a1-d160-11d6-bb9c-646533376c37', 
	'67c9eac0-d160-11d6-bb9c-646533376c37', 
	'67c9eac0-d160-11d6-bb9c-646533376c37');


--echo ==========================================================;
--echo == Create Association types ClassificationScheme ====================================;
--echo =====================================================================================;



insert into xr.repositoryobjectversion (
	id, 
	resourceid, 
	name, 
	versionName,
	folderid, 
	uri, 
	versionuri, 
	description,
	ownerid, 
	creatorid, 
	versiondate, 
	changedate, 
	previousversion
) values (
	'67bf3c60-d160-11d6-bb9c-646533376c37',
	'67bf3c61-d160-11d6-bb9c-646533376c37', 
	'Business Relationship Types', '1.0',  
	'225924f8-1a72-42c9-a58d-05b41d8415ce',
	'Business+Relationship+Types',
	'1.0/Business+Relationship+Types/1.0',
	'Valid values for the Type property of a Business Relationship',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP, 
	CURRENT TIMESTAMP,
	null
	);

insert into xr.repositoryobjectresource (
	id, 
	type, 
	subtype, 
	creatorid, 
	createdate, 
	currentVersion, 
	versionControlled
) values (
	'67bf3c61-d160-11d6-bb9c-646533376c37', 
	'CLASSIFICATION_SCHEME', 
	'19adf2cf-0a2e-4d98-8f68-221708370b4c',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP,
	'67bf3c60-d160-11d6-bb9c-646533376c37', 
	0);

-- This scheme goes into root folder
insert into xr.objectgroup_content (
	versionid,
	memberid
) values (
	'077bb8c4-0894-4d99-937a-356c315d26e2',
	'67bf3c61-d160-11d6-bb9c-646533376c37'
);

insert into xr.classificationscheme (
	id, 
	structuretype
) values (
	'67bf3c61-d160-11d6-bb9c-646533376c37', 
	'LIST');

-- This scheme classified as INTERNAL_USE
insert into xr.classifications ( 
	objectversionid, 
	valueid 
) values (
	'67bf3c60-d160-11d6-bb9c-646533376c37',
	'196b15da-136f-4e19-933f-036f01481f5f');



--echo =====================================================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
) values ( 
	'67bf3c60-d160-11d6-bb9c-646533376c37', 
	'67bf3c62-d160-11d6-bb9c-646533376c37', 
	'CUSTOMER',
	'CUSTOMER');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid 
) values ( 
	'67bf3c60-d160-11d6-bb9c-646533376c37', 
	'67bf3c62-d160-11d6-bb9c-646533376c37', 
	'67bf3c62-d160-11d6-bb9c-646533376c37');

--echo =====================================================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'67bf3c60-d160-11d6-bb9c-646533376c37', 
	'67bf3c63-d160-11d6-bb9c-646533376c37', 
	'SUPPLIER',
	'SUPPLIER');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid
)values ( 
	'67bf3c60-d160-11d6-bb9c-646533376c37', 
	'67bf3c63-d160-11d6-bb9c-646533376c37', 
	'67bf3c63-d160-11d6-bb9c-646533376c37');

--echo =====================================================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'67bf3c60-d160-11d6-bb9c-646533376c37', 
	'67c0c300-d160-11d6-bb9c-646533376c37', 
	'PARTNER',
	'PARTNER');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid 
) values ( 
	'67bf3c60-d160-11d6-bb9c-646533376c37', 
	'67c0c300-d160-11d6-bb9c-646533376c37', 
	'67c0c300-d160-11d6-bb9c-646533376c37');

--echo =====================================================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
) values ( 
	'67bf3c60-d160-11d6-bb9c-646533376c37', 
	'67c0c301-d160-11d6-bb9c-646533376c37', 
	'MANAGER',
	'MANAGER');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid 
) values ( 
	'67bf3c60-d160-11d6-bb9c-646533376c37', 
	'67c0c301-d160-11d6-bb9c-646533376c37', 
	'67c0c301-d160-11d6-bb9c-646533376c37');


--echo == Create Scheme Types ClassificationScheme ====================================;

insert into xr.repositoryobjectversion (
	id, 
	resourceid, 
	name, 
	versionName,
	folderid, 
	uri, 
	versionuri, 
	description,
	ownerid, 
	creatorid, 
	versiondate, 
	changedate, 
	previousversion
) values (
	'0a6a053e-1837-48dc-b7c4-11082c8c3b35',
	'1a07217d-3a90-4266-bc7e-1e493ea22b8f', 
	'Classification Scheme Types', '1.0',  
	'225924f8-1a72-42c9-a58d-05b41d8415ce',
	'Classification+Scheme+Types',
	'1.0/Classification+Scheme+Types/1.0',
	'Valid values for the ClassificationSchemeType property of a Classification Scheme',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP, CURRENT TIMESTAMP,
	null
	);

insert into xr.repositoryobjectresource (
	id, type, subtype, creatorid, createdate, currentVersion, versionControlled
	) values (
	'1a07217d-3a90-4266-bc7e-1e493ea22b8f', 'CLASSIFICATION_SCHEME', 
	'19adf2cf-0a2e-4d98-8f68-221708370b4c',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP,
	'0a6a053e-1837-48dc-b7c4-11082c8c3b35', 0);

-- This scheme goes into root folder
insert into xr.objectgroup_content (
	versionid,
	memberid
) values (
	'077bb8c4-0894-4d99-937a-356c315d26e2',
	'1a07217d-3a90-4266-bc7e-1e493ea22b8f'
);

insert into xr.classificationscheme (id, structuretype) 
	values ('1a07217d-3a90-4266-bc7e-1e493ea22b8f', 'LIST');

-- This scheme classified as INTERNAL_USE
insert into xr.classifications ( objectversionid, valueid )
	values ('0a6a053e-1837-48dc-b7c4-11082c8c3b35', '196b15da-136f-4e19-933f-036f01481f5f');

insert into xr.classification_values ( versionid, valueid, value )
	values ( '0a6a053e-1837-48dc-b7c4-11082c8c3b35', 
	'19adf2cf-0a2e-4d98-8f68-221708370b4c', 'VALUE_LIST');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '0a6a053e-1837-48dc-b7c4-11082c8c3b35', 
	'19adf2cf-0a2e-4d98-8f68-221708370b4c', '19adf2cf-0a2e-4d98-8f68-221708370b4c');

--echo == Create MimeTypes ClassificationScheme ====================================;

insert into xr.repositoryobjectversion (
	id, 
	resourceid, 
	name, 
	versionName,
	folderid, 
	uri, 
	versionuri, 
	description,
	ownerid, 
	creatorid, 
	versiondate, 
	changedate, 
	previousversion
) values (
	'131fe75d-2447-45d3-87a2-37ee31dd08d4',
	'392005d2-1949-4aae-856e-2425093733e2', 
	'Mime Types', '1.0', 
	'225924f8-1a72-42c9-a58d-05b41d8415ce',
	 'Mime+Types',
	'1.0/Mime+Types/1,0',
	'Valid values for the MimeType property of a Registry Entry',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP, CURRENT TIMESTAMP,
	null
	);

insert into xr.repositoryobjectresource (
	id, type, subtype, creatorid, createdate, currentVersion, versionControlled
	) values (
	'392005d2-1949-4aae-856e-2425093733e2', 'CLASSIFICATION_SCHEME', 
	'19adf2cf-0a2e-4d98-8f68-221708370b4c',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP,
	'131fe75d-2447-45d3-87a2-37ee31dd08d4', 0);

-- This scheme goes into root folder
insert into xr.objectgroup_content (
	versionid,
	memberid
) values (
	'077bb8c4-0894-4d99-937a-356c315d26e2',
	'392005d2-1949-4aae-856e-2425093733e2'
);

insert into xr.classificationscheme (id, structuretype) 
	values ('392005d2-1949-4aae-856e-2425093733e2', 'LIST');

-- This scheme classified as INTERNAL_USE
insert into xr.classifications ( objectversionid, valueid )
	values ('131fe75d-2447-45d3-87a2-37ee31dd08d4', '196b15da-136f-4e19-933f-036f01481f5f');




insert into xr.classification_values ( versionid, valueid, value, description )
	values ( '131fe75d-2447-45d3-87a2-37ee31dd08d4', 
	'212936b2-32bd-4328-b2e5-0a7915bb225b', 'TEXT/S-SSI-HTML',
	'For: htmls and shtml file types');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '131fe75d-2447-45d3-87a2-37ee31dd08d4', 
	'212936b2-32bd-4328-b2e5-0a7915bb225b', '212936b2-32bd-4328-b2e5-0a7915bb225b');


insert into xr.classification_values ( versionid, valueid, value, description )
	values ( '131fe75d-2447-45d3-87a2-37ee31dd08d4', 
	'3e190308-2a03-4487-b77f-1a3631f22240', 'TEXT/PLAIN',
	'For: htmls and shtml file types');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '131fe75d-2447-45d3-87a2-37ee31dd08d4', 
	'3e190308-2a03-4487-b77f-1a3631f22240', '3e190308-2a03-4487-b77f-1a3631f22240');

insert into xr.classification_values ( versionid, valueid, value, description )
	values ( '131fe75d-2447-45d3-87a2-37ee31dd08d4', 
	'24fe3d5b-336e-48f0-ae28-12e5034338f4', 'APPLICATION/OCTET-STREAM',
	'For: htmls and shtml file types');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '131fe75d-2447-45d3-87a2-37ee31dd08d4', 
	'24fe3d5b-336e-48f0-ae28-12e5034338f4', '24fe3d5b-336e-48f0-ae28-12e5034338f4');



insert into xr.classification_values ( versionid, valueid, value, description )
	values ( '131fe75d-2447-45d3-87a2-37ee31dd08d4', 
	'1d5d253a-3109-4c9f-857b-209304163595', 'APPLICATION/XML',
	'For: xsl file type');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '131fe75d-2447-45d3-87a2-37ee31dd08d4', 
	'1d5d253a-3109-4c9f-857b-209304163595', '1d5d253a-3109-4c9f-857b-209304163595');

insert into xr.classification_values ( versionid, valueid, value, description )
	values ( '131fe75d-2447-45d3-87a2-37ee31dd08d4', 
	'33cb30fa-1be9-4dc6-8473-3aa4164d1f98', 'TEXT/RICHTEXT',
	'For: rtx file type');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '131fe75d-2447-45d3-87a2-37ee31dd08d4', 
	'33cb30fa-1be9-4dc6-8473-3aa4164d1f98', '33cb30fa-1be9-4dc6-8473-3aa4164d1f98');

insert into xr.classification_values ( versionid, valueid, value, description )
	values ( '131fe75d-2447-45d3-87a2-37ee31dd08d4', 
	'2c1ff6aa-04a6-47ee-adb3-3cf40fb82a8d', 'TEXT/CSS',
	'For: css and s file type');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '131fe75d-2447-45d3-87a2-37ee31dd08d4', 
	'2c1ff6aa-04a6-47ee-adb3-3cf40fb82a8d', '2c1ff6aa-04a6-47ee-adb3-3cf40fb82a8d');

insert into xr.classification_values ( versionid, valueid, value, description )
	values ( '131fe75d-2447-45d3-87a2-37ee31dd08d4', 
	'3e300f8a-1224-4909-9233-1a7c26c106d3', 'APPLICATION/X-TEXTINFO',
	'For: texi and texinfo file types');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '131fe75d-2447-45d3-87a2-37ee31dd08d4', 
	'3e300f8a-1224-4909-9233-1a7c26c106d3', '3e300f8a-1224-4909-9233-1a7c26c106d3');

insert into xr.classification_values ( versionid, valueid, value, description )
	values ( '131fe75d-2447-45d3-87a2-37ee31dd08d4', 
	'1b2e14e9-27c7-42f2-a6ce-0ef50768010b', 'TEXT/HTML',
	'For: htm and html file types');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '131fe75d-2447-45d3-87a2-37ee31dd08d4', 
	'1b2e14e9-27c7-42f2-a6ce-0ef50768010b', '1b2e14e9-27c7-42f2-a6ce-0ef50768010b');

insert into xr.classification_values ( versionid, valueid, value, description )
	values ( '131fe75d-2447-45d3-87a2-37ee31dd08d4', 
	'36233866-3249-4fd5-9fea-0f5d119f270c', 'TEXT/XML',
	'For: xml and dtd file types');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '131fe75d-2447-45d3-87a2-37ee31dd08d4', 
	'36233866-3249-4fd5-9fea-0f5d119f270c', '36233866-3249-4fd5-9fea-0f5d119f270c');

--echo == Create Group Types ClassificationScheme ====================================;

insert into xr.repositoryobjectversion (
	id, 
	resourceid, 
	name, 
	versionName,
	folderid, 
	uri, 
	versionuri, 
	description,
	ownerid, 
	creatorid, 
	versiondate, 
	changedate, 
	previousversion
) values (
	'33a51bf1-2460-4533-a976-161a3ba50377',
	'186335e7-1ed0-4b6c-bc9e-362410001b2d', 
	'Collection Types', '1.0',  
	'225924f8-1a72-42c9-a58d-05b41d8415ce',
	'Collection+Types',
	'1.0/Collection+Types/1.0',
	'Valid values for the CollectionType property of a Collection',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP, CURRENT TIMESTAMP,
	null
	);

insert into xr.repositoryobjectresource (
	id, type, subtype, creatorid, createdate, currentVersion, versionControlled
	) values (
	'186335e7-1ed0-4b6c-bc9e-362410001b2d', 'CLASSIFICATION_SCHEME', 
	'19adf2cf-0a2e-4d98-8f68-221708370b4c',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP,
	'33a51bf1-2460-4533-a976-161a3ba50377', 0);

-- This scheme goes into root folder
insert into xr.objectgroup_content (
	versionid,
	memberid
) values (
	'077bb8c4-0894-4d99-937a-356c315d26e2',
	'186335e7-1ed0-4b6c-bc9e-362410001b2d'
);

insert into xr.classificationscheme (id, structuretype) 
	values ('186335e7-1ed0-4b6c-bc9e-362410001b2d', 'LIST');

-- This scheme classified as INTERNAL_USE
insert into xr.classifications ( objectversionid, valueid )
	values ('33a51bf1-2460-4533-a976-161a3ba50377', '196b15da-136f-4e19-933f-036f01481f5f');


--echo ==================================================================================;

insert into xr.classification_values ( versionid, valueid, value )
	values ( '33a51bf1-2460-4533-a976-161a3ba50377', 
	'11a832a5-0bb1-46db-a000-147906f70021', 'FOLDER');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '33a51bf1-2460-4533-a976-161a3ba50377', 
	'11a832a5-0bb1-46db-a000-147906f70021', '11a832a5-0bb1-46db-a000-147906f70021');

insert into xr.classification_values ( versionid, valueid, value )
	values ( '33a51bf1-2460-4533-a976-161a3ba50377', 
	'3bf533e0-0d39-4f8c-b109-07cd125e2ab1', 'PROJECT');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '33a51bf1-2460-4533-a976-161a3ba50377', 
	'3bf533e0-0d39-4f8c-b109-07cd125e2ab1', '3bf533e0-0d39-4f8c-b109-07cd125e2ab1');

--echo == Create Object Relationship Types ClassificationScheme ====================================;

insert into xr.repositoryobjectversion (
	id, 
	resourceid, 
	name, 
	versionName,
	folderid, 
	uri, 
	versionuri, 
	description,
	ownerid, 
	creatorid, 
	versiondate, 
	changedate, 
	previousversion
) values (
	'29556c11-35e9-44a5-84a0-3249345a221e',
	'33e5380b-30ca-47ad-8e1c-24ec3f5932af', 
	'Object Relationship Types','1.0',  
	'225924f8-1a72-42c9-a58d-05b41d8415ce',
	'Object+Relationship+Types',
	'1.0/Object+Relationship+Types/1.0',
	'Valid values for the RelationshipType property of an Object Relationship',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP, CURRENT TIMESTAMP,
	null
	);

insert into xr.repositoryobjectresource (
	id, type, subtype, creatorid, createdate, currentVersion, versionControlled
	) values (
	'33e5380b-30ca-47ad-8e1c-24ec3f5932af', 'CLASSIFICATION_SCHEME', 
	'19adf2cf-0a2e-4d98-8f68-221708370b4c',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP,
	'29556c11-35e9-44a5-84a0-3249345a221e', 0);

-- This scheme goes into root folder
insert into xr.objectgroup_content (
	versionid,
	memberid
) values (
	'077bb8c4-0894-4d99-937a-356c315d26e2',
	'33e5380b-30ca-47ad-8e1c-24ec3f5932af'
);

insert into xr.classificationscheme (id, structuretype) 
	values ('33e5380b-30ca-47ad-8e1c-24ec3f5932af', 'LIST');

-- This scheme classified as INTERNAL_USE
insert into xr.classifications ( objectversionid, valueid )
	values ('29556c11-35e9-44a5-84a0-3249345a221e', '196b15da-136f-4e19-933f-036f01481f5f');



--echo =====================================================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'29556c11-35e9-44a5-84a0-3249345a221e', 
	'023e3ccc-3c29-445c-98cf-22a030041508', 
	'INCLUDES',
	'INCLUDES');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid 
) values ( 
	'29556c11-35e9-44a5-84a0-3249345a221e', 
	'023e3ccc-3c29-445c-98cf-22a030041508', 
	'023e3ccc-3c29-445c-98cf-22a030041508');

--echo =====================================================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'29556c11-35e9-44a5-84a0-3249345a221e', 
	'06d427a6-149b-436a-acfd-38d61f503891', 
	'IMPORTS',
	'IMPORTS');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid 
) values ( 
	'29556c11-35e9-44a5-84a0-3249345a221e', 
	'06d427a6-149b-436a-acfd-38d61f503891', 
	'06d427a6-149b-436a-acfd-38d61f503891');


--echo =====================================================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'29556c11-35e9-44a5-84a0-3249345a221e', 
	'2c662810-2e25-489c-ad14-14d31e9a06cb', 
	'REDEFINES',
	'REDEFINES');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid 
) values ( 
	'29556c11-35e9-44a5-84a0-3249345a221e', 
	'2c662810-2e25-489c-ad14-14d31e9a06cb', 
	'2c662810-2e25-489c-ad14-14d31e9a06cb');

--echo == Create Scheme Usage ClassificationScheme ====================================;
--echo ================================================================================;

insert into xr.repositoryobjectversion (
	id, 
	resourceid, 
	name, 
	versionName,
	folderid, 
	uri, 
	versionuri, 
	description,
	ownerid, 
	creatorid, 
	versiondate, 
	changedate, 
	previousversion
) values (
	'32590e5c-1a2d-4631-8693-3a330f163b62',
	'13321d30-38aa-4b7a-a8f8-03a710ab0480', 
	'Classification Scheme Uses', '1.0', 
	'225924f8-1a72-42c9-a58d-05b41d8415ce',
	'Classification+Scheme+Uses',
	'1.0/Classification+Scheme+Uses/1.0',
	'Usage categories for Classification Schemes',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP, CURRENT TIMESTAMP,
	null
	);

insert into xr.repositoryobjectresource (
	id, type, subtype, creatorid, createdate, currentVersion, versionControlled
	) values (
	'13321d30-38aa-4b7a-a8f8-03a710ab0480', 
	'CLASSIFICATION_SCHEME', 
	'19adf2cf-0a2e-4d98-8f68-221708370b4c',
	'013026c8-1b22-487a-a189-1c7b16811035',
	CURRENT TIMESTAMP,
	'32590e5c-1a2d-4631-8693-3a330f163b62', 0);

-- This scheme goes into root folder
insert into xr.objectgroup_content (
	versionid,
	memberid
) values (
	'077bb8c4-0894-4d99-937a-356c315d26e2',
	'13321d30-38aa-4b7a-a8f8-03a710ab0480'
);

insert into xr.classificationscheme (id, structuretype) 
	values ('13321d30-38aa-4b7a-a8f8-03a710ab0480', 'LIST');

-- This scheme classified as INTERNAL_USE
insert into xr.classifications ( objectversionid, valueid )
	values ('32590e5c-1a2d-4631-8693-3a330f163b62', '196b15da-136f-4e19-933f-036f01481f5f');

-- This scheme classified as CLASSIFIES_CLASSIFICATION_SCHEME
insert into xr.classifications ( objectversionid, valueid )
	values ('32590e5c-1a2d-4631-8693-3a330f163b62', '081e2e55-2c65-4358-aecc-2faf0a2a15fa');

--echo ================================================================================;

-- Values
insert into xr.classification_values ( versionid, valueid, value, description, parentvalueid )
	values ( '32590e5c-1a2d-4631-8693-3a330f163b62', 
	'059b1ce1-0a6c-46f8-9a4f-153607e10bf1', 'CLASSIFIES_ALL',
	'Values in this ClassificationScheme can classify objects of any type.',
	null);

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '32590e5c-1a2d-4631-8693-3a330f163b62', 
	'059b1ce1-0a6c-46f8-9a4f-153607e10bf1', '059b1ce1-0a6c-46f8-9a4f-153607e10bf1');
--echo ================================================================================;

insert into xr.classification_values ( versionid, valueid, value, description, parentvalueid )
	values ( '32590e5c-1a2d-4631-8693-3a330f163b62', 
	'196b15da-136f-4e19-933f-036f01481f5f', 'INTERNAL_USE',
	'This ClassificationScheme has a special use in XR.',
	null);

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '32590e5c-1a2d-4631-8693-3a330f163b62', 
	'196b15da-136f-4e19-933f-036f01481f5f', '196b15da-136f-4e19-933f-036f01481f5f');
--echo ================================================================================;

insert into xr.classification_values ( versionid, valueid, value, description, parentvalueid )
	values ( '32590e5c-1a2d-4631-8693-3a330f163b62', 
	'2e2f1074-03ff-40f3-8e00-329e1ba410ee', 'CLASSIFIES_ORGANIZATION',
	'Values in this ClassificationScheme can classify objects of type ORGANIZATION.',
	null);

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '32590e5c-1a2d-4631-8693-3a330f163b62', 
	'2e2f1074-03ff-40f3-8e00-329e1ba410ee', '2e2f1074-03ff-40f3-8e00-329e1ba410ee');
--echo ================================================================================;

insert into xr.classification_values ( versionid, valueid, value, description, parentvalueid )
	values ( '32590e5c-1a2d-4631-8693-3a330f163b62', 
	'2c7e121d-2a88-48b6-8714-3e231ed83a1a', 'CLASSIFIES_REGISTRY_OBJECTS',
	'Values in this ClassificationScheme can classify objects of type REGISTRY_OBJECT.',
	null);

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '32590e5c-1a2d-4631-8693-3a330f163b62', 
	'2c7e121d-2a88-48b6-8714-3e231ed83a1a', '2c7e121d-2a88-48b6-8714-3e231ed83a1a');
--echo ================================================================================;

insert into xr.classification_values ( versionid, valueid, value, description, parentvalueid )
	values ( '32590e5c-1a2d-4631-8693-3a330f163b62', 
	'34c93f68-3724-4d5a-b911-2f721a1013dc', 'CLASSIFIES_REGISTRY_ENTRIES',
	'Values in this ClassificationScheme can classify objects of type REGISTRY_ENTRY.',
	null);

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '32590e5c-1a2d-4631-8693-3a330f163b62', 
	'34c93f68-3724-4d5a-b911-2f721a1013dc', '34c93f68-3724-4d5a-b911-2f721a1013dc');
--echo ================================================================================;

insert into xr.classification_values ( versionid, valueid, value, description, parentvalueid )
	values ( '32590e5c-1a2d-4631-8693-3a330f163b62', 
	'3f08339c-1628-4e2a-9476-3e3e014f18c3', 'CLASSIFIES_COLLECTIONS',
	'Values in this ClassificationScheme can classify objects of type COLLECTIONS.',
	null);

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '32590e5c-1a2d-4631-8693-3a330f163b62', 
	'3f08339c-1628-4e2a-9476-3e3e014f18c3', '3f08339c-1628-4e2a-9476-3e3e014f18c3');
--echo ================================================================================;

insert into xr.classification_values ( versionid, valueid, value, description, parentvalueid )
	values ( '32590e5c-1a2d-4631-8693-3a330f163b62', 
	'081e2e55-2c65-4358-aecc-2faf0a2a15fa', 'CLASSIFIES_CLASSIFICATION_SCHEMES',
	'values in this ClassificationScheme can classify objects of type CLASSIFICATION_SCHEMES',
	null);

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '32590e5c-1a2d-4631-8693-3a330f163b62', 
	'081e2e55-2c65-4358-aecc-2faf0a2a15fa', '081e2e55-2c65-4358-aecc-2faf0a2a15fa');

--echo ================================================================================;
--echo ================================================================================;



--echo == RepositoryEntry Status values ClassificationScheme ====================================;

insert into xr.repositoryobjectversion (
	id, 
	resourceid, 
	name, 
	versionName,
	folderid, 
	uri, 
	versionuri, 
	description,
	ownerid, 
	creatorid, 
	versiondate, 
	changedate, 
	previousversion
) values (
	'33a12091-136a-46f6-a9a5-27b71f4a1ad1',
	'1a551af6-1f62-4429-9025-3948276625d0', 
	'Status Values', '1.0',  
	'225924f8-1a72-42c9-a58d-05b41d8415ce',
	'Status+Values',
	'1.0/Status+Values/1.0',
	'Valid values for the Status property of a Registry Entry',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP, CURRENT TIMESTAMP,
	null
	);

insert into xr.repositoryobjectresource (
	id, type, subtype, creatorid, createdate, currentVersion, versionControlled
	) values (
	'1a551af6-1f62-4429-9025-3948276625d0', 'CLASSIFICATION_SCHEME', 
	'19adf2cf-0a2e-4d98-8f68-221708370b4c',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP,
	'33a12091-136a-46f6-a9a5-27b71f4a1ad1', 0);

-- This scheme goes into root folder
insert into xr.objectgroup_content (
	versionid,
	memberid
) values (
	'077bb8c4-0894-4d99-937a-356c315d26e2',
	'1a551af6-1f62-4429-9025-3948276625d0'
);

insert into xr.classificationscheme (id, structuretype) 
	values ('1a551af6-1f62-4429-9025-3948276625d0', 'LIST');

-- This scheme classified as INTERNAL_USE
insert into xr.classifications ( objectversionid, valueid )
	values ('33a12091-136a-46f6-a9a5-27b71f4a1ad1', '196b15da-136f-4e19-933f-036f01481f5f');

insert into xr.classification_values ( versionid, valueid, value )
	values ( '33a12091-136a-46f6-a9a5-27b71f4a1ad1', 
	'233f0687-3328-43b9-a120-165b04a93bc8', 'Submitted');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '33a12091-136a-46f6-a9a5-27b71f4a1ad1', 
	'233f0687-3328-43b9-a120-165b04a93bc8', '233f0687-3328-43b9-a120-165b04a93bc8');

insert into xr.classification_values ( versionid, valueid, value )
	values ( '33a12091-136a-46f6-a9a5-27b71f4a1ad1', 
	'024e19ad-0d4c-4f86-94a4-1a993c5f28d3', 'Approved');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '33a12091-136a-46f6-a9a5-27b71f4a1ad1', 
	'024e19ad-0d4c-4f86-94a4-1a993c5f28d3', '024e19ad-0d4c-4f86-94a4-1a993c5f28d3');

insert into xr.classification_values ( versionid, valueid, value )
	values ( '33a12091-136a-46f6-a9a5-27b71f4a1ad1', 
	'34011142-0eca-4a0e-be48-26b405c30552', 'Deprecated');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '33a12091-136a-46f6-a9a5-27b71f4a1ad1', 
	'34011142-0eca-4a0e-be48-26b405c30552', '34011142-0eca-4a0e-be48-26b405c30552');

insert into xr.classification_values ( versionid, valueid, value )
	values ( '33a12091-136a-46f6-a9a5-27b71f4a1ad1', 
	'1cd72ab1-05c3-4164-898e-090e09832441', 'Withdrawn');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '33a12091-136a-46f6-a9a5-27b71f4a1ad1', 
	'1cd72ab1-05c3-4164-898e-090e09832441', '1cd72ab1-05c3-4164-898e-090e09832441');

--echo == Create Industries ClassificationScheme ====================================;

insert into xr.repositoryobjectversion (
	id, 
	resourceid, 
	name, 
	versionName,
	folderid, 
	uri, 
	versionuri, 
	description,
	ownerid, 
	creatorid, 
	versiondate, 
	changedate, 
	previousversion
) values (
	'1230aea4-05a3-435f-832d-2fd639e525f3',
	'36ad1483-123f-48c3-9050-1e52358825f2', 
	'Industries', '1.0',  
	'225924f8-1a72-42c9-a58d-05b41d8415ce',
	'Industries', '1.0/industries/1.0',
	'North American Industry Classification Scheme',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP, CURRENT TIMESTAMP,
	null
	);

insert into xr.repositoryobjectresource (
	id, type, subtype, creatorid, createdate, currentVersion, versionControlled
	) values (
	'36ad1483-123f-48c3-9050-1e52358825f2', 
	'CLASSIFICATION_SCHEME', 
	'19adf2cf-0a2e-4d98-8f68-221708370b4c',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP,
	'1230aea4-05a3-435f-832d-2fd639e525f3', 0);

-- This scheme goes into root folder
insert into xr.objectgroup_content (
	versionid,
	memberid
) values (
	'077bb8c4-0894-4d99-937a-356c315d26e2',
	'36ad1483-123f-48c3-9050-1e52358825f2'
);

insert into xr.classificationscheme (id, structuretype) 
	values ('36ad1483-123f-48c3-9050-1e52358825f2', 'TREE');

-- This scheme classified as CLASSIFIES_ALL
	insert into xr.classifications ( objectversionid, valueid )
	values ('1230aea4-05a3-435f-832d-2fd639e525f3', '059b1ce1-0a6c-46f8-9a4f-153607e10bf1');

insert into xr.classification_values ( versionid, valueid, value )
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'3b43294d-034d-44f6-94bd-30bb3ca61cae', 'Wholesale Trade');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'3b43294d-034d-44f6-94bd-30bb3ca61cae', '3b43294d-034d-44f6-94bd-30bb3ca61cae');

insert into xr.classification_values ( versionid, valueid, value, parentvalueid)
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'039802aa-015a-4642-8d4e-2f9c17b0013c', 'Wholesale Trade, Durable Goods',
	'3b43294d-034d-44f6-94bd-30bb3ca61cae');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'039802aa-015a-4642-8d4e-2f9c17b0013c', '039802aa-015a-4642-8d4e-2f9c17b0013c');

-- Wholesale Trade is ancestor of Wholesale Trade, Durable Goods
insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'039802aa-015a-4642-8d4e-2f9c17b0013c', '3b43294d-034d-44f6-94bd-30bb3ca61cae');

insert into xr.classification_values ( versionid, valueid, value)
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'396528a3-34c6-47e3-af6c-133e2a6c328b', 'Manufacturing');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'396528a3-34c6-47e3-af6c-133e2a6c328b', '396528a3-34c6-47e3-af6c-133e2a6c328b');

insert into xr.classification_values ( versionid, valueid, value, parentvalueid)
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'1af03872-2727-4bb4-9d2b-1d82392e1d66', 'Food Manufacturing',
	'396528a3-34c6-47e3-af6c-133e2a6c328b');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'1af03872-2727-4bb4-9d2b-1d82392e1d66', '1af03872-2727-4bb4-9d2b-1d82392e1d66');

-- Manufacturing is ancestor of Food Manufacturing
insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'1af03872-2727-4bb4-9d2b-1d82392e1d66', '396528a3-34c6-47e3-af6c-133e2a6c328b');

insert into xr.classification_values ( versionid, valueid, value, parentvalueid)
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'009f16be-33b6-4846-ac9a-1ff932930615', 'Computer and Electronic Product Manufacturing',
	'396528a3-34c6-47e3-af6c-133e2a6c328b');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'009f16be-33b6-4846-ac9a-1ff932930615', '009f16be-33b6-4846-ac9a-1ff932930615');

-- Manufacturing is ancestor of Computer and Electronic Product Manufacturing
insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'009f16be-33b6-4846-ac9a-1ff932930615', '396528a3-34c6-47e3-af6c-133e2a6c328b');

insert into xr.classification_values ( versionid, valueid, value)
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'15ce1711-2938-4170-bf5d-333105ab0d02', 'Utilities');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'15ce1711-2938-4170-bf5d-333105ab0d02', '15ce1711-2938-4170-bf5d-333105ab0d02');

insert into xr.classification_values ( versionid, valueid, value)
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'23de3f4b-002b-40ba-b0f4-3ab20cdc1bf2', 'Construction');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'23de3f4b-002b-40ba-b0f4-3ab20cdc1bf2', '23de3f4b-002b-40ba-b0f4-3ab20cdc1bf2');

insert into xr.classification_values ( versionid, valueid, value)
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'225708f3-00a7-4406-b5e3-0ff72a082216', 'Retail Trade');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'225708f3-00a7-4406-b5e3-0ff72a082216', '225708f3-00a7-4406-b5e3-0ff72a082216');

insert into xr.classification_values ( versionid, valueid, value)
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'3d1926f7-27f7-462e-afb9-2fba15b00be1', 'Finance and Insurance');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'3d1926f7-27f7-462e-afb9-2fba15b00be1', '3d1926f7-27f7-462e-afb9-2fba15b00be1');

insert into xr.classification_values ( versionid, valueid, value, parentvalueid)
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'22651e88-2c3f-4552-9994-3ac7271e0e1c', 'Insurance Carriers and Related Activities',
	'3d1926f7-27f7-462e-afb9-2fba15b00be1');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'22651e88-2c3f-4552-9994-3ac7271e0e1c', '22651e88-2c3f-4552-9994-3ac7271e0e1c');

-- Finance and Insurance is ancestor of Insurance Carriers and Related Activities
insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'22651e88-2c3f-4552-9994-3ac7271e0e1c', '3d1926f7-27f7-462e-afb9-2fba15b00be1');

insert into xr.classification_values ( versionid, valueid, value, parentvalueid)
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'19db32d6-059b-4ced-ae72-24d7105a3a15', 'Funds, Trusts, and Other Financial Vehicles',
	'3d1926f7-27f7-462e-afb9-2fba15b00be1');

insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'19db32d6-059b-4ced-ae72-24d7105a3a15', '19db32d6-059b-4ced-ae72-24d7105a3a15');

-- Finance and Insurance is ancestor of Funds, Trusts, and Other Financial Vehicles
	insert into xr.classification_value_ancestors ( versionid, valueid, ancestorid )
	values ( '1230aea4-05a3-435f-832d-2fd639e525f3', 
	'19db32d6-059b-4ced-ae72-24d7105a3a15', '3d1926f7-27f7-462e-afb9-2fba15b00be1');

--echo ==========================================================;
--echo == Web Service Schemes ===================================;
--echo ==========================================================;


--XRADMIN ID '26747154-0dfc-46af-a85d-1dc30c230c4e' 
-- repositoryobjectversion.folderid references xr.repositoryobjectresource(id)
--ROOT FolderID foreign '225924f8-1a72-42c9-a58d-05b41d8415ce' 


--echo ==========================================================;
--echo == WSDL Function =========================================;
--echo ==========================================================;



insert into xr.repositoryobjectversion (
	id, 
	resourceid, 
	name, 
	versionName,
	folderid, 
	uri, 
	versionuri, 
	description,
	ownerid, 
	creatorid, 
	versiondate, 
	changedate, 
	previousversion
) values (
	'05832d21-039e-47ce-95f0-11b320540c89',
	'009738ce-2da3-428e-ad78-1ae53e9c0373', 
	'WSDL Function', '1.0',   
	'225924f8-1a72-42c9-a58d-05b41d8415ce',
	'WSDL+Function',
	'1.0/WSDL+Function/1.0',
	'The function that this wsdl provides, (interface/implementation/both/etc) ',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP, 
	CURRENT TIMESTAMP,
	null
	);

insert into xr.repositoryobjectresource (
	id, 
	type, 
	subtype, 
	creatorid, 
	createdate, 
	currentVersion, 
	versionControlled
) values (
	'009738ce-2da3-428e-ad78-1ae53e9c0373', 
	'CLASSIFICATION_SCHEME', 
	'19adf2cf-0a2e-4d98-8f68-221708370b4c',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP,
	'05832d21-039e-47ce-95f0-11b320540c89', 
	0);

-- This scheme goes into root folder
insert into xr.objectgroup_content (
	versionid,
	memberid
) values (
	'077bb8c4-0894-4d99-937a-356c315d26e2',
	'009738ce-2da3-428e-ad78-1ae53e9c0373'
);

insert into xr.classificationscheme (
	id, 
	structuretype
) values (
	'009738ce-2da3-428e-ad78-1ae53e9c0373', 
	'LIST');



-- This scheme classified as 
insert into xr.classifications ( 
	objectversionid, 
	valueid 
) values (
	'05832d21-039e-47ce-95f0-11b320540c89',
	'34c93f68-3724-4d5a-b911-2f721a1013dc');






--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
) values ( 
	'05832d21-039e-47ce-95f0-11b320540c89', 
	'26701eaa-288d-4939-b26d-32d3302b3a43', 
	'INTERFACE',
	'INTERFACE');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid 
) values ( 
	'05832d21-039e-47ce-95f0-11b320540c89', 
	'26701eaa-288d-4939-b26d-32d3302b3a43', 
	'26701eaa-288d-4939-b26d-32d3302b3a43');

--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'05832d21-039e-47ce-95f0-11b320540c89', 
	'37620d70-0430-4b10-94ab-339304bd3a32', 
	'IMPLEMENTAION',
	'IMPLEMENTAION');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid
)values ( 
	'05832d21-039e-47ce-95f0-11b320540c89', 
	'37620d70-0430-4b10-94ab-339304bd3a32', 
	'37620d70-0430-4b10-94ab-339304bd3a32');


--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'05832d21-039e-47ce-95f0-11b320540c89', 
	'3a6c14c0-3943-479b-b4b5-00613ddd389d', 
	'ALL',
	'This WSDL doc stands alone and has all of the information in one file');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid
)values ( 
	'05832d21-039e-47ce-95f0-11b320540c89', 
	'3a6c14c0-3943-479b-b4b5-00613ddd389d', 
	'3a6c14c0-3943-479b-b4b5-00613ddd389d');


--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'05832d21-039e-47ce-95f0-11b320540c89', 
	'15150e99-251d-471f-a8a7-346d0da93131', 
	'BINDING',
	'BINDING');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid
)values ( 
	'05832d21-039e-47ce-95f0-11b320540c89', 
	'15150e99-251d-471f-a8a7-346d0da93131', 
	'15150e99-251d-471f-a8a7-346d0da93131');



--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'05832d21-039e-47ce-95f0-11b320540c89', 
	'26171148-19be-4fb9-87c3-182001341f78', 
	'SERVICE',
	'SERVICE');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid
)values ( 
	'05832d21-039e-47ce-95f0-11b320540c89', 
	'26171148-19be-4fb9-87c3-182001341f78', 
	'26171148-19be-4fb9-87c3-182001341f78');



--echo ==========================================================;
--echo == Web Service Creation Toolkit ==========================;
--echo ==========================================================;




insert into xr.repositoryobjectversion (
	id, 
	resourceid, 
	name, 
	versionName,
	folderid, 
	uri, 
	versionuri, 
	description,
	ownerid, 
	creatorid, 
	versiondate, 
	changedate, 
	previousversion
) values (
	'2d0b2e03-3880-4529-967f-25401e3f1216',
	'11971f7b-360c-4701-aff8-3585338d1452', 
	'Web Service Toolkit', '1.0',   
	'225924f8-1a72-42c9-a58d-05b41d8415ce',
	'Web+Service+Toolkit',
	'1.0/Web+Service+Toolkit/1.0',
	'Web Service toolkit used to create this Document',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP, 
	CURRENT TIMESTAMP,
	null
	);

insert into xr.repositoryobjectresource (
	id, 
	type, 
	subtype, 
	creatorid, 
	createdate, 
	currentVersion, 
	versionControlled
) values (
	'11971f7b-360c-4701-aff8-3585338d1452', 
	'CLASSIFICATION_SCHEME', 
	'19adf2cf-0a2e-4d98-8f68-221708370b4c',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP,
	'2d0b2e03-3880-4529-967f-25401e3f1216', 
	0);

-- This scheme goes into root folder
insert into xr.objectgroup_content (
	versionid,
	memberid
) values (
	'077bb8c4-0894-4d99-937a-356c315d26e2',
	'11971f7b-360c-4701-aff8-3585338d1452'
);

insert into xr.classificationscheme (
	id, 
	structuretype
) values (
	'11971f7b-360c-4701-aff8-3585338d1452', 
	'LIST');



-- This scheme classified as 
insert into xr.classifications ( 
	objectversionid, 
	valueid 
) values (
	'2d0b2e03-3880-4529-967f-25401e3f1216',
	'34c93f68-3724-4d5a-b911-2f721a1013dc');




--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
) values ( 
	'2d0b2e03-3880-4529-967f-25401e3f1216', 
	'0b99365a-0d1d-4bae-b1a7-3b6f2299023f', 
	'MICROSOFT',
	'MICROSOFT');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid 
) values ( 
	'2d0b2e03-3880-4529-967f-25401e3f1216', 
	'0b99365a-0d1d-4bae-b1a7-3b6f2299023f', 
	'0b99365a-0d1d-4bae-b1a7-3b6f2299023f');

--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'2d0b2e03-3880-4529-967f-25401e3f1216', 
	'280f08d6-02f2-4623-9602-08d21bb715d9', 
	'WSAD',
	'WSAD');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid
)values ( 
	'2d0b2e03-3880-4529-967f-25401e3f1216', 
	'280f08d6-02f2-4623-9602-08d21bb715d9', 
	'280f08d6-02f2-4623-9602-08d21bb715d9');


--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'2d0b2e03-3880-4529-967f-25401e3f1216', 
	'1c8c0380-0551-495a-bf80-39a0076a01e9', 
	'WSTK',
	'WSTK');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid
)values ( 
	'2d0b2e03-3880-4529-967f-25401e3f1216', 
	'1c8c0380-0551-495a-bf80-39a0076a01e9', 
	'1c8c0380-0551-495a-bf80-39a0076a01e9');

--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'2d0b2e03-3880-4529-967f-25401e3f1216', 
	'3a551d74-30b7-4ea7-a2ac-1c3f1d3e2961', 
	'WSIF',
	'WSIF');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid
)values ( 
	'2d0b2e03-3880-4529-967f-25401e3f1216', 
	'3a551d74-30b7-4ea7-a2ac-1c3f1d3e2961', 
	'3a551d74-30b7-4ea7-a2ac-1c3f1d3e2961');



--echo ==========================================================;
--echo == WSDL Binding Schema ===================================;
--echo ==========================================================;




insert into xr.repositoryobjectversion (
	id, 
	resourceid, 
	name, 
	versionName,
	folderid, 
	uri, 
	versionuri, 
	description,
	ownerid, 
	creatorid, 
	versiondate, 
	changedate, 
	previousversion
) values (
	'24eb2dd7-0092-493b-a7f8-0ca520410f63',
	'016934a5-3a94-490b-8c14-13540df318c0', 
	'WSDL Binding Schema', '1.0',  
	'225924f8-1a72-42c9-a58d-05b41d8415ce',
	 'WSDL+Binding+Schema',
	'1.0/WSDL+Binding+Schema/1.0',
	'Valid types of soap binding style, see www.w3c.org wsdl/soap spec',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP, 
	CURRENT TIMESTAMP,
	null
	);

insert into xr.repositoryobjectresource (
	id, 
	type, 
	subtype, 
	creatorid, 
	createdate, 
	currentVersion, 
	versionControlled
) values (
	'016934a5-3a94-490b-8c14-13540df318c0', 
	'CLASSIFICATION_SCHEME', 
	'19adf2cf-0a2e-4d98-8f68-221708370b4c',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP,
	'24eb2dd7-0092-493b-a7f8-0ca520410f63', 
	0);

-- This scheme goes into root folder
insert into xr.objectgroup_content (
	versionid,
	memberid
) values (
	'077bb8c4-0894-4d99-937a-356c315d26e2',
	'016934a5-3a94-490b-8c14-13540df318c0'
);

insert into xr.classificationscheme (
	id, 
	structuretype
) values (
	'016934a5-3a94-490b-8c14-13540df318c0', 
	'LIST');



-- This scheme classified as 
insert into xr.classifications ( 
	objectversionid, 
	valueid 
) values (
	'24eb2dd7-0092-493b-a7f8-0ca520410f63',
	'34c93f68-3724-4d5a-b911-2f721a1013dc');






--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
) values ( 
	'24eb2dd7-0092-493b-a7f8-0ca520410f63', 
	'01a00568-1775-4d6b-877f-048e252d3f2a', 
	'SOAP',
	'SOAP');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid 
) values ( 
	'24eb2dd7-0092-493b-a7f8-0ca520410f63', 
	'01a00568-1775-4d6b-877f-048e252d3f2a', 
	'01a00568-1775-4d6b-877f-048e252d3f2a');

--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'24eb2dd7-0092-493b-a7f8-0ca520410f63', 
	'337e3856-29e2-492f-8843-3f9d00d927fe', 
	'HTTP',
	'HTTP');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid
)values ( 
	'24eb2dd7-0092-493b-a7f8-0ca520410f63', 
	'337e3856-29e2-492f-8843-3f9d00d927fe', 
	'337e3856-29e2-492f-8843-3f9d00d927fe');


--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'24eb2dd7-0092-493b-a7f8-0ca520410f63', 
	'2e483da3-1d10-4770-a8ca-13612feb1e5b', 
	'MIME',
	'MIME');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid
)values ( 
	'24eb2dd7-0092-493b-a7f8-0ca520410f63', 
	'2e483da3-1d10-4770-a8ca-13612feb1e5b', 
	'2e483da3-1d10-4770-a8ca-13612feb1e5b');

--echo ==========================================================;
--echo == SOAP Binding Style ====================================;
--echo ==========================================================;




insert into xr.repositoryobjectversion (
	id, 
	resourceid, 
	name, 
	versionName,
	folderid, 
	uri, 
	versionuri, 
	description,
	ownerid, 
	creatorid, 
	versiondate, 
	changedate, 
	previousversion
) values (
	'0d4c084d-3a39-4ad0-9c83-063a305c1b15',
	'3fab3fc4-229f-48ff-86b8-2f6227791557', 
	'SOAP Binding Style', '1.0',  
	'225924f8-1a72-42c9-a58d-05b41d8415ce',
	'SOAP+Binding+Style',
	'1.0/SOAP+Binding+Style/1.0',
	'Valid types of soap binding style, see www.w3c.org wsdl/soap spec',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP, 
	CURRENT TIMESTAMP,
	null
	);

insert into xr.repositoryobjectresource (
	id, 
	type, 
	subtype, 
	creatorid, 
	createdate, 
	currentVersion, 
	versionControlled
) values (
	'3fab3fc4-229f-48ff-86b8-2f6227791557', 
	'CLASSIFICATION_SCHEME', 
	'19adf2cf-0a2e-4d98-8f68-221708370b4c',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP,
	'0d4c084d-3a39-4ad0-9c83-063a305c1b15', 
	0);

-- This scheme goes into root folder
insert into xr.objectgroup_content (
	versionid,
	memberid
) values (
	'077bb8c4-0894-4d99-937a-356c315d26e2',
	'3fab3fc4-229f-48ff-86b8-2f6227791557'
);

insert into xr.classificationscheme (
	id, 
	structuretype
) values (
	'3fab3fc4-229f-48ff-86b8-2f6227791557', 
	'LIST');



-- This scheme classified as 
insert into xr.classifications ( 
	objectversionid, 
	valueid 
) values (
	'0d4c084d-3a39-4ad0-9c83-063a305c1b15',
	'34c93f68-3724-4d5a-b911-2f721a1013dc');




--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
) values ( 
	'0d4c084d-3a39-4ad0-9c83-063a305c1b15', 
	'04851881-3671-4a7d-b413-168f357b0290', 
	'RPC',
	'RPC');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid 
) values ( 
	'0d4c084d-3a39-4ad0-9c83-063a305c1b15', 
	'04851881-3671-4a7d-b413-168f357b0290', 
	'04851881-3671-4a7d-b413-168f357b0290');

--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'0d4c084d-3a39-4ad0-9c83-063a305c1b15', 
	'016934a5-3a94-490b-8c14-13540df318c0', 
	'DOCUMENT',
	'DOCUMENT');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid
)values ( 
	'0d4c084d-3a39-4ad0-9c83-063a305c1b15', 
	'016934a5-3a94-490b-8c14-13540df318c0', 
	'016934a5-3a94-490b-8c14-13540df318c0');

--echo ==========================================================;
--echo == WSDL Port types ====================================;
--echo =========================================================;


insert into xr.repositoryobjectversion (
	id, 
	resourceid, 
	name, 
	versionName,
	folderid, 
	uri, 
	versionuri, 
	description,
	ownerid, 
	creatorid, 
	versiondate, 
	changedate, 
	previousversion
) values (
	'006921cd-2c79-4d29-afe0-2bd3010232ce',
	'245d81e5-34c4-4ddc-bb6e-2cc134d83fdc', 
	'WSDL Port Type Operations', '1.0',  
	'225924f8-1a72-42c9-a58d-05b41d8415ce',
	'WSDL+Port+Type+Operations',
	'1.0/WSDL+Port+Type+Operations/1.0',
	'Valid types for port type operation, see www.w3c.org wsdl spec',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP, 
	CURRENT TIMESTAMP,
	null
	);

insert into xr.repositoryobjectresource (
	id, 
	type, 
	subtype, 
	creatorid, 
	createdate, 
	currentVersion, 
	versionControlled
) values (
	'245d81e5-34c4-4ddc-bb6e-2cc134d83fdc', 
	'CLASSIFICATION_SCHEME', 
	'19adf2cf-0a2e-4d98-8f68-221708370b4c',
	'26747154-0dfc-46af-a85d-1dc30c230c4e',
	CURRENT TIMESTAMP,
	'006921cd-2c79-4d29-afe0-2bd3010232ce', 
	0);

-- This scheme goes into root folder
insert into xr.objectgroup_content (
	versionid,
	memberid
) values (
	'077bb8c4-0894-4d99-937a-356c315d26e2',
	'245d81e5-34c4-4ddc-bb6e-2cc134d83fdc'
);

insert into xr.classificationscheme (
	id, 
	structuretype
) values (
	'245d81e5-34c4-4ddc-bb6e-2cc134d83fdc', 
	'LIST');


-- This scheme classified as 
insert into xr.classifications ( 
	objectversionid, 
	valueid 
) values (
	'006921cd-2c79-4d29-afe0-2bd3010232ce',
	'34c93f68-3724-4d5a-b911-2f721a1013dc');




--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
) values ( 
	'006921cd-2c79-4d29-afe0-2bd3010232ce', 
	'392929b5-32d6-40a9-ac16-233013e637c8', 
	'ONE-WAY',
	'ONE-WAY');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid 
) values ( 
	'006921cd-2c79-4d29-afe0-2bd3010232ce', 
	'392929b5-32d6-40a9-ac16-233013e637c8', 
	'392929b5-32d6-40a9-ac16-233013e637c8');

--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'006921cd-2c79-4d29-afe0-2bd3010232ce', 
	'0370089e-378a-438c-88c8-351605a82fed', 
	'REQUEST-RESPONSE',
	'REQUEST-RESPONSE');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid
)values ( 
	'006921cd-2c79-4d29-afe0-2bd3010232ce', 
	'0370089e-378a-438c-88c8-351605a82fed', 
	'0370089e-378a-438c-88c8-351605a82fed');

--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
)values ( 
	'006921cd-2c79-4d29-afe0-2bd3010232ce', 
	'08c63f80-037e-4636-ae06-0a1a3b260232', 
	'SOLICIT-RESPONSE',
	'SOLICIT-RESPONSE');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid 
) values ( 
	'006921cd-2c79-4d29-afe0-2bd3010232ce', 
	'08c63f80-037e-4636-ae06-0a1a3b260232', 
	'08c63f80-037e-4636-ae06-0a1a3b260232');

--echo ==========================================================;


insert into xr.classification_values ( 
	versionid, 
	valueid, 
	value, 
	description 
) values ( 
	'006921cd-2c79-4d29-afe0-2bd3010232ce', 
	'335b149b-34b6-423f-929e-1cfa12f522cb', 
	'NOTIFICATION',
	'NOTIFICATION');

insert into xr.classification_value_ancestors ( 
	versionid, 
	valueid, 
	ancestorid 
) values ( 
	'006921cd-2c79-4d29-afe0-2bd3010232ce', 
	'335b149b-34b6-423f-929e-1cfa12f522cb', 
	'335b149b-34b6-423f-929e-1cfa12f522cb');

-- Now do really what I wanted (this gets NullPointerException before the fix):

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 20000;

SELECT id, versionid, name, versionName, folderid, uri, versionuri, ownerid, ownername, ownerauthid,
       description, versionComment, objecttype, subtypeid, subtype, createdate,versiondate,
       changedate, deletedate, versioncontrolled, currentversion, previousversion, checkedout,
       statusid, status, startdate, expirationdate, contentchangedate, versioncontentid, sourceid,
       targetid, structuretype 
FROM xr.repositoryobjectallversionview rov 
where (uname = UPPER('two') or uname = UPPER('my project'))
and (versionid in
      (select versionid
       from xr.versionlabel
       where UPPER(label) = UPPER('Snapshot')))
  and deletedate is null
order by versionname asc;

-- NOTE: EXCEPT THE FIRST TWO TABLES IN OUTPUT (REPOSITORYOBJECTRESOURCE AND REPOSITORYOBJECTVERSION),
-- ALL OTHER TABLES SHOULD HAVE: Number of opens = 0 AND Rows seen = 0.  THIS IS BECAUSE PREDICATE
-- ON UNAME OF REPOSITORYOBJECTVERSION SHOULD BE PUSHED DOWN ALL THE WAY TO BASE TABLE ! bug 4983
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

rollback;

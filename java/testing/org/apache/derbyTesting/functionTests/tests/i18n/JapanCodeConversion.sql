drop table T1_EUC_JP;
-- table for data in EUC_JP encoding
create table T1_EUC_JP (	jnum int,
				jtime time,
				jstring char(200) );

-- import data in EUC_JP encoding 
call SYSCS_UTIL.SYSCS_IMPORT_TABLE(null, 'T1_EUC_JP' ,
	 		          'extin/jap_EUC_JP.dat' , 
				  null, null, 'EUC_JP', 0) ;

-- export to file with EUC_JP encoding 
call SYSCS_UTIL.SYSCS_EXPORT_TABLE('APP', 'T1_EUC_JP' ,
	 		          'extinout/jap_EUC_JP.dump' , 
				  null, null, 'EUC_JP') ;

-- export to file with SJIS encoding	
call SYSCS_UTIL.SYSCS_EXPORT_TABLE('APP', 'T1_EUC_JP' ,
	 		          'extinout/jap_SJIS.dump' , 
				  null, null, 'SJIS') ;

-- convert from EUC_JP to unicode with native2ascii
! 'native2ascii -encoding EUC_JP extinout/jap_EUC_JP.dump';

-- convert from SJIS to unicode with native2ascii
! 'native2ascii -encoding SJIS extinout/jap_SJIS.dump';

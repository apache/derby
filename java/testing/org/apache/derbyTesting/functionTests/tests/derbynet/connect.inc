// this include file has all the protocol needed to make a connection
// to the wombat database at sqlam 7
createDssRequest
startDdm EXCSAT
writeScalarString EXTNAM "test"
startDdm MGRLVLLS
writeCodePoint4Bytes SQLAM 7
endDdm
endDdm
endDss
flush
createDssRequest
startDdm ACCSEC
writeScalar2Bytes SECMEC 3 //  userid password
writeScalarString RDBNAM "wombat            " 
endDdm
endDss
flush
skipDss		// don't care about the EXCSATRM so just skip
skipDss		// don't care about the ACCSECRD so just skip
createDssRequest
startDdm  SECCHK
writeScalar2Bytes SECMEC 3 // userid password
writeScalarString RDBNAM "wombat            " 
writeScalarString USRID test
writeScalarString PASSWORD test
endDdm
endDss
createDssRequest
startDdm ACCRDB
writeScalar2Bytes RDBACCCL SQLAM
writeScalarBytes CRRTKN "0xd5c6f0f0f0f0f0f14bc7c3c2f600ec774aa60e"
writeScalarString PRDID "TST01000"
writeScalarString TYPDEFNAM "QTDSQLASC"
startDdm TYPDEFOVR
writeScalar2Bytes CCSIDSBC	1208
writeScalar2Bytes CCSIDMBC	1208
endDdm
endDdm
endDss
flush
skipDss // don't care about SECCHKRM so just skip
skipDss // don't care about ACCRDBRM so just skip

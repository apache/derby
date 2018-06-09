
createDssRequest
startDdm EXCSAT
writeScalarString EXTNAM "test"
endDdm
endDss
flush
createDssRequest
startDdm ACCSEC
writeScalar2Bytes SECMEC 3 //  userid password
endDdm
endDss
flush
skipDss		// don't care about the EXCSATRM so just skip
skipDss		// don't care about the ACCSECRD so just skip
createDssRequest
startDdm  SECCHK
writeScalar2Bytes SECMEC 3 // userid password
writeScalarString RDBNAM "wombat;create=true            " 
writeScalarString USRID test
writeScalarString PASSWORD test
endDdm
endDss

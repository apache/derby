createDssRequest
startDdm EXCSAT
writeScalarString EXTNAM "test"
endDdm
endDss
createDssRequest
startDdm ACCSEC
writeScalar2Bytes SECMEC 3 // userid password
writeScalarPaddedBytes RDBNAM test 18
endDdm
endDss
flush
skipDss		// don't care about the EXCSATRM so just skip
skipDss		// don't care about the ACCSECRD so just skip

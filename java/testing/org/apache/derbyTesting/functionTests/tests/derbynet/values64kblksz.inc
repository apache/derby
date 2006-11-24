// protocol including OPNQRY for values(1) statement
include connect.inc
createDssRequest
startDdm PRPSQLSTT
startDdm PKGNAMCSN
writePaddedString "wombat" 18
writePaddedString "rdbcolid" 18
writePaddedString "pkgid"  18
writeBytes "0x0101010101010101"
writeShort 1
endDdm
endDdm
endDss
createDssObject 
startDdm SQLSTT
writeByte 0	// null indicator
writeEncodedLDString "values(1)" "UTF-8" 4
writeByte 255 // null indicator
endDdm
endDss
flush
checkSQLCard 0 "     "
createDssRequest
startDdm OPNQRY
startDdm PKGNAMCSN
writePaddedString "wombat" 18
writePaddedString "rdbcolid" 18
writePaddedString "pkgid"  18
writeBytes "0x0101010101010101"
writeShort 1
endDdm
startDdm QRYBLKSZ
writeInt 65535
endDdm
endDdm
endDss
flush
readReplyDss
readLengthAndCodepoint OPNQRYRM
skipBytes
readReplyDss
readLengthAndCodepoint QRYDSC
skipBytes
readReplyDss
readLengthAndCodepoint QRYDTA
skipBytes

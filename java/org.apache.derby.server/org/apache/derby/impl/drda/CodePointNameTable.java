/*

   Derby - Class org.apache.derby.impl.drda.CodePointNameTable

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derby.impl.drda;

/**
  This class has a hashtable of CodePoint values.  It is used by the tracing
  code and by the protocol testing code
  It is arranged in alphabetical order.
*/

class CodePointNameTable extends java.util.Hashtable<Integer, String>
{
  CodePointNameTable ()
  {
    put(CodePoint.ABNUOWRM, "ABNUOWRM");
    put(CodePoint.ACCRDB, "ACCRDB");
    put(CodePoint.ACCRDBRM, "ACCRDBRM");
    put(CodePoint.ACCSEC, "ACCSEC");
    put(CodePoint.ACCSECRD, "ACCSECRD");
    put(CodePoint.AGENT, "AGENT");
    put(CodePoint.AGNPRMRM, "AGNPRMRM");
    put(CodePoint.BGNBND, "BGNBND");
    put(CodePoint.BGNBNDRM, "BGNBNDRM");
    put(CodePoint.BNDSQLSTT, "BNDSQLSTT");
    put(CodePoint.CCSIDSBC, "CCSIDSBC");
    put(CodePoint.CCSIDMBC, "CCSIDMBC");
    put(CodePoint.CCSIDDBC, "CCSIDDBC");
    put(CodePoint.CLSQRY, "CLSQRY");
    put(CodePoint.CMDATHRM, "CMDATHRM");
    put(CodePoint.CMDCHKRM, "CMDCHKRM");
    put(CodePoint.CMDCMPRM, "CMDCMPRM");
    put(CodePoint.CMDNSPRM, "CMDNSPRM");
    put(CodePoint.CMMRQSRM, "CMMRQSRM");
    put(CodePoint.CMDVLTRM, "CMDVLTRM");
    put(CodePoint.CNTQRY, "CNTQRY");
    put(CodePoint.CRRTKN, "CRRTKN");
    put(CodePoint.DRPPKG, "DRPPKG");
    put(CodePoint.DSCRDBTBL, "DSCRDBTBL");
    put(CodePoint.DSCINVRM, "DSCINVRM");
    put(CodePoint.DSCSQLSTT, "DSCSQLSTT");
    put(CodePoint.DTAMCHRM, "DTAMCHRM");
    put(CodePoint.ENDBND, "ENDBND");
    put(CodePoint.ENDQRYRM, "ENDQRYRM");
    put(CodePoint.ENDUOWRM, "ENDUOWRM");
    put(CodePoint.EXCSAT, "EXCSAT");
    put(CodePoint.EXCSATRD, "EXCSATRD");
    put(CodePoint.EXCSQLIMM, "EXCSQLIMM");
    put(CodePoint.EXCSQLSET, "EXCSQLSET");
    put(CodePoint.EXCSQLSTT, "EXCSQLSTT");
    put(CodePoint.EXTNAM, "EXTNAM");
    put(CodePoint.FRCFIXROW, "FRCFIXROW");
    put(CodePoint.MAXBLKEXT, "MAXBLKEXT");
    put(CodePoint.MAXRSLCNT, "MAXRSLCNT");
    put(CodePoint.MGRDEPRM, "MGRDEPRM");
    put(CodePoint.MGRLVLLS, "MGRLVLLS");
    put(CodePoint.MGRLVLRM, "MGRLVLRM");
    put(CodePoint.MONITOR, "MONITOR");
    put(CodePoint.NBRROW, "NBRROW");
    put(CodePoint.OBJNSPRM, "OBJNSPRM");
    put(CodePoint.OPNQFLRM, "OPNQFLRM");
    put(CodePoint.OPNQRY, "OPNQRY");
    put(CodePoint.OPNQRYRM, "OPNQRYRM");
    put(CodePoint.OUTEXP, "OUTEXP");
    put(CodePoint.OUTOVR, "OUTOVR");
    put(CodePoint.OUTOVROPT, "OUTOVROPT");
    put(CodePoint.PASSWORD, "PASSWORD");
    put(CodePoint.PKGID, "PKGID");
    put(CodePoint.PKGBNARM, "PKGBNARM");
    put(CodePoint.PKGBPARM, "PKGBPARM");
    put(CodePoint.PKGNAMCSN, "PKGNAMCSN");
    put(CodePoint.PKGNAMCT, "PKGNAMCT");
    put(CodePoint.PRCCNVRM, "PRCCNVRM");
    put(CodePoint.PRDID, "PRDID");
    put(CodePoint.PRDDTA, "PRDDTA");
    put(CodePoint.PRMNSPRM, "PRMNSPRM");
    put(CodePoint.PRPSQLSTT, "PRPSQLSTT");
    put(CodePoint.QRYBLKCTL, "QRYBLKCTL");
    put(CodePoint.QRYBLKRST, "QRYBLKRST");
    put(CodePoint.QRYBLKSZ, "QRYBLKSZ");
    put(CodePoint.QRYCLSIMP, "QRYCLSIMP");
    put(CodePoint.QRYCLSRLS, "QRYCLSRLS");
    put(CodePoint.QRYDSC, "QRYDSC");
    put(CodePoint.QRYDTA, "QRYDTA");
    put(CodePoint.QRYINSID, "QRYINSID");
    put(CodePoint.QRYNOPRM, "QRYNOPRM");
    put(CodePoint.QRYPOPRM, "QRYPOPRM");
    put(CodePoint.QRYRELSCR, "QRYRELSCR");
    put(CodePoint.QRYRFRTBL, "QRYRFRTBL");
    put(CodePoint.QRYROWNBR, "QRYROWNBR");
    put(CodePoint.QRYROWSNS, "QRYROWSNS");
    put(CodePoint.QRYRTNDTA, "QRYRTNDTA");
    put(CodePoint.QRYSCRORN, "QRYSCRORN");
    put(CodePoint.QRYROWSET, "QRYROWSET");
    put(CodePoint.RDBAFLRM, "RDBAFLRM");
    put(CodePoint.RDBACCCL, "RDBACCCL");
    put(CodePoint.RDBACCRM, "RDBACCRM");
    put(CodePoint.RDBALWUPD, "RDBALWUPD");
    put(CodePoint.RDBATHRM, "RDBATHRM");
    put(CodePoint.RDBCMM, "RDBCMM");
    put(CodePoint.RDBCMTOK, "RDBCMTOK");
    put(CodePoint.RDBNACRM, "RDBNACRM");
    put(CodePoint.RDBNAM, "RDBNAM");
    put(CodePoint.RDBNFNRM, "RDBNFNRM");
    put(CodePoint.RDBRLLBCK, "RDBRLLBCK");
    put(CodePoint.RDBUPDRM, "RDBUPDRM");
    put(CodePoint.REBIND, "REBIND");
    put(CodePoint.RSCLMTRM, "RSCLMTRM");
    put(CodePoint.RSLSETRM, "RSLSETRM");
    put(CodePoint.RTNEXTDTA, "RTNEXTDTA");
    put(CodePoint.RTNSQLDA, "RTNSQLDA");
    put(CodePoint.SECCHK, "SECCHK");
    put(CodePoint.SECCHKCD, "SECCHKCD");
    put(CodePoint.SECCHKRM, "SECCHKRM");
    put(CodePoint.SECMEC, "SECMEC");
    put(CodePoint.SECMGRNM, "SECMGRNM");
    put(CodePoint.SECTKN, "SECTKN");
    put(CodePoint.SPVNAM, "SPVNAM");
    put(CodePoint.SQLAM, "SQLAM");
    put(CodePoint.SQLATTR, "SQLATTR");
    put(CodePoint.SQLCARD, "SQLCARD");
    put(CodePoint.SQLERRRM, "SQLERRRM");
    put(CodePoint.SQLDARD, "SQLDARD");
    put(CodePoint.SQLDTA, "SQLDTA");
    put(CodePoint.SQLDTARD, "SQLDTARD");
    put(CodePoint.SQLSTT, "SQLSTT");
    put(CodePoint.SQLSTTVRB, "SQLSTTVRB");
    put(CodePoint.SRVCLSNM, "SRVCLSNM");
    put(CodePoint.SRVRLSLV, "SRVRLSLV");
    put(CodePoint.SRVNAM, "SRVNAM");
    put(CodePoint.SVRCOD, "SVRCOD");
    put(CodePoint.SYNCCTL, "SYNCCTL");
    put(CodePoint.SYNCLOG, "SYNCLOG");
    put(CodePoint.SYNCRSY, "SYNCRSY");
    put(CodePoint.SYNTAXRM, "SYNTAXRM");
    put(CodePoint.TRGNSPRM, "TRGNSPRM");
    put(CodePoint.TYPDEFNAM, "TYPDEFNAM");
    put(CodePoint.TYPDEFOVR, "TYPDEFOVR");
    put(CodePoint.TYPSQLDA, "TYPSQLDA");
    put(CodePoint.UOWDSP, "UOWDSP");
    put(CodePoint.USRID, "USRID");
    put(CodePoint.VALNSPRM, "VALNSPRM");
    put(CodePoint.PBSD, "PBSD");
    put(CodePoint.PBSD_ISO, "PBSD_ISO");
    put(CodePoint.PBSD_SCHEMA, "PBSD_SCHEMA");
    put(CodePoint.UNICODEMGR, "UNICODEMGR");
  }

  String lookup (int codePoint)
  {
    return get(codePoint);
  }

}

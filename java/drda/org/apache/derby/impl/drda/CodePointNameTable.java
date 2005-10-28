/*

   Derby - Class org.apache.derby.impl.drda.CodePointNameTable

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

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

class CodePointNameTable extends java.util.Hashtable
{
  CodePointNameTable ()
  {
    put (new Integer (CodePoint.ABNUOWRM), "ABNUOWRM");
    put (new Integer (CodePoint.ACCRDB), "ACCRDB");
    put (new Integer (CodePoint.ACCRDBRM), "ACCRDBRM");
    put (new Integer (CodePoint.ACCSEC), "ACCSEC");
    put (new Integer (CodePoint.ACCSECRD), "ACCSECRD");
    put (new Integer (CodePoint.AGENT), "AGENT");
    put (new Integer (CodePoint.AGNPRMRM), "AGNPRMRM");
    put (new Integer (CodePoint.BGNBND), "BGNBND");
    put (new Integer (CodePoint.BGNBNDRM), "BGNBNDRM");
    put (new Integer (CodePoint.BNDSQLSTT), "BNDSQLSTT");
    put (new Integer (CodePoint.CCSIDSBC), "CCSIDSBC");
    put (new Integer (CodePoint.CCSIDMBC), "CCSIDMBC");
    put (new Integer (CodePoint.CCSIDDBC), "CCSIDDBC");
    put (new Integer (CodePoint.CLSQRY), "CLSQRY");
    put (new Integer (CodePoint.CMDATHRM), "CMDATHRM");
    put (new Integer (CodePoint.CMDCHKRM), "CMDCHKRM");
    put (new Integer (CodePoint.CMDCMPRM), "CMDCMPRM");
    put (new Integer (CodePoint.CMDNSPRM), "CMDNSPRM");
    put (new Integer (CodePoint.CMMRQSRM), "CMMRQSRM");
    put (new Integer (CodePoint.CMDVLTRM), "CMDVLTRM");
    put (new Integer (CodePoint.CNTQRY), "CNTQRY");
    put (new Integer (CodePoint.CRRTKN), "CRRTKN");
    put (new Integer (CodePoint.DRPPKG), "DRPPKG");
    put (new Integer (CodePoint.DSCRDBTBL), "DSCRDBTBL");
    put (new Integer (CodePoint.DSCINVRM), "DSCINVRM");
    put (new Integer (CodePoint.DSCSQLSTT), "DSCSQLSTT");
    put (new Integer (CodePoint.DTAMCHRM), "DTAMCHRM");
    put (new Integer (CodePoint.ENDBND), "ENDBND");
    put (new Integer (CodePoint.ENDQRYRM), "ENDQRYRM");
    put (new Integer (CodePoint.ENDUOWRM), "ENDUOWRM");
    put (new Integer (CodePoint.EXCSAT), "EXCSAT");
    put (new Integer (CodePoint.EXCSATRD), "EXCSATRD");
    put (new Integer (CodePoint.EXCSQLIMM), "EXCSQLIMM");
    put (new Integer (CodePoint.EXCSQLSET), "EXCSQLSET");
    put (new Integer (CodePoint.EXCSQLSTT), "EXCSQLSTT");
    put (new Integer (CodePoint.EXTNAM), "EXTNAM");
    put (new Integer (CodePoint.MAXBLKEXT), "MAXBLKEXT");
    put (new Integer (CodePoint.MAXRSLCNT), "MAXRSLCNT");
    put (new Integer (CodePoint.MGRDEPRM), "MGRDEPRM");
    put (new Integer (CodePoint.MGRLVLLS), "MGRLVLLS");
    put (new Integer (CodePoint.MGRLVLRM), "MGRLVLRM");
    put (new Integer (CodePoint.NBRROW), "NBRROW");
    put (new Integer (CodePoint.OBJNSPRM), "OBJNSPRM");
    put (new Integer (CodePoint.OPNQFLRM), "OPNQFLRM");
    put (new Integer (CodePoint.OPNQRY), "OPNQRY");
    put (new Integer (CodePoint.OPNQRYRM), "OPNQRYRM");
    put (new Integer (CodePoint.OUTEXP), "OUTEXP");
    put (new Integer (CodePoint.OUTOVR), "OUTOVR");
    put (new Integer (CodePoint.OUTOVROPT), "OUTOVROPT");
    put (new Integer (CodePoint.PASSWORD), "PASSWORD");
    put (new Integer (CodePoint.PKGID), "PKGID");
    put (new Integer (CodePoint.PKGBNARM), "PKGBNARM");
    put (new Integer (CodePoint.PKGBPARM), "PKGBPARM");
    put (new Integer (CodePoint.PKGNAMCSN), "PKGNAMCSN");
    put (new Integer (CodePoint.PRCCNVRM), "PRCCNVRM");
    put (new Integer (CodePoint.PRDID), "PRDID");
    put (new Integer (CodePoint.PRDDTA), "PRDDTA");
    put (new Integer (CodePoint.PRMNSPRM), "PRMNSPRM");
    put (new Integer (CodePoint.PRPSQLSTT), "PRPSQLSTT");
    put (new Integer (CodePoint.QRYBLKCTL), "QRYBLKCTL");
    put (new Integer (CodePoint.QRYBLKRST), "QRYBLKRST");
    put (new Integer (CodePoint.QRYBLKSZ), "QRYBLKSZ");
    put (new Integer (CodePoint.QRYDSC), "QRYDSC");
    put (new Integer (CodePoint.QRYDTA), "QRYDTA");
    put (new Integer (CodePoint.QRYINSID), "QRYINSID");
    put (new Integer (CodePoint.QRYNOPRM), "QRYNOPRM");
    put (new Integer (CodePoint.QRYPOPRM), "QRYPOPRM");
    put (new Integer (CodePoint.QRYRELSCR), "QRYRELSCR");
    put (new Integer (CodePoint.QRYRFRTBL), "QRYRFRTBL");
    put (new Integer (CodePoint.QRYROWNBR), "QRYROWNBR");
    put (new Integer (CodePoint.QRYROWSNS), "QRYROWSNS");
    put (new Integer (CodePoint.QRYRTNDTA), "QRYRTNDTA");
    put (new Integer (CodePoint.QRYSCRORN), "QRYSCRORN");
    put (new Integer (CodePoint.QRYROWSET), "QRYROWSET");
    put (new Integer (CodePoint.RDBAFLRM), "RDBAFLRM");
    put (new Integer (CodePoint.RDBACCCL), "RDBACCCL");
    put (new Integer (CodePoint.RDBACCRM), "RDBACCRM");
    put (new Integer (CodePoint.RDBALWUPD), "RDBALWUPD");
    put (new Integer (CodePoint.RDBATHRM), "RDBATHRM");
    put (new Integer (CodePoint.RDBCMM), "RDBCMM");
    put (new Integer (CodePoint.RDBCMTOK), "RDBCMTOK");
    put (new Integer (CodePoint.RDBNACRM), "RDBNACRM");
    put (new Integer (CodePoint.RDBNAM), "RDBNAM");
    put (new Integer (CodePoint.RDBNFNRM), "RDBNFNRM");
    put (new Integer (CodePoint.RDBRLLBCK), "RDBRLLBCK");
    put (new Integer (CodePoint.RDBUPDRM), "RDBUPDRM");
    put (new Integer (CodePoint.REBIND), "REBIND");
    put (new Integer (CodePoint.RSCLMTRM), "RSCLMTRM");
    put (new Integer (CodePoint.RSLSETRM), "RSLSETRM");
    put (new Integer (CodePoint.RTNEXTDTA), "RTNEXTDTA");
    put (new Integer (CodePoint.RTNSQLDA), "RTNSQLDA");
    put (new Integer (CodePoint.SECCHK), "SECCHK");
    put (new Integer (CodePoint.SECCHKCD), "SECCHKCD");
    put (new Integer (CodePoint.SECCHKRM), "SECCHKRM");
    put (new Integer (CodePoint.SECMEC), "SECMEC");
    put (new Integer (CodePoint.SECMGRNM), "SECMGRNM");
    put (new Integer (CodePoint.SECTKN), "SECTKN");
    put (new Integer (CodePoint.SPVNAM), "SPVNAM");
    put (new Integer (CodePoint.SQLAM), "SQLAM");
    put (new Integer (CodePoint.SQLATTR), "SQLATTR");
    put (new Integer (CodePoint.SQLCARD), "SQLCARD");
    put (new Integer (CodePoint.SQLERRRM), "SQLERRRM");
    put (new Integer (CodePoint.SQLDARD), "SQLDARD");
    put (new Integer (CodePoint.SQLDTA), "SQLDTA");
    put (new Integer (CodePoint.SQLDTARD), "SQLDTARD");
    put (new Integer (CodePoint.SQLSTT), "SQLSTT");
    put (new Integer (CodePoint.SQLSTTVRB), "SQLSTTVRB");
    put (new Integer (CodePoint.SRVCLSNM), "SRVCLSNM");
    put (new Integer (CodePoint.SRVRLSLV), "SRVRLSLV");
    put (new Integer (CodePoint.SRVNAM), "SRVNAM");
    put (new Integer (CodePoint.SVRCOD), "SVRCOD");
    put (new Integer (CodePoint.SYNCCTL), "SYNCCTL");
    put (new Integer (CodePoint.SYNCLOG), "SYNCLOG");
    put (new Integer (CodePoint.SYNCRSY), "SYNCRSY");
    put (new Integer (CodePoint.SYNTAXRM), "SYNTAXRM");
    put (new Integer (CodePoint.TRGNSPRM), "TRGNSPRM");
    put (new Integer (CodePoint.TYPDEFNAM), "TYPDEFNAM");
    put (new Integer (CodePoint.TYPDEFOVR), "TYPDEFOVR");
    put (new Integer (CodePoint.TYPSQLDA), "TYPSQLDA");
    put (new Integer (CodePoint.UOWDSP), "UOWDSP");
    put (new Integer (CodePoint.USRID), "USRID");
    put (new Integer (CodePoint.VALNSPRM), "VALNSPRM");
  }

  String lookup (int codePoint)
  {
    return (String) get (new Integer (codePoint));
  }

}

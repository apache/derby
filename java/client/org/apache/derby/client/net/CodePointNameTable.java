/*

   Derby - Class org.apache.derby.client.net.CodePointNameTable

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

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

package org.apache.derby.client.net;

// This mapping is used by DssTrace only.
// This is not part of the driver and is not initialized unless dss tracing is enabled.
// This is an abstract mapping from 2-byte code point to a string representing the name of the code point.
// This data type may be modified for performance to adapt to any sort of lookup implementation,
// such as binary search on an underlying sorted array.

class CodePointNameTable extends java.util.Hashtable {
    CodePointNameTable() {
        put(new Integer(CodePoint.ACCSECRD), "ACCSECRD");
        put(new Integer(CodePoint.TYPDEFNAM), "TYPDEFNAM");
        put(new Integer(CodePoint.TYPDEFOVR), "TYPDEFOVR");
        put(new Integer(CodePoint.EXCSAT), "EXCSAT");
        put(new Integer(CodePoint.SYNCCTL), "SYNCCTL");
        put(new Integer(CodePoint.SYNCCRD), "SYNCCRD");
        put(new Integer(CodePoint.SYNCRSY), "SYNCRSY");
        put(new Integer(CodePoint.ACCSEC), "ACCSEC");
        put(new Integer(CodePoint.SECCHK), "SECCHK");
        put(new Integer(CodePoint.MGRLVLRM), "MGRLVLRM");
        put(new Integer(CodePoint.SECCHKRM), "SECCHKRM");
        put(new Integer(CodePoint.CMDNSPRM), "CMDNSPRM");
        put(new Integer(CodePoint.OBJNSPRM), "OBJNSPRM");
        put(new Integer(CodePoint.CMDCHKRM), "CMDCHKRM");
        put(new Integer(CodePoint.SYNTAXRM), "SYNTAXRM");
        put(new Integer(CodePoint.VALNSPRM), "VALNSPRM");
        put(new Integer(CodePoint.EXCSATRD), "EXCSATRD");
        put(new Integer(CodePoint.ACCRDB), "ACCRDB");
        put(new Integer(CodePoint.CLSQRY), "CLSQRY");
        put(new Integer(CodePoint.CNTQRY), "CNTQRY");
        put(new Integer(CodePoint.DSCSQLSTT), "DSCSQLSTT");
        put(new Integer(CodePoint.EXCSQLIMM), "EXCSQLIMM");
        put(new Integer(CodePoint.EXCSQLSTT), "EXCSQLSTT");
        put(new Integer(CodePoint.OPNQRY), "OPNQRY");
        put(new Integer(CodePoint.PRPSQLSTT), "PRPSQLSTT");
        put(new Integer(CodePoint.RDBCMM), "RDBCMM");
        put(new Integer(CodePoint.RDBRLLBCK), "RDBRLLBCK");
        put(new Integer(CodePoint.DSCRDBTBL), "DSCRDBTBL");
        put(new Integer(CodePoint.ACCRDBRM), "ACCRDBRM");
        put(new Integer(CodePoint.QRYNOPRM), "QRYNOPRM");
        put(new Integer(CodePoint.RDBATHRM), "RDBATHRM");
        put(new Integer(CodePoint.RDBNACRM), "RDBNACRM");
        put(new Integer(CodePoint.OPNQRYRM), "OPNQRYRM");
        put(new Integer(CodePoint.RDBACCRM), "RDBACCRM");
        put(new Integer(CodePoint.ENDQRYRM), "ENDQRYRM");
        put(new Integer(CodePoint.ENDUOWRM), "ENDUOWRM");
        put(new Integer(CodePoint.ABNUOWRM), "ABNUOWRM");
        put(new Integer(CodePoint.DTAMCHRM), "DTAMCHRM");
        put(new Integer(CodePoint.QRYPOPRM), "QRYPOPRM");
        put(new Integer(CodePoint.RDBNFNRM), "RDBNFNRM");
        put(new Integer(CodePoint.OPNQFLRM), "OPNQFLRM");
        put(new Integer(CodePoint.SQLERRRM), "SQLERRRM");
        put(new Integer(CodePoint.RDBUPDRM), "RDBUPDRM");
        put(new Integer(CodePoint.RSLSETRM), "RSLSETRM");
        put(new Integer(CodePoint.RDBAFLRM), "RDBAFLRM");
        put(new Integer(CodePoint.SQLCARD), "SQLCARD");
        put(new Integer(CodePoint.SQLDARD), "SQLDARD");
        put(new Integer(CodePoint.SQLDTA), "SQLDTA");
        put(new Integer(CodePoint.SQLDTARD), "SQLDTARD");
        put(new Integer(CodePoint.SQLSTT), "SQLSTT");
        put(new Integer(CodePoint.QRYDSC), "QRYDSC");
        put(new Integer(CodePoint.QRYDTA), "QRYDTA");
        put(new Integer(CodePoint.PRCCNVRM), "PRCCNVRM");
        put(new Integer(CodePoint.EXCSQLSET), "EXCSQLSET");
        put(new Integer(CodePoint.EXTDTA), "EXTDTA");
    }

    String lookup(int codePoint) {
        return (String) get(new Integer(codePoint));
    }
}

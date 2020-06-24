/*

   Derby - Class org.apache.derby.client.net.CodePointNameTable

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

package org.apache.derby.client.net;

// This mapping is used by DssTrace only.

import java.util.Hashtable;

// This is not part of the driver and is not initialized unless dss tracing is enabled.
// This is an abstract mapping from 2-byte code point to a string representing the name of the code point.
// This data type may be modified for performance to adapt to any sort of lookup implementation,
// such as binary search on an underlying sorted array.

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
class CodePointNameTable extends Hashtable<Integer, String> {
    CodePointNameTable() {
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
        put(CodePoint.ACCSECRD, "ACCSECRD");
        put(CodePoint.TYPDEFNAM, "TYPDEFNAM");
        put(CodePoint.TYPDEFOVR, "TYPDEFOVR");
        put(CodePoint.EXCSAT, "EXCSAT");
        put(CodePoint.SYNCCTL, "SYNCCTL");
        put(CodePoint.SYNCCRD, "SYNCCRD");
        put(CodePoint.SYNCRSY, "SYNCRSY");
        put(CodePoint.ACCSEC, "ACCSEC");
        put(CodePoint.SECCHK, "SECCHK");
        put(CodePoint.MGRLVLRM, "MGRLVLRM");
        put(CodePoint.SECCHKRM, "SECCHKRM");
        put(CodePoint.CMDNSPRM, "CMDNSPRM");
        put(CodePoint.OBJNSPRM, "OBJNSPRM");
        put(CodePoint.CMDCHKRM, "CMDCHKRM");
        put(CodePoint.SYNTAXRM, "SYNTAXRM");
        put(CodePoint.VALNSPRM, "VALNSPRM");
        put(CodePoint.EXCSATRD, "EXCSATRD");
        put(CodePoint.ACCRDB, "ACCRDB");
        put(CodePoint.CLSQRY, "CLSQRY");
        put(CodePoint.CNTQRY, "CNTQRY");
        put(CodePoint.DSCSQLSTT, "DSCSQLSTT");
        put(CodePoint.EXCSQLIMM, "EXCSQLIMM");
        put(CodePoint.EXCSQLSTT, "EXCSQLSTT");
        put(CodePoint.OPNQRY, "OPNQRY");
        put(CodePoint.OUTOVR, "OUTOVR");
        put(CodePoint.PRPSQLSTT, "PRPSQLSTT");
        put(CodePoint.RDBCMM, "RDBCMM");
        put(CodePoint.RDBRLLBCK, "RDBRLLBCK");
        put(CodePoint.DSCRDBTBL, "DSCRDBTBL");
        put(CodePoint.ACCRDBRM, "ACCRDBRM");
        put(CodePoint.QRYNOPRM, "QRYNOPRM");
        put(CodePoint.RDBATHRM, "RDBATHRM");
        put(CodePoint.RDBNACRM, "RDBNACRM");
        put(CodePoint.OPNQRYRM, "OPNQRYRM");
        put(CodePoint.RDBACCRM, "RDBACCRM");
        put(CodePoint.ENDQRYRM, "ENDQRYRM");
        put(CodePoint.ENDUOWRM, "ENDUOWRM");
        put(CodePoint.ABNUOWRM, "ABNUOWRM");
        put(CodePoint.DTAMCHRM, "DTAMCHRM");
        put(CodePoint.QRYPOPRM, "QRYPOPRM");
        put(CodePoint.RDBNFNRM, "RDBNFNRM");
        put(CodePoint.OPNQFLRM, "OPNQFLRM");
        put(CodePoint.SQLERRRM, "SQLERRRM");
        put(CodePoint.RDBUPDRM, "RDBUPDRM");
        put(CodePoint.RSLSETRM, "RSLSETRM");
        put(CodePoint.RDBAFLRM, "RDBAFLRM");
        put(CodePoint.SQLCARD, "SQLCARD");
        put(CodePoint.SQLDARD, "SQLDARD");
        put(CodePoint.SQLDTA, "SQLDTA");
        put(CodePoint.SQLDTARD, "SQLDTARD");
        put(CodePoint.SQLSTT, "SQLSTT");
        put(CodePoint.QRYDSC, "QRYDSC");
        put(CodePoint.QRYDTA, "QRYDTA");
        put(CodePoint.PRCCNVRM, "PRCCNVRM");
        put(CodePoint.EXCSQLSET, "EXCSQLSET");
        put(CodePoint.EXTDTA, "EXTDTA");
        put(CodePoint.PBSD, "PBSD");
        put(CodePoint.PBSD_ISO, "PBSD_ISO");
        put(CodePoint.PBSD_SCHEMA, "PBSD_SCHEMA");
    }

    String lookup(int codePoint) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        return get(codePoint);
    }
}

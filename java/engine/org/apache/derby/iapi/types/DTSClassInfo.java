/*

   Derby - Class org.apache.derby.iapi.types.DTSClassInfo

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.types;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatableInstanceGetter;

public class DTSClassInfo extends FormatableInstanceGetter {

        public Object getNewInstance() {

                switch (fmtId) {
                /* Wrappers */
                case StoredFormatIds.SQL_BIT_ID: return new SQLBit();
                case StoredFormatIds.SQL_BOOLEAN_ID: return new SQLBoolean();
                case StoredFormatIds.SQL_CHAR_ID: return new SQLChar();
                case StoredFormatIds.SQL_DATE_ID: return new SQLDate();
                case StoredFormatIds.SQL_DECIMAL_ID: return new SQLDecimal();
                case StoredFormatIds.SQL_DOUBLE_ID: return new SQLDouble();
                case StoredFormatIds.SQL_INTEGER_ID: return new SQLInteger();
                case StoredFormatIds.SQL_LONGINT_ID: return new SQLLongint();
                case StoredFormatIds.SQL_NATIONAL_CHAR_ID: return new SQLNationalChar();
                case StoredFormatIds.SQL_NATIONAL_LONGVARCHAR_ID: return new SQLNationalLongvarchar();
                case StoredFormatIds.SQL_NATIONAL_VARCHAR_ID: return new SQLNationalVarchar();
                case StoredFormatIds.SQL_REAL_ID: return new SQLReal();
                case StoredFormatIds.SQL_REF_ID: return new SQLRef();
                case StoredFormatIds.SQL_SMALLINT_ID: return new SQLSmallint();
                case StoredFormatIds.SQL_TIME_ID: return new SQLTime();
                case StoredFormatIds.SQL_TIMESTAMP_ID: return new SQLTimestamp();
                case StoredFormatIds.SQL_TINYINT_ID: return new SQLTinyint();
                case StoredFormatIds.SQL_VARCHAR_ID: return new SQLVarchar();
                case StoredFormatIds.SQL_LONGVARCHAR_ID: return new SQLLongvarchar();
                case StoredFormatIds.SQL_VARBIT_ID: return new SQLVarbit();
                case StoredFormatIds.SQL_LONGVARBIT_ID: return new SQLLongVarbit();
                case StoredFormatIds.SQL_USERTYPE_ID_V3: return new UserType();
                case StoredFormatIds.SQL_BLOB_ID: return new SQLBlob();
                case StoredFormatIds.SQL_CLOB_ID: return new SQLClob();
                case StoredFormatIds.SQL_NCLOB_ID: return new SQLNClob();

                /* Type ids */
                case StoredFormatIds.BIT_TYPE_ID: 
                case StoredFormatIds.BOOLEAN_TYPE_ID: 
                case StoredFormatIds.CHAR_TYPE_ID: 
                case StoredFormatIds.DATE_TYPE_ID: 
                case StoredFormatIds.DECIMAL_TYPE_ID: 
                case StoredFormatIds.DOUBLE_TYPE_ID: 
                case StoredFormatIds.INT_TYPE_ID: 
                case StoredFormatIds.LONGINT_TYPE_ID: 
                case StoredFormatIds.LONGVARBIT_TYPE_ID: 
                case StoredFormatIds.LONGVARCHAR_TYPE_ID: 
                case StoredFormatIds.NATIONAL_CHAR_TYPE_ID: 
                case StoredFormatIds.NATIONAL_LONGVARCHAR_TYPE_ID: 
                case StoredFormatIds.NATIONAL_VARCHAR_TYPE_ID: 
                case StoredFormatIds.REAL_TYPE_ID: 
                case StoredFormatIds.REF_TYPE_ID: 
                case StoredFormatIds.SMALLINT_TYPE_ID: 
                case StoredFormatIds.TIME_TYPE_ID: 
                case StoredFormatIds.TIMESTAMP_TYPE_ID: 
                case StoredFormatIds.TINYINT_TYPE_ID: 
                case StoredFormatIds.USERDEFINED_TYPE_ID_V3: 
                case StoredFormatIds.VARBIT_TYPE_ID: 
                case StoredFormatIds.VARCHAR_TYPE_ID: 
                case StoredFormatIds.BLOB_TYPE_ID:
                case StoredFormatIds.CLOB_TYPE_ID:
                case StoredFormatIds.NCLOB_TYPE_ID:
                        return new TypeId(fmtId);
                default:
                        return null;
                }

        }
}

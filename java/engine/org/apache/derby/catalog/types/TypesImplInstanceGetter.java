/*

   Derby - Class org.apache.derby.catalog.types.TypesImplInstanceGetter

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

package org.apache.derby.catalog.types;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatableInstanceGetter;

public class TypesImplInstanceGetter extends FormatableInstanceGetter {

        public Object getNewInstance() {

                switch (fmtId) {
                  case StoredFormatIds.BOOLEAN_TYPE_ID_IMPL:
                  case StoredFormatIds.INT_TYPE_ID_IMPL:
                  case StoredFormatIds.SMALLINT_TYPE_ID_IMPL:
                  case StoredFormatIds.TINYINT_TYPE_ID_IMPL:
                  case StoredFormatIds.LONGINT_TYPE_ID_IMPL:
                  case StoredFormatIds.DOUBLE_TYPE_ID_IMPL:
                  case StoredFormatIds.REAL_TYPE_ID_IMPL:
                  case StoredFormatIds.REF_TYPE_ID_IMPL:
                  case StoredFormatIds.CHAR_TYPE_ID_IMPL:
                  case StoredFormatIds.VARCHAR_TYPE_ID_IMPL:
                  case StoredFormatIds.LONGVARCHAR_TYPE_ID_IMPL:
                  case StoredFormatIds.NATIONAL_CHAR_TYPE_ID_IMPL:
                  case StoredFormatIds.NATIONAL_VARCHAR_TYPE_ID_IMPL:
                  case StoredFormatIds.NATIONAL_LONGVARCHAR_TYPE_ID_IMPL:
                  case StoredFormatIds.BIT_TYPE_ID_IMPL:
                  case StoredFormatIds.VARBIT_TYPE_ID_IMPL:
                  case StoredFormatIds.LONGVARBIT_TYPE_ID_IMPL:
                  case StoredFormatIds.DATE_TYPE_ID_IMPL:
                  case StoredFormatIds.TIME_TYPE_ID_IMPL:
                  case StoredFormatIds.TIMESTAMP_TYPE_ID_IMPL:
                  case StoredFormatIds.BLOB_TYPE_ID_IMPL:
                  case StoredFormatIds.CLOB_TYPE_ID_IMPL:
                  case StoredFormatIds.NCLOB_TYPE_ID_IMPL:
                          return new BaseTypeIdImpl(fmtId);
                  case StoredFormatIds.DECIMAL_TYPE_ID_IMPL:
                          return new DecimalTypeIdImpl();
                  default:
                        return null;
                }
        }
}

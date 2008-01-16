/*

   Derby - Class org.apache.derby.iapi.types.DTSClassInfo

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

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
        	
        	// Does not handle StoredFormatIds.SQL_DECIMAL_ID as different
        	// implementations are required for different VMs.

        	//The format id for DVDs are handled first.  
        	Object returnObject = DataValueFactoryImpl.getNullDVDWithUCS_BASICcollation(fmtId);
        	if (returnObject != null) return returnObject;
        	//If we are still here, then it means that we are not working with
        	//format id for DVD. Handle the other format ids in following code.
        	switch (fmtId) {        	
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
                case StoredFormatIds.XML_TYPE_ID:
                case StoredFormatIds.ROW_MULTISET_CATALOG_ID:
                        return new TypeId(fmtId);
                default:
                        return null;
                }

        }
}

/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.catalog.types
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.catalog.types;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatableInstanceGetter;

public class TypesImplInstanceGetter extends FormatableInstanceGetter {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

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

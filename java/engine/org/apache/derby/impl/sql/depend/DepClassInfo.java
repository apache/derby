/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.depend
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.depend;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatableInstanceGetter;

// lifted from TypeFactoryImpl.DTSClassInfo
public class DepClassInfo extends FormatableInstanceGetter { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

	public Object getNewInstance() {
		switch (fmtId) 
		{
			case StoredFormatIds.PROVIDER_INFO_V02_ID:
				return new BasicProviderInfo();
			default:
				return null;
		}
	}
}

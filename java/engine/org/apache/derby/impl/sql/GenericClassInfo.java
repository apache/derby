/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatableInstanceGetter;

public class GenericClassInfo extends FormatableInstanceGetter 
{

	public Object getNewInstance() 
	{

		switch (fmtId) 
		{
		//case  StoredFormatIds.GENERIC_COLUMN_DESCRIPTOR_V01_ID:
			///return new GenericColumnDescriptor(20);
		default:
			return null;
		}
	}
}

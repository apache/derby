/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.catalog
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatableInstanceGetter;

public class CoreDDFinderClassInfo extends FormatableInstanceGetter { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

	public Object getNewInstance() 
	{
		switch (fmtId) 
		{
			/* DependableFinders */
			case StoredFormatIds.ALIAS_DESCRIPTOR_FINDER_V01_ID: 
			case StoredFormatIds.CONGLOMERATE_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.CONSTRAINT_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.DEFAULT_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.FILE_INFO_FINDER_V01_ID:
			case StoredFormatIds.SCHEMA_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.SPS_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.TABLE_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.TRIGGER_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.VIEW_DESCRIPTOR_FINDER_V01_ID:
				return new DDdependableFinder(fmtId);
			case StoredFormatIds.COLUMN_DESCRIPTOR_FINDER_V01_ID:
				return new DDColumnDependableFinder(fmtId);
			default:
				return null;
		}

	}
}

/*

   Derby - Class org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo

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

package	org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatableInstanceGetter;

public class CoreDDFinderClassInfo extends FormatableInstanceGetter {

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

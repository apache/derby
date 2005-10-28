/*

   Derby - Class org.apache.derby.impl.store.access.PC_XenaVersion

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.store.access;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.catalog.UUID;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.StreamCorruptedException;
import java.util.Enumeration;
import java.util.Properties;

public class PC_XenaVersion implements Formatable
{
	private static final int XENA_MAJOR_VERSION = 1;
	private static final int XENA_MINOR_VERSION_0 = 0;

	//
	//Persistent state. The default value defined here is 
	//over-ridden by readExternal when reading serialized
	//versions.
	private int minorVersion = XENA_MINOR_VERSION_0;
	

	private boolean isUpgradeNeeded(PC_XenaVersion fromVersion)
	{
		return
			fromVersion == null ||
			getMajorVersionNumber() != fromVersion.getMajorVersionNumber();
	}

	public void upgradeIfNeeded(TransactionController tc,
								PropertyConglomerate pc,
								Properties serviceProperties)
		 throws StandardException
	{
		PC_XenaVersion dbVersion =
			(PC_XenaVersion)pc.getProperty(tc,DataDictionary.PROPERTY_CONGLOMERATE_VERSION);
		if (isUpgradeNeeded(dbVersion))
		{
			throw StandardException.newException(SQLState.UPGRADE_UNSUPPORTED, dbVersion, this);
		}
	}

	public int getMajorVersionNumber() {return XENA_MAJOR_VERSION;}
	public int getMinorVersionNumber() {return minorVersion;}
	
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeInt(getMajorVersionNumber());
		out.writeInt(getMinorVersionNumber());
	}

	public void readExternal(ObjectInput in) throws IOException
	{
		int majorVersion = in.readInt();
		minorVersion = in.readInt();
	}

	public int getTypeFormatId() {return StoredFormatIds.PC_XENA_VERSION_ID;}

	public String toString() {return getMajorVersionNumber()+"."+getMinorVersionNumber();}
}

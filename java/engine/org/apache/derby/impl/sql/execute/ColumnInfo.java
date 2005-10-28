/*

   Derby - Class org.apache.derby.impl.sql.execute.ColumnInfo

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatIdUtil;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.catalog.DefaultInfo;
import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.iapi.services.io.FormatableIntHolder;
import org.apache.derby.iapi.services.io.FormatableLongHolder;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 *	This is the Column descriptor that is passed from Compilation to Execution
 *	for CREATE TABLE statements.
 *
 *	@version 0.1
 *	@author Rick Hillegas
 */

public class ColumnInfo implements Formatable
{
	/********************************************************
	**
	**	This class implements Formatable. That means that it
	**	can write itself to and from a formatted stream. If
	**	you add more fields to this class, make sure that you
	**	also write/read them with the writeExternal()/readExternal()
	**	methods.
	**
	**	If, inbetween releases, you add more fields to this class,
	**	then you should bump the version number emitted by the getTypeFormatId()
	**	method.
	**
	********************************************************/

	public  int							action;
	public	String						name;
	public	DataTypeDescriptor			dataType;
	public	DefaultInfo					defaultInfo;
	public	DataValueDescriptor			defaultValue;
	public	UUID						newDefaultUUID;
	public	UUID						oldDefaultUUID;
	// autoinc columns.
	public long 						autoincStart;
	public long 						autoincInc;

	public static final int CREATE					= 0;
	public static final int MODIFY_COLUMN_DEFAULT	= 1;
	public static final int DROP					= 2;
	public static final int MODIFY_COLUMN_TYPE      = 3;
	public static final int MODIFY_COLUMN_CONSTRAINT = 4;
	public static final int MODIFY_COLUMN_CONSTRAINT_NOT_NULL = 5;
	// CONSTRUCTORS

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
    public	ColumnInfo() {}

	/**
	 *	Make one of these puppies.
	 *
	 *  @param name			Column name.
	 *  @param dataType		Column type.
	 *  @param defaultValue	Column default value.
	 *  @param defaultInfo	Column default info.
	 *  @param newDefaultUUID	New UUID for default.
	 *  @param oldDefaultUUID	Old UUID for default.
	 *	@param action		Action (create, modify default, etc.)
	 * 	@param autoincStart Start of autoincrement values.
	 *  @param autoincInc	Increment of autoincrement values-- if parameter
	 *						is 0, it implies that this is not an autoincrement
	 *						value.
	 */
	public	ColumnInfo(
		               String						name,
					   DataTypeDescriptor			dataType,
					   DataValueDescriptor			defaultValue,
					   DefaultInfo					defaultInfo,
					   UUID							newDefaultUUID,
					   UUID							oldDefaultUUID,
					   int							action,
					   long							autoincStart,
					   long							autoincInc)
	{
		this.name = name;
		this.dataType = dataType;
		this.defaultValue = defaultValue;
		this.defaultInfo = defaultInfo;
		this.newDefaultUUID = newDefaultUUID;
		this.oldDefaultUUID = oldDefaultUUID;
		this.action = action;
		this.autoincStart = autoincStart;
		this.autoincInc = autoincInc;
	}

	// Formatable methods

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal( ObjectInput in )
		 throws IOException, ClassNotFoundException
	{

		FormatableLongHolder flh;

		FormatableHashtable fh = (FormatableHashtable)in.readObject();
		name = (String)fh.get("name");
		dataType = (DataTypeDescriptor) fh.get("dataType");
		defaultValue = (DataValueDescriptor)fh.get("defaultValue");
		defaultInfo = (DefaultInfo)fh.get("defaultInfo");
		newDefaultUUID = (UUID)fh.get("newDefaultUUID");
		oldDefaultUUID = (UUID)fh.get("oldDefaultUUID");
		action = fh.getInt("action");
		
		if (fh.get("autoincStart") != null)
		{
			autoincStart = fh.getLong("autoincStart");
			autoincInc = fh.getLong("autoincInc");
		}
		else
		{
			autoincInc = autoincStart = 0;
		}
	}

	/**
	 * Write this object to a stream of stored objects.
	 *
	 * @param out write bytes here.
	 *
	 * @exception IOException		thrown on error
	 */
	public void writeExternal( ObjectOutput out )
		 throws IOException
	{
		FormatableHashtable fh = new FormatableHashtable();
		fh.put("name", name);
		fh.put("dataType", dataType);
		fh.put("defaultValue", defaultValue);
		fh.put("defaultInfo", defaultInfo);
		fh.put("newDefaultUUID", newDefaultUUID);
		fh.put("oldDefaultUUID", oldDefaultUUID );
		fh.putInt("action", action);
		
		if (autoincInc != 0)
		{
			// only write out autoinc values if its an autoinc column.
			fh.putLong("autoincStart", autoincStart);
			fh.putLong("autoincInc", autoincInc);
		}
		out.writeObject(fh);
	}
 
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.COLUMN_INFO_V02_ID; }

	/*
	  Object methods.
	  */
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			String	traceName;
			String  traceDataType;
			String  traceDefaultValue;
			String  traceDefaultInfo;
			String  traceNewDefaultUUID;
			String  traceOldDefaultUUID;
			String  traceAction;
			String  traceAI;
			if (name == null)
			{
				traceName = "name: null ";
			}
			else
			{
				traceName = "name: "+name+" ";
			}

			if (dataType == null)
			{
				traceDataType = "dataType: null ";
			}
			else
			{
				traceDataType = "dataType: "+dataType+" ";
			}

			if (defaultValue == null)
			{
				traceDefaultValue = "defaultValue: null ";
			}
			else
			{
				traceDefaultValue = "defaultValue: "+defaultValue+" ";
			}

			if (defaultInfo == null)
			{
				traceDefaultInfo = "defaultInfo: null ";
			}
			else
			{
				traceDefaultInfo = "defaultInfo: "+defaultInfo+" ";
			}

			if (newDefaultUUID == null)
			{
				traceNewDefaultUUID = "newDefaultUUID: null ";
			}
			else
			{
				traceNewDefaultUUID = "newDefaultUUID: "+newDefaultUUID+" ";
			}

			if (oldDefaultUUID == null)
			{
				traceOldDefaultUUID = "oldDefaultUUID: null ";
			}
			else
			{
				traceOldDefaultUUID = "oldDefaultUUID: "+oldDefaultUUID+" ";
			}

			traceAction = "action: "+action+" ";

			if (autoincInc != 0)
			{
				traceAI = "autoincrement, start: " + autoincStart +
					" increment:" + autoincInc;
			}
			else
			{
				traceAI = "NOT autoincrement";
			}
			return "ColumnInfo: ("+traceName+traceDataType+traceDefaultValue+
							   traceDefaultInfo+traceNewDefaultUUID+traceOldDefaultUUID+traceAction+traceAI+")";
		}
		else
		{
			return "";
		}
	}
}

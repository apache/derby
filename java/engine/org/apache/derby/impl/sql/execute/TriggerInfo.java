/*

   Derby - Class org.apache.derby.impl.sql.execute.TriggerInfo

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.GenericDescriptorList;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.catalog.UUID;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.util.Enumeration;

import java.util.Vector;

/**
 * This is a simple class used to store the run time information
 * about a foreign key.  Used by DML to figure out what to
 * check.
 *
 * @author jamie
 */
public class TriggerInfo implements Formatable 
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
	**	method.  OR, since this is something that is used
	**	in stored prepared statements, it is ok to change it
	**	if you make sure that stored prepared statements are
	**	invalidated across releases.
	**
	********************************************************/

	public TriggerDescriptor[] 	triggerArray; 
	public String[]				columnNames;
	public int[]				columnIds;

	/**
	 * Niladic constructor for Formattable
	 */
	public TriggerInfo() {}

	/**
	 * Constructor for TriggerInfo
	 *
	 * @param td the table upon which the trigger is declared
	 * @param changedCols the columns that are changed in the dml that is
	 *		causing the trigger to fire
	 * @param triggers the list of trigger descriptors
	 * 	
	 * @exception StandardException on error
	 */
	public TriggerInfo
	(
		TableDescriptor			td,
		int[]					changedCols,
		GenericDescriptorList	triggers
	) throws StandardException
	{
		this.columnIds = changedCols;

		if (columnIds != null)
		{
			/*
			** Find the names of all the columns that are
			** being changd.
			*/
			columnNames = new String[columnIds.length];
			for (int i = 0; i < columnIds.length; i++)
			{
				columnNames[i] = td.getColumnDescriptor(columnIds[i]).getColumnName();
			}
		}

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(triggers != null, "null trigger descriptor list");
			SanityManager.ASSERT(triggers.size() > 0, "trigger descriptor list has no elements");
		}

		/*
		** Copy the trigger descriptors into an array of the right type
		*/
		Enumeration descs =  triggers.elements();
		
		int size = triggers.size();
		triggerArray = new TriggerDescriptor[size];

		for (int i = 0; i < size; i++)
		{
			triggerArray[i] = (TriggerDescriptor) descs.nextElement();
		}
	}

	/*
	 * private constructor for TriggerInfo
	 */
	private TriggerInfo
	(
		TriggerDescriptor[]		triggers,
		int[]					changedColsIds,
		String[]				changedColsNames
	) 
	{
		this.columnIds = changedColsIds;
		this.columnNames = changedColsNames;
		this.triggerArray = triggers;
	}

	/**
	 * Do we have a trigger or triggers that meet
	 * the criteria
	 *
	 * @param isBefore	true for a before trigger, false
	 *					for after trigger, null for either
	 * @param isRow		true for a row trigger, false
	 *					for statement trigger, null for either
	 *
	 * @return true if we have a trigger that meets the
	 * 		criteria
	 */
	boolean hasTrigger(boolean isBefore, boolean isRow)
	{
		if (triggerArray == null)
		{
			return false;
		}

		return hasTrigger(new Boolean(isBefore), new Boolean(isRow));
	}

	/**
	 * Do we have a trigger or triggers that meet
	 * the criteria
	 *
	 * @param isBefore	true for a before trigger, false
	 *					for after trigger, null for either
	 * @param isRow		true for a row trigger, false
	 *					for statement trigger, null for either
	 *
	 * @return true if we have a trigger that meets the
	 * 		criteria
	 */
	private boolean hasTrigger(Boolean isBefore, Boolean isRow)
	{
		if (triggerArray == null)
		{
			return false;
		}
		for (int i = 0; i < triggerArray.length; i++)
		{
			if (((isBefore == null) || 
					(triggerArray[i].isBeforeTrigger() == isBefore.booleanValue())) &&
			    ((isRow == null) || 
					(triggerArray[i].isRowTrigger() == isRow.booleanValue())))
			{
				return true;
			}
		}
		return false;
	}

	TriggerDescriptor[] getTriggerArray()
	{
		return triggerArray;
	}
	//////////////////////////////////////////////
	//
	// FORMATABLE
	//
	//////////////////////////////////////////////
	/**
	 * Write this object out
	 *
	 * @param out write bytes here
	 *
 	 * @exception IOException thrown on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		ArrayUtil.writeArray(out, triggerArray);
		ArrayUtil.writeIntArray(out, columnIds);
		ArrayUtil.writeArray(out, columnNames);
	}

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal(ObjectInput in)
		throws IOException, ClassNotFoundException
	{
		triggerArray = new TriggerDescriptor[ArrayUtil.readArrayLength(in)];
		ArrayUtil.readArrayItems(in, triggerArray);

		columnIds = ArrayUtil.readIntArray(in);

		int len = ArrayUtil.readArrayLength(in);
		if (len > 0)
		{
			columnNames = new String[len];
			ArrayUtil.readArrayItems(in, columnNames);
		}
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int getTypeFormatId()	{ return StoredFormatIds.TRIGGER_INFO_V01_ID; }

	//////////////////////////////////////////////////////////////
	//
	// Misc
	//
	//////////////////////////////////////////////////////////////
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			StringBuffer str = new StringBuffer();
			str.append("\nColumn names modified:\t\t(");
			for (int i = 0; i < columnNames.length; i++)
			{
				if (i > 0)
					str.append(",");
			
				str.append(columnNames[i]);
			}
			str.append(")");

			str.append("\nColumn ids modified:\t\t(");
			for (int i = 0; i < columnIds.length; i++)
			{
				if (i > 0)
					str.append(",");
			
				str.append(columnIds[i]);
			}
			str.append(")");

			str.append("\nTriggers:");
			for (int i = 0; i < triggerArray.length; i++)
			{
				str.append("\n"+triggerArray[i]);
			}
			return str.toString();
		}
		else
		{
			return "";
		}
	}
}

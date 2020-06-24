/*

   Derby - Class org.apache.derby.impl.sql.execute.TriggerInfo

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.sql.dictionary.TriggerDescriptorList;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;

import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.shared.common.util.ArrayUtil;
import org.apache.derby.iapi.services.io.Formatable;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;


/**
 * This is a simple class used to store the run time information
 * about a foreign key.  Used by DML to figure out what to
 * check.
 *
 */
public final class TriggerInfo implements Formatable 
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

	TriggerDescriptor[] 	triggerArray; 

	/**
	 * Niladic constructor for Formattable
	 */
	public TriggerInfo() {}

	/**
	 * Constructor for TriggerInfo
	 *
	 * @param triggers the list of trigger descriptors
	 * 	
	 */
	public TriggerInfo
	(
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        TriggerDescriptorList   triggers
	)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(triggers != null, "null trigger descriptor list");
			SanityManager.ASSERT(triggers.size() > 0, "trigger descriptor list has no elements");
		}

		/*
		** Copy the trigger descriptors into an array of the right type
		*/
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        triggerArray = triggers.toArray(new TriggerDescriptor[triggers.size()]);
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
		for (int i = 0; i < triggerArray.length; i++)
		{
            if ((triggerArray[i].isBeforeTrigger() == isBefore) &&
                (triggerArray[i].isRowTrigger() == isRow))
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
    @Override
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
            StringBuilder str = new StringBuilder();
			str.append("\nTriggers:");
			for (int i = 0; i < triggerArray.length; i++)
			{
                str.append('\n');
                str.append(triggerArray[i]);
			}
			return str.toString();
		}
		else
		{
			return "";
		}
	}
}

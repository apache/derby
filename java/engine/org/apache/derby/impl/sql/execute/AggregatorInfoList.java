/*

   Derby - Class org.apache.derby.impl.sql.execute.AggregatorInfoList

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.Formatable;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.util.Vector;

/**
 * Vector of AggergatorInfo objects.
 *
 * @see java.util.Vector
 *
 * @author jamie
 */
public class AggregatorInfoList extends Vector implements Formatable 
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

	/**
	 * Niladic constructor for Formatable
	 */
	public AggregatorInfoList() {}

	/**
	 * Indicate whether i have a distinct or not.
	 *
	 * @return indicates if there is a distinct
	 */
	public boolean hasDistinct()
	{
		int count = size();
		for (int i = 0; i < count; i++)
		{
			AggregatorInfo aggInfo = (AggregatorInfo) elementAt(i);
			if (aggInfo.isDistinct())
			{
				return true;
			}
		}
		return false;
	}

	//////////////////////////////////////////////
	//
	// FORMATABLE
	//
	//////////////////////////////////////////////

	/** @exception  IOException thrown on error */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		int count = size();
		out.writeInt(count);
		for (int i = 0; i < count; i++)
		{
			out.writeObject(elementAt(i));
		}
	}

	/** 
	 * @see java.io.Externalizable#readExternal 
	 *
	 * @exception IOException on error	
	 * @exception ClassNotFoundException on error	
	 */
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
	{
		int count = in.readInt();

		ensureCapacity(count);
		for (int i = 0; i < count; i++)
		{
			AggregatorInfo agg = (AggregatorInfo)in.readObject();
			addElement(agg);
		}	
	}

	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.AGG_INFO_LIST_V01_ID; }

	///////////////////////////////////////////////////////////////
	//
	// OBJECT INTERFACE
	//
	///////////////////////////////////////////////////////////////
}

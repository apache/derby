/*

   Derby - Class org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl

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

package org.apache.derby.catalog.types;


import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.catalog.ReferencedColumns;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.IOException;

public class ReferencedColumnsDescriptorImpl
	implements ReferencedColumns, Formatable
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

	private int[] referencedColumns;

	/**
	 * Constructor for an ReferencedColumnsDescriptorImpl
	 *
	 * @param referencedColumns The array of referenced columns.
	 */

	public ReferencedColumnsDescriptorImpl(	int[] referencedColumns)
	{
		this.referencedColumns = referencedColumns;
	}

	/** Zero-argument constructor for Formatable interface */
	public ReferencedColumnsDescriptorImpl()
	{
	}
	/**
	* @see ReferencedColumns#getReferencedColumnPositions
	*/
	public int[] getReferencedColumnPositions()
	{
		return referencedColumns;
	}

	/* Externalizable interface */

	/**
	 * @see java.io.Externalizable#readExternal
	 *
	 * @exception IOException	Thrown on read error
	 */
	public void readExternal(ObjectInput in) throws IOException
	{
		int rcLength = in.readInt();
		referencedColumns = new int[rcLength];
		for (int i = 0; i < rcLength; i++)
		{
			referencedColumns[i] = in.readInt();
		}
	}

	/**
	 * @see java.io.Externalizable#writeExternal
	 *
	 * @exception IOException	Thrown on write error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeInt(referencedColumns.length);
		for (int i = 0; i < referencedColumns.length; i++)
		{
			out.writeInt(referencedColumns[i]);
		}
	}

	/* TypedFormat interface */
	public int getTypeFormatId()
	{
		return StoredFormatIds.REFERENCED_COLUMNS_DESCRIPTOR_IMPL_V01_ID;
	}

	/**
	  @see java.lang.Object#toString
	  */
	public String	toString()
	{
		StringBuffer sb = new StringBuffer(60);

		sb.append('(');
		for (int index = 0; index < referencedColumns.length; index++)
		{
			if (index > 0)
				sb.append(',');
			sb.append(String.valueOf(referencedColumns[index]));

		}
		sb.append(')');
		return sb.toString();
	}
}

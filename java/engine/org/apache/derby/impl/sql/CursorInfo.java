/*

   Derby - Class org.apache.derby.impl.sql.CursorInfo

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

package org.apache.derby.impl.sql;

import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.execute.ExecCursorTableReference;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.Formatable;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
/**
 * A basic holder for information about cursors
 * for execution.
 * 
 * @author jamie
 */
public class CursorInfo
	implements Formatable
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

	ExecCursorTableReference	targetTable; 
	ResultColumnDescriptor[]	targetColumns; 
	String[] 					updateColumns; 
	int 						updateMode;

	/**
	 * Niladic constructor for Formatable
	 */
	public CursorInfo()
	{
	}

	/**
	 *
	 */
	public CursorInfo
	(
		int							updateMode,
		ExecCursorTableReference	targetTable,
		ResultColumnDescriptor[]	targetColumns,
		String[]					updateColumns
	)
	{
		this.updateMode = updateMode;
		this.targetTable = targetTable;
		this.targetColumns = targetColumns;
		this.updateColumns = updateColumns;
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
		out.writeInt(updateMode);
		out.writeObject(targetTable);
		ArrayUtil.writeArray(out, targetColumns);
		ArrayUtil.writeArray(out, updateColumns);
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
		updateMode = in.readInt();
		targetTable = (ExecCursorTableReference)in.readObject();
		int len = ArrayUtil.readArrayLength(in);
		if (len != 0)
		{
			targetColumns = new ResultColumnDescriptor[len];
			ArrayUtil.readArrayItems(in, targetColumns);
		}
		len = ArrayUtil.readArrayLength(in);
		if (len != 0)
		{
			updateColumns = new String[len];
			ArrayUtil.readArrayItems(in, updateColumns);
		}
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.CURSOR_INFO_V01_ID; }

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			StringBuffer strbuf = new StringBuffer();
		
			strbuf.append("CursorInfo"+
				"\n\tupdateMode: "+updateMode+
				"\n\ttargetTable: "+targetTable+
				"\n\tupdateColumns: ");

			if (updateColumns == null)
			{
				strbuf.append("NULL\n");
			}
			else
			{
				strbuf.append("{");
				for (int i = 0; i < updateColumns.length; i++)
				{
					if (i > 0)
						strbuf.append(",");
					strbuf.append(updateColumns[i]);
				}
				strbuf.append(")\n");
			}

			strbuf.append("\tTargetColumnDescriptors: \n");
			if (targetColumns == null)
			{
				strbuf.append("NULL");
			}
			else
			{
				for (int i = 0; i < targetColumns.length; i++)
				{
					strbuf.append(targetColumns[i]);
				}
				strbuf.append("\n");
			}
			return strbuf.toString();	
		}
		else
		{
			return "";
		}
	}
}

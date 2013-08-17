/*

   Derby - Class org.apache.derby.impl.sql.CursorInfo

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

package org.apache.derby.impl.sql;

import org.apache.derby.iapi.sql.execute.ExecCursorTableReference;
import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.Formatable;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A basic holder for information about cursors
 * for execution.
 * 
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
    List<String>          updateColumns;
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
        List<String>             updateColumns
	)
	{
		this.updateMode = updateMode;
		this.targetTable = targetTable;
        this.updateColumns = (updateColumns == null) ?
                updateColumns : new ArrayList<String>(updateColumns);
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

        // For backwards compatibility. Used to write an array of
        // target column descriptors here.
        ArrayUtil.writeArray(out, (Object[]) null);

        ArrayUtil.writeArray(out, updateColumns == null ?
                null : updateColumns.toArray());
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

        // For backwards compatibility. Read and discard array that's no
        // longer used.
        ArrayUtil.readObjectArray(in);

        int len = ArrayUtil.readArrayLength(in);
        if (len > 0) {
            updateColumns = Arrays.asList(ArrayUtil.readStringArray(in));
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
            return "CursorInfo" +
				"\n\tupdateMode: "+updateMode+
				"\n\ttargetTable: "+targetTable+
                "\n\tupdateColumns: " + updateColumns + '\n';
		}
		else
		{
			return "";
		}
	}
}

/*

   Derby - Class org.apache.derby.iapi.services.io.FormatableArrayHolder

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

package org.apache.derby.iapi.services.io;

import org.apache.derby.shared.common.sanity.SanityManager;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.util.Arrays;

import org.apache.derby.shared.common.util.ArrayUtil;

/**
 * A formatable holder for an array of formatables.
 * Used to avoid serializing arrays.
 */
public class FormatableArrayHolder implements Formatable
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

	// the array
	private Object[] array;
	
	/**
	 * Niladic constructor for formatable
	 */
	public FormatableArrayHolder() 
	{
	}

	/**
	 * Construct a FormatableArrayHolder using the input
	 * array.
	 *
	 * @param array the array to hold
	 */
	public FormatableArrayHolder(Object[] array)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(array != null, 
					"array input to constructor is null, code can't handle this.");
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-6188
		setArray( array );
	}

	/**
	 * Set the held array to the input array.
	 *
	 * @param array the array to hold
	 */
	public void setArray(Object[] array)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(array != null, 
					"array input to setArray() is null, code can't handle this.");
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-6188
		this.array = ArrayUtil.copy( array );
	}

	/**
	 * Get the held array of formatables, and return
     * it in an array that is an instance of {@code arrayClass}.
	 *
     * @param <E> The type of the array cell
     * @param arrayClass the type of array to return
	 *
	 * @return an array of formatables
	 */
    public <E> E[] getArray(Class<E[]> arrayClass)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6292
        return Arrays.copyOf(array, array.length, arrayClass);
	}

	//////////////////////////////////////////////
	//
	// FORMATABLE
	//
	//////////////////////////////////////////////
	/**
	 * Write this array out
	 *
	 * @param out write bytes here
	 *
 	 * @exception IOException thrown on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(array != null, "Array is null, which isn't expected");
		}

		ArrayUtil.writeArrayLength(out, array);
		ArrayUtil.writeArrayItems(out, array);
	}

	/**
	 * Read this array from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal(ObjectInput in)
		throws IOException, ClassNotFoundException
	{
		array = new Object[ArrayUtil.readArrayLength(in)];
		ArrayUtil.readArrayItems(in, array);
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.FORMATABLE_ARRAY_HOLDER_V01_ID; }
}

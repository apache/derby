/*

   Derby - Class org.apache.derby.iapi.services.io.FormatableArrayHolder

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

package org.apache.derby.iapi.services.io;

import org.apache.derby.iapi.services.io.ArrayInputStream;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.lang.reflect.Array;

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
	 * @array the array to hold
	 */
	public FormatableArrayHolder(Object[] array)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(array != null, 
					"array input to constructor is null, code can't handle this.");
		}

		this.array = array;
	}

	/**
	 * Set the held array to the input array.
	 *
	 * @array the array to hold
	 */
	public void setArray(Object[] array)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(array != null, 
					"array input to setArray() is null, code can't handle this.");
		}

		this.array = array;
	}

	/**
	 * Get the held array of formatables, and return
	 * it in an array of type inputClass.
	 *
	 * @param inputClass	the class to use for the returned array
	 *
	 * @return an array of formatables
	 */
	public Object[] getArray(Class inputClass)
	{
		Object[] outArray = (Object[])Array.newInstance(inputClass, array.length);
		
		/*
		** HACK: on as400 the following arraycopy() throws an
		** ArrayStoreException because the output array isn't
		** assignment compatible with the input array.  This
		** is a bug on as400, but to get around it we are
		** going to do an element by element copy.
		*/
		//System.arraycopy(array, 0, outArray, 0, outArray.length);
		for (int i = 0; i < outArray.length; i++)
		{
			outArray[i] = array[i];
		}

		return outArray;
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
	public void readExternal(ArrayInputStream in)
		throws IOException, ClassNotFoundException
	{
		array = new Formatable[ArrayUtil.readArrayLength(in)];
		ArrayUtil.readArrayItems(in, array);
	}
	
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.FORMATABLE_ARRAY_HOLDER_V01_ID; }
}

/*

   Derby - Class org.apache.derby.iapi.services.io.FormatableLongHolder

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

package org.apache.derby.iapi.services.io;

import org.apache.derby.iapi.services.io.ArrayInputStream;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 * A formatable holder for an long.
 */
public class FormatableLongHolder implements Formatable
{

	// the int
	private long theLong;
	
	/**
	 * Niladic constructor for formatable
	 */
	public FormatableLongHolder() 
	{
	}

	/**
	 * Construct a FormatableLongHolder using the input integer.
	 *
	 * @param theLong the long to hold
	 */
	public FormatableLongHolder(long theLong)
	{
		this.theLong = theLong;
	}

	/**
	 * Set the held int to the input int.
	 *
	 * @param theInt the int to hold
	 */
	public void setLong(int theLong)
	{
		this.theLong = theLong;
	}

	/**
	 * Get the held int.
	 *
	 * @return	The held int.
	 */
	public long getLong()
	{
		return theLong;
	}

	/**
	 * Create and return an array of FormatableLongHolders
	 * given an array of ints.
	 *
	 * @param theInts	The array of ints
	 *
	 * @return	An array of FormatableLongHolders
	 */
	public static FormatableLongHolder[] getFormatableLongHolders(long[] theLongs)
	{
		if (theLongs == null)
		{
			return null;
		}

		FormatableLongHolder[] flhArray = new FormatableLongHolder[theLongs.length];

		for (int index = 0; index < theLongs.length; index++)
		{
			flhArray[index] = new FormatableLongHolder(theLongs[index]);
		}
		return flhArray;
	}

	//////////////////////////////////////////////
	//
	// FORMATABLE
	//
	//////////////////////////////////////////////
	/**
	 * Write this formatable out
	 *
	 * @param out write bytes here
	 *
 	 * @exception IOException thrown on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeLong(theLong);
	}

	/**
	 * Read this formatable from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 */
	public void readExternal(ObjectInput in)
		throws IOException
	{
		theLong = in.readLong();
	}
	public void readExternal(ArrayInputStream in)
		throws IOException
	{
		theLong = in.readLong();
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.FORMATABLE_LONG_HOLDER_V01_ID; }
}

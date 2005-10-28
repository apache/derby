/*

   Derby - Class org.apache.derby.iapi.services.io.FormatableIntHolder

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

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 * A formatable holder for an int.
 */
public class FormatableIntHolder implements Formatable
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

	// the int
	private int theInt;
	
	/**
	 * Niladic constructor for formatable
	 */
	public FormatableIntHolder() 
	{
	}

	/**
	 * Construct a FormatableIntHolder using the input int.
	 *
	 * @param theInt the int to hold
	 */
	public FormatableIntHolder(int theInt)
	{
		this.theInt = theInt;
	}

	/**
	 * Set the held int to the input int.
	 *
	 * @param theInt the int to hold
	 */
	public void setInt(int theInt)
	{
		this.theInt = theInt;
	}

	/**
	 * Get the held int.
	 *
	 * @return	The held int.
	 */
	public int getInt()
	{
		return theInt;
	}

	/**
	 * Create and return an array of FormatableIntHolders
	 * given an array of ints.
	 *
	 * @param theInts	The array of ints
	 *
	 * @return	An array of FormatableIntHolders
	 */
	public static FormatableIntHolder[] getFormatableIntHolders(int[] theInts)
	{
		if (theInts == null)
		{
			return null;
		}

		FormatableIntHolder[] fihArray = new FormatableIntHolder[theInts.length];

		for (int index = 0; index < theInts.length; index++)
		{
			fihArray[index] = new FormatableIntHolder(theInts[index]);
		}
		return fihArray;
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
		out.writeInt(theInt);
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
		theInt = in.readInt();
	}
	public void readExternal(ArrayInputStream in)
		throws IOException
	{
		theInt = in.readInt();
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.FORMATABLE_INT_HOLDER_V01_ID; }
}

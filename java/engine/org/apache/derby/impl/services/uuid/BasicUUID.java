/*

   Derby - Class org.apache.derby.impl.services.uuid.BasicUUID

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.services.uuid;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.catalog.UUID;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.io.StringReader;


public class BasicUUID implements UUID, Formatable
{
	/*
	** Fields of BasicUUID
	*/
	
	private long majorId; // only using 48 bits
	private long timemillis;
	private int sequence;

	/*
	** Methods of BasicUUID
	*/

	/**
		Constructor only called by BasicUUIDFactory.
	**/
	public BasicUUID(long majorId, long timemillis, int sequence)
	{
		this.majorId = majorId;
		this.timemillis = timemillis;
		this.sequence = sequence;
	}

	/**
		Constructor only called by BasicUUIDFactory.
		Constructs a UUID from the string representation
		produced by toString.
		@see BasicUUID#toString
	**/
	public BasicUUID(String uuidstring)
	{
		StringReader sr = new StringReader(uuidstring);
		sequence = (int) readMSB(sr);

		long ltimemillis = readMSB(sr) << 32;
		ltimemillis += readMSB(sr) << 16;
		ltimemillis += readMSB(sr);
		timemillis = ltimemillis;
		majorId = readMSB(sr);
	}

	/**
		Constructor only called by BasicUUIDFactory.
		Constructs a UUID from the byte array representation
		produced by toByteArrayio.
		@see BasicUUID#toByteArray
	**/
	public BasicUUID(byte[] b)
	{
		int lsequence = 0;
		for (int ix = 0; ix < 4; ix++)
		{
			lsequence = lsequence << 8;
			lsequence = lsequence | (0xff & b[ix]);
		}

		long ltimemillis = 0;
		for (int ix = 4; ix < 10; ix++)
		{
			ltimemillis = ltimemillis << 8;
			ltimemillis = ltimemillis | (0xff & b[ix]);
		}

		long linetaddr = 0;
		for (int ix = 10; ix < 16; ix++)
		{
			linetaddr = linetaddr << 8;
			linetaddr = linetaddr | (0xff & b[ix]);
		}

		sequence = lsequence;
		timemillis = ltimemillis;
		majorId = linetaddr;
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public BasicUUID() { super(); }

	/**
		Write this out.
		@exception IOException error writing to log stream
	*/
	public void writeExternal(ObjectOutput out) throws IOException 
	{
		// RESOLVE: write out the byte array instead?
		out.writeLong(majorId);
		out.writeLong(timemillis);
		out.writeInt(sequence);
	}

	/**
		Read this in
		@exception IOException error reading from log stream
	*/
	public void readExternal(ObjectInput in) throws IOException
	{
		majorId = in.readLong();
		timemillis = in.readLong();
		sequence = in.readInt();
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.BASIC_UUID;
	}

	private static void writeMSB(char[] data, int offset, long value, int nbytes)
    {
    	for (int i = nbytes - 1; i >= 0; i--)
		{
		   long b = (value & (255L << (8 * i))) >>> (8 * i);

		   int c = (int) ((b & 0xf0) >> 4);
		   data[offset++] = (char) (c < 10 ? c + '0' : (c - 10) + 'a');
		   c = (int) (b & 0x0f);
		   data[offset++] = (char) (c < 10 ? c + '0' : (c - 10) + 'a');
		} 
    }

    /**
		Read a long value, msb first, from its character 
		representation in the string reader, using '-' or
		end of string to delimit.
	**/
	private static long readMSB(StringReader sr)
    {
		long value = 0;

		try
		{
			int c;
			while ((c = sr.read()) != -1)
			{
				if (c == '-')
					break;
				value <<= 4;

				int nibble;
				if (c <= '9')
					nibble = c - '0';
				else if (c <= 'F')
					nibble = c - 'A' + 10;
				else
					nibble = c - 'a' + 10;
				value += nibble;
			}
		}
		catch (Exception e)
		{
		}

		return value;
    }

	/*
	** Methods of UUID
	*/

	/**
		Implement value equality.

	**/
	public boolean equals(Object otherObject)
	{
		if (!(otherObject instanceof BasicUUID))
			return false;

		BasicUUID other = (BasicUUID) otherObject;

		return (this.sequence == other.sequence)
			&& (this.timemillis == other.timemillis)
			&& (this.majorId == other.majorId);
	}

	/**
		Provide a hashCode which is compatible with
		the equals() method.
	**/
	public int hashCode()
	{
		long hc = majorId ^ timemillis;

		return sequence ^ ((int) (hc >> 4));
	}

	/**
		Produce a string representation of this UUID which
		can be passed to UUIDFactory.recreateUUID later on
		to reconstruct it.  The funny representation is 
		designed to (sort of) match the format of Microsoft's
		UUIDGEN utility.
	 */
	public String toString() {return stringWorkhorse( '-' );}

	/**
		Produce a string representation of this UUID which
		is suitable for use as a unique ANSI identifier.
	 */
	public String toANSIidentifier() {return "U" + stringWorkhorse( 'X' );}

	/**
	  *	Private workhorse of the string making routines.
	  *
	  *	@param	separator	Character to separate number blocks.
	  *                     Null means do not include a separator.
	  *
	  *	@return	string representation of UUID.
	  */
	public	String	stringWorkhorse( char separator )
	{
		char[] data = new char[36];

		writeMSB(data, 0, (long) sequence, 4);

		int offset = 8;
		if (separator != 0) data[offset++] = separator;

		long ltimemillis = timemillis;
		writeMSB(data, offset, (ltimemillis & 0x0000ffff00000000L) >>> 32, 2);
		offset += 4;
		if (separator != 0) data[offset++] = separator;
		writeMSB(data, offset, (ltimemillis & 0x00000000ffff0000L) >>> 16, 2);
		offset += 4;
		if (separator != 0) data[offset++] = separator;
		writeMSB(data, offset, (ltimemillis & 0x000000000000ffffL), 2);
		offset += 4;
		if (separator != 0) data[offset++] = separator;
		writeMSB(data, offset, majorId, 6);
		offset += 12;

		return new String(data, 0, offset);
	}

	/**
	  Store this UUID in a byte array. Arrange the bytes in the UUID
	  in the same order the code which stores a UUID in a string
	  does.
	  
	  @see org.apache.derby.catalog.UUID#toByteArray
	*/
	public byte[] toByteArray()
	{
		byte[] result = new byte[16];

		int lsequence = sequence; 
		result[0] = (byte)(lsequence >>> 24);
		result[1] = (byte)(lsequence >>> 16);
		result[2] = (byte)(lsequence >>> 8);
		result[3] = (byte)lsequence;

		long ltimemillis = timemillis;
		result[4] = (byte)(ltimemillis >>> 40);
		result[5] = (byte)(ltimemillis >>> 32);
		result[6] = (byte)(ltimemillis >>> 24);
		result[7] = (byte)(ltimemillis >>> 16);
 		result[8] = (byte)(ltimemillis >>> 8);
		result[9] = (byte)ltimemillis;

		long linetaddr = majorId;
		result[10] = (byte)(linetaddr >>> 40);
		result[11] = (byte)(linetaddr >>> 32);
		result[12] = (byte)(linetaddr >>> 24);
		result[13] = (byte)(linetaddr >>> 16);
		result[14] = (byte)(linetaddr >>> 8);
		result[15] = (byte)linetaddr;

		return result;
	}

	/**
	  Clone this UUID.

	  @return	a copy of this UUID
	  */
	public UUID cloneMe()
	{
		return	new	BasicUUID(majorId, timemillis, sequence);
	}

	public String toHexString() {return stringWorkhorse( (char) 0 );}
}


/*

   Derby - Class org.apache.derby.iapi.services.io.ArrayUtil

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

package org.apache.derby.iapi.services.io;

import org.apache.derby.iapi.services.sanity.SanityManager;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.lang.reflect.Array;

/**
  Utility class for constructing and reading and writing arrays from/to
  formatId streams.
 
  @version 0.1
  @author Rick Hillegas
 */
public abstract class ArrayUtil
{
	///////////////////////////////////////////////////////////////////
	//
	// Methods for Arrays of OBJECTS.  Cannot be used for an
	// array of primitives, see below for something for primitives
	//
	///////////////////////////////////////////////////////////////////
	/**
	  Write the length of an array of objects to an output stream.

	  The length

	  @param	out		ObjectOutput stream
	  @param	a		array of objects whose length should be written.

	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static void writeArrayLength(ObjectOutput out, Object[] a)
		 throws IOException
	{
		out.writeInt(a.length);
	}

	/**
	  Write an array of objects to an output stream.

	  @param	out		Object output stream to write to.
	  @param	a		array of objects to write.

	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static void writeArrayItems(ObjectOutput out, Object[] a)
		 throws IOException
	{
		if (a == null)
			return;

		for(int ix = 0; ix < a.length; ix++)
		{	out.writeObject(a[ix]); }
	}

	/**
	  Write an array of objects and length to an output stream.
	  Does equivalent of writeArrayLength() followed by writeArrayItems()

	  @param	out		Object output stream to write to.
	  @param	a		array of objects to write.

	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static void writeArray(ObjectOutput out, Object[] a)
		 throws IOException
	{
		if (a == null) 
		{
			out.writeInt(0);
			return;
		}

		out.writeInt(a.length);
		for(int ix = 0; ix < a.length; ix++)
		{	out.writeObject(a[ix]); }
	}

	/**
	  Read an array of objects out of a stream.

	  @param	in	Input stream
	  @param	a	array to read into

	  @exception java.io.IOException The write caused an IOException. 
	  @exception java.lang.ClassNotFoundException The Class for an Object we are reading does not exist
	  */
	public static void readArrayItems(ObjectInput in, Object[] a)
		 throws IOException, ClassNotFoundException
	{
		for (int ix=0; ix<a.length; ix++)
		{
			a[ix]=in.readObject();
		}
	}

	/**
	  Read the length of an array of objects in an object stream.

	  @param	in	Input stream.

	  @return	length of the array of objects
	  
	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static int readArrayLength(ObjectInput in)
		 throws IOException
	{
		return in.readInt();
	}

	/**
	  Reads an array of objects from the stream.

	  @param	in	Input stream

	  @exception java.io.IOException The write caused an IOException. 
	  @exception java.lang.ClassNotFoundException The Class for an Object we are reading does not exist
	  */
	public static Object[] readObjectArray(ObjectInput in)
		 throws IOException, ClassNotFoundException
	{
		int	size = in.readInt();
		if ( size == 0 ) { return null; }

		Object[]	result = new Object[ size ];

		readArrayItems( in, result );

		return result;
	}

	///////////////////////////////////////////////////////////////////
	//
	// Methods for Arrays of INTs
	//
	///////////////////////////////////////////////////////////////////

	/**
	  Write an array of integers to an ObjectOutput. This writes the array
	  in a format readIntArray understands.

	  @param out the ObjectOutput.
	  @param a the array.
	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static void writeIntArray(ObjectOutput out, int[] a) throws IOException {
		if (a == null)
			out.writeInt(0);
		else {
			out.writeInt(a.length);
			for (int i=0; i<a.length; i++)
				out.writeInt(a[i]);
		}
	}

	/**
	  Read an array of integers from an ObjectInput. This allocates the
	  array.

	  @param	in	the ObjectInput.
	  @return   the array of integers.

	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static int[] readIntArray(ObjectInput in) throws IOException {
		int length = in.readInt();
		if (length == 0)
			return null;
		int[] a = new int[length];
		for (int i=0; i<length; i++)
			a[i] = in.readInt();
		return a;
	}

	public	static	void	writeInts( ObjectOutput out, int[][] val )
		throws IOException
	{
		if (val == null)
		{
			out.writeBoolean(false);
		}
		else
		{
			out.writeBoolean(true);

			int	count = val.length;
			out.writeInt( count );

			for (int i = 0; i < count; i++)
			{
				ArrayUtil.writeIntArray( out, val[i] );
			}
		}
	}

	public	static	int[][]	readInts( ObjectInput in )
		 throws IOException, ClassNotFoundException
	{
		int[][]	retVal = null;

		if ( in.readBoolean() )
		{
			int	count = in.readInt();

			retVal = new int[ count ][];

			for (int i = 0; i < count; i++)
			{
				retVal[ i ] = ArrayUtil.readIntArray( in );
			}
		}

		return retVal;
	}

	///////////////////////////////////////////////////////////////////
	//
	// Methods for Arrays of LONGs
	//
	///////////////////////////////////////////////////////////////////

	/**
	  Write an array of longs to an ObjectOutput. This writes the array
	  in a format readLongArray understands.

	  @param out the ObjectOutput.
	  @param a the array.
	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static void writeLongArray(ObjectOutput out, long[] a) throws IOException {
		if (a == null)
			out.writeInt(0);
		else {
			out.writeInt(a.length);
			for (int i=0; i<a.length; i++)
				out.writeLong(a[i]);
		}
	}

	/**
	  Read an array of integers from an ObjectInput. This allocates the
	  array.

	  @param	in	the ObjectInput.
	  @return   the array of integers.

	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static long[] readLongArray(ObjectInput in) throws IOException {
		int length = in.readInt();
		long[] a = new long[length];
		for (int i=0; i<length; i++)
			a[i] = in.readLong();
		return a;
	}

	/**
	  Read an array of strings from an ObjectInput. This allocates the
	  array.

	  @param	in	the ObjectInput.
	  @return   the array of integers.

	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static String[] readStringArray(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
		Object[] objArray = readObjectArray(in);
		int size = 0;

		if (objArray == null)
			return null;

		String[] stringArray = new String[size = objArray.length];

		for (int i = 0; i < size; i++)
		{
			stringArray[i] = (String)objArray[i];
		} 

		return stringArray;
	}
	
	///////////////////////////////////////////////////////////////////
	//
	// Methods for Arrays of BOOLEANS
	//
	///////////////////////////////////////////////////////////////////

	/**
	  Write an array of booleans to an ObjectOutput. This writes the array
	  in a format readBooleanArray understands.

	  @param out the ObjectOutput.
	  @param a the array.
	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static void writeBooleanArray(ObjectOutput out, boolean[] a) throws IOException {
		if (a == null)
			out.writeInt(0);
		else {
			out.writeInt(a.length);
			for (int i=0; i<a.length; i++)
				out.writeBoolean(a[i]);
		}
	}

	/**
	  Read an array of integers from an ObjectInput. This allocates the
	  array.

	  @param	in	the ObjectInput.
	  @return   the array of integers.

	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static boolean[] readBooleanArray(ObjectInput in) throws IOException {
		int length = in.readInt();
		boolean[] a = new boolean[length];
		for (int i=0; i<length; i++)
			a[i] = in.readBoolean();
		return a;
	}
}

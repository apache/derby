/*

   Derby - Class org.apache.derby.shared.common.util.ArrayUtil

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

package org.apache.derby.shared.common.util;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
  Utility class for constructing and reading and writing arrays from/to
  formatId streams and for performing other operations on arrays.
 
  @version 0.1
 */
public abstract class ArrayUtil
{
    /**
     * An instance of an empty byte array. Since empty arrays are immutable,
     * this instance can safely be shared. Code that needs an empty byte
     * array can use this static instance instead of allocating a new one.
     */
    public final static byte[] EMPTY_BYTE_ARRAY = {};

	///////////////////////////////////////////////////////////////////
	//
	// Methods to copy arrays.
	//
	///////////////////////////////////////////////////////////////////

    /**
     * Copy an array of objects; the original array could be null
     *
     * @param <T> The type of the array cells
     *
     * @param original The original array to copy
     *
     * @return a copy of the array
     */
    public  static <T> T[] copy( T[] original )
    {
        return (original == null) ?
                null :
                Arrays.copyOf(original, original.length);
    }

    /**
     * Copy a (possibly null) array of booleans
     *
     * @param original The original array to copy
     *
     * @return a copy of the array
     */
    public  static  boolean[]   copy( boolean[] original )
    {
        return (original == null) ? null : (boolean[]) original.clone();
    }

    /**
     * Copy a (possibly null) array of bytes
     *
     * @param original The original array to copy
     *
     * @return a copy of the array
     */
    public  static  byte[]   copy( byte[] original )
    {
        return (original == null) ? null : (byte[]) original.clone();
    }

    /**
     * Copy a (possibly null) array of ints
     *
     * @param original The original array to copy
     *
     * @return a copy of the array
     */
    public  static  int[]   copy( int[] original )
    {
        return (original == null) ? null : (int[]) original.clone();
    }

    /**
     * Copy a (possibly null) array of longs
     *
     * @param original The original array to copy
     *
     * @return a copy of the array
     */
    public  static  long[]   copy( long[] original )
    {
        return (original == null) ? null : (long[]) original.clone();
    }

    /**
     * Copy a (possibly null) 2-dimensional array of ints
     *
     * @param original The original array to copy
     *
     * @return a copy of the array
     */
    public  static  int[][]   copy2( int[][] original )
    {
        if ( original == null ) { return null; }

        int[][] result = new int[ original.length ][];
        for ( int i = 0; i < original.length; i++ )
        {
            result[ i ] = copy( original[ i ] );
        }
        
        return result;
    }

    /**
     * Make the contents of an array available as a read-only list. If the
     * array is null, an empty list will be returned.
     *
     *
     * @param <T> The type of the array cells
     *
     * @param array The array to turn into a read-only list
     *
     * @return a read-only list
     */
    @SafeVarargs
    public static <T> List<T> asReadOnlyList(T... array) {
        if (array == null) {
            return Collections.emptyList();
        } else {
            return Collections.unmodifiableList(Arrays.asList(array));
        }
    }

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

      @return the array of objects

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

    public static String toString(int[] value)
    {
        if (value == null || value.length == 0)
        {
            return "null";
        }
        else
        {
            StringBuffer ret_val = new StringBuffer();
            for (int i = 0; i < value.length; i++)
            {
                ret_val.append("[").append(value[i]).append("],");
            }
            return ret_val.toString();
        }
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
	  @exception java.lang.ClassNotFoundException On error
	  */
	public static String[] readStringArray(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
        String[] stringArray = null;

        int size = readArrayLength(in);
        if (size > 0) {
            stringArray = new String[size];
            readArrayItems(in, stringArray);
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

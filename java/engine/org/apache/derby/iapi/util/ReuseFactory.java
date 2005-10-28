/*

   Derby - Class com.ihost.cs.ReuseFactory

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

package org.apache.derby.iapi.util;

/**
	Factory methods for reusable objects. So far, the objects allocated
	by this factory are all immutable. Any immutable object can be re-used.

	All the methods in this class are static.
*/
public class ReuseFactory {

	/** Private constructor so no instances can be made */
	private ReuseFactory() {
	}

	private static final Integer[] staticInts =
		{new Integer(0), new Integer(1), new Integer(2), new Integer(3),
		 new Integer(4), new Integer(5), new Integer(6), new Integer(7),
		 new Integer(8), new Integer(9), new Integer(10), new Integer(11),
		 new Integer(12), new Integer(13), new Integer(14), new Integer(15),
		 new Integer(16), new Integer(17), new Integer(18)};
	private static final Integer FIFTY_TWO = new Integer(52);
	private static final Integer TWENTY_THREE = new Integer(23);
	private static final Integer MAXINT = new Integer(Integer.MAX_VALUE);
	private static final Integer MINUS_ONE = new Integer(-1);

	public static Integer getInteger(int i)
	{
		if (i >= 0 && i < staticInts.length)
		{
			return staticInts[i];
		}
		else
		{
			// Look for other common values
			switch (i)
			{
			  case 23:
				return TWENTY_THREE;	// precision of Int

			  case 52:
				return FIFTY_TWO;	// precision of Double

			  case Integer.MAX_VALUE:
				return MAXINT;

			  case -1:
				return MINUS_ONE;

			  default:
				return new Integer(i);
			}
		}
	}

	private static final Short[] staticShorts =
		{new Short((short) 0), new Short((short) 1), new Short((short) 2),
		 new Short((short) 3), new Short((short) 4), new Short((short) 5),
		 new Short((short) 6), new Short((short) 7), new Short((short) 8),
		 new Short((short) 9), new Short((short) 10)};

	public static Short getShort(short i)
	{
		if (i >= 0 && i < staticShorts.length)
			return staticShorts[i];
		else
			return new Short(i);
	}

	private static final Byte[] staticBytes =
		{new Byte((byte) 0), new Byte((byte) 1), new Byte((byte) 2),
		 new Byte((byte) 3), new Byte((byte) 4), new Byte((byte) 5),
		 new Byte((byte) 6), new Byte((byte) 7), new Byte((byte) 8),
		 new Byte((byte) 9), new Byte((byte) 10)};

	public static Byte getByte(byte i)
	{
		if (i >= 0 && i < staticBytes.length)
			return staticBytes[i];
		else
			return new Byte(i);
	}

	private static final Long[] staticLongs =
		{new Long(0), new Long(1), new Long(2),
		 new Long(3), new Long(4), new Long(5),
		 new Long(6), new Long(7), new Long(8),
		 new Long(9), new Long(10)};

	public static Long getLong(long i)
	{
		if (i >= 0 && i < staticLongs.length)
			return staticLongs[(int) i];
		else
			return new Long(i);
	}

    private static final Boolean staticFalse = new Boolean( false);
    private static final Boolean staticTrue = new Boolean( true);

    public static Boolean getBoolean( boolean b)
    {
        return b ? staticTrue : staticFalse;
    }
}

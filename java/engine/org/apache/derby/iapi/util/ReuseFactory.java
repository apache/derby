/*

   Derby - Class org.apache.derby.iapi.util.ReuseFactory

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

	public static Integer getInteger(int i)
	{
        return Integer.valueOf(i);
	}

	public static Short getShort(short i)
	{
        return Short.valueOf(i);
	}

	public static Byte getByte(byte i)
	{
        return Byte.valueOf(i);
	}

	public static Long getLong(long i)
	{
        return Long.valueOf(i);
	}

    public static Boolean getBoolean( boolean b)
    {
        return Boolean.valueOf(b);
    }

	private static final byte[] staticZeroLenByteArray = new byte[0];
	public static byte[] getZeroLenByteArray() 
	{
		return staticZeroLenByteArray;
	}
}

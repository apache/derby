/*

   Derby - Class org.apache.derby.iapi.tools.ToolUtils

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

package org.apache.derby.iapi.tools;

public  abstract    class   ToolUtils
{
	///////////////////////////////////////////////////////////////////
	//
	// Methods to copy arrays. We'd like to use java.util.copyOf(), but
    // we have to run on Java 5. The same methods also appear in
    // org.apache.derby.iapi.services.io.ArrayUtil. They are repeated here
    // in order to avoid sealing issues.
	//
	///////////////////////////////////////////////////////////////////

    /** Copy an array of objects; the original array could be null */
    public  static  Object[]    copy( Object[] original )
    {
        return (original == null) ? null : (Object[]) original.clone();
    }

    /** Copy a (possibly null) array of strings */
    public  static  String[]    copy( String[] original )
    {
        return (original == null) ? null : (String[]) original.clone();
    }

    /** Copy a (possibly null) array of booleans */
    public  static  boolean[]   copy( boolean[] original )
    {
        return (original == null) ? null : (boolean[]) original.clone();
    }

    /** Copy a (possibly null) array of bytes */
    public  static  byte[]   copy( byte[] original )
    {
        return (original == null) ? null : (byte[]) original.clone();
    }

    /** Copy a (possibly null) array of ints */
    public  static  int[]   copy( int[] original )
    {
        return (original == null) ? null : (int[]) original.clone();
    }

    /** Copy a (possibly null) 2-dimensional array of ints */
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


}

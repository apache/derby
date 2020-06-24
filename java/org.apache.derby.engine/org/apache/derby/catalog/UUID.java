/*

   Derby - Class org.apache.derby.catalog.UUID

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.catalog;

/**

//IC see: https://issues.apache.org/jira/browse/DERBY-2400
 An interface for accessing Derby UUIDs, unique identifiers.
		
	<p>The values in the
	system catalog held in ID columns with a type of CHAR(36) are the
	string representations of these UUIDs.

	<p>A UUID implements equals() and hashCode based on value equality.

 */

public interface UUID extends java.io.Externalizable
{
    /** NULL UUID */
    static  final   String  NULL = "NULL";

	/**
	  UUID_BYTE_LENGTH

	  The number of bytes in the array toByteArray returns.
	  */
	static int UUID_BYTE_LENGTH = 16;
	
	/**
		Produce a string representation of this UUID which
		is suitable for use as a unique ANSI identifier.

        @return an ANSI identifier
	 */
	String toANSIidentifier();

	/**
	  Clone this UUID.

	  @return	a copy of this UUID
	  */
	UUID cloneMe();
}


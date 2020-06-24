/*

   Derby - Class org.apache.derby.catalog.DependableFinder

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

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

/**
	
  A DependableFinder is an object that can find an in-memory
  Dependable, given the Dependable's ID.
  
  
  <P>
  The DependableFinder is able to write itself to disk and,
  once read back into memory, locate the in-memory Dependable that it
  represents.

  <P>
  DependableFinder objects are stored in SYS.SYSDEPENDS to record
  dependencies between database objects.
  */
public interface DependableFinder
{
	/**
	  *	Get the in-memory object associated with the passed-in object ID.
	  *
      * @param dd DataDictionary to use for lookup.
	  *	@param	dependableObjectID the ID of a Dependable. Used to locate that Dependable.
	  *
	  *	@return	the associated Dependable
	  * @exception StandardException		thrown if the object cannot be found or on error o
	  */
    public	Dependable	getDependable(DataDictionary dd,
            UUID dependableObjectID) throws StandardException;
//IC see: https://issues.apache.org/jira/browse/DERBY-2138

	/**
	  * The name of the class of Dependables as a "SQL Object" which this
	  * Finder can find.
	  * This is a value like "Table" or "View".
	  *	Every DependableFinder can find some class of Dependables. 
	  *
	  *
	  *	@return	String type of the "SQL Object" which this Finder can find.
	  * @see Dependable
	  */
	public	String	getSQLObjectType();
}

/*

   Derby - Class org.apache.derby.catalog.DependableFinder

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

package org.apache.derby.catalog;

import java.sql.SQLException;

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
	  *	@param	dependableObjectID the ID of a Dependable. Used to locate that Dependable.
	  *
	  *	@return	the associated Dependable
	  * @exception SQLException		thrown on error
	  */
	public	Dependable	getDependable(UUID dependableObjectID) throws SQLException;

	/**
	  *	Get the in-memory object associated with the passed-in object ID.
	  *
	  *	@param	dependableObjectID the UUID of the Dependable as a String.
	  * Used to locate that Dependable
	  *
	  *	@return	the associated Dependable
	  * @exception SQLException		thrown on error
	  */
	public	Dependable	getDependable(String dependableObjectID) throws SQLException;

	/**
	  * The name of the class of Dependables as a "SQL Object" which this
	  * Finder can find.
	  * This is a value like "Table", "View", or "Publication".
	  *	Every DependableFinder can find some class of Dependables. 
	  *
	  *
	  *	@return	String type of the "SQL Object" which this Finder can find.
	  * @see Dependable
	  */
	public	String	getSQLObjectType();

	/**
	  * Get the name of the SQL Object that corresponds to the specified 
	  * UUID String. For example, if getSQLObjectType() returns "Table", 
	  * this will return the table name.
	  *
	  *	@param	idString the UUID String of a Dependable. Used to locate that Dependable.
	  *
	  *	@return	String		Name of the associated Dependable
	  * @exception SQLException		thrown on error
	  */
	public	String	getSQLObjectName(String idString) throws SQLException;

}

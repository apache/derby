/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.UniqueSQLObjectDescriptor

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.catalog.UUID;

/**
 * This is a descriptor for something that is a 
 * SQL object that has the following properties:
 * <UL>
 *	<LI> resides in a schema </LI>
 *	<LI> has a name (that is unique when combined with schema) </LI>
 *	<LI> has a unique identifier (UUID) </LI>
 * </UL>
 *
 * UUIDS.
 *
 */
public abstract class UniqueSQLObjectDescriptor extends UniqueTupleDescriptor
{
    /** Pass-through constructors */
    public  UniqueSQLObjectDescriptor() { super(); }
    public  UniqueSQLObjectDescriptor( DataDictionary dd ) { super( dd ); }
    
	/**
	 * Get the name of this object.  E.g. for a table descriptor,
	 * this will be the table name.
	 * 
	 * @return the name
	 */
	public abstract String getName();

	/**
	 * Get the objects schema descriptor
	 *
	 * @return the schema descriptor
	 *
	 * @exception StandardException on error
	 */
	public abstract SchemaDescriptor getSchemaDescriptor()
		throws StandardException;
}

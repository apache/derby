/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.ConsInfo

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.iapi.error.StandardException;

/**
 *	This interface describes the columns in a referenced constraint. Added
 *	to be the protocol version of ConstraintInfo.
 *
 * @version 0.1
 * @author Rick Hillegas
 */

public interface ConsInfo	extends	Formatable
{
	/**
	  *	This ConsInfo describes columns in a referenced table. What is
	  *	the schema that the referenced table lives in?
	  *
	  *	@param	dd	data dictionary to search for schema
	  *
	  *	@return	schema that referenced table lives in
	  *	@exception	StandardException thrown on oops
	  */
	public SchemaDescriptor getReferencedTableSchemaDescriptor(DataDictionary dd)
		throws StandardException;

	/**
	  *	This ConsInfo describes columns in a referenced table. What is
	  *	that table?
	  *
	  *	@param	dd	data dictionary to search for table
	  *
	  *	@return	referenced table
	  *	@exception	StandardException thrown on oops
	  */
	public TableDescriptor getReferencedTableDescriptor(DataDictionary dd)
		throws StandardException;

	/**
	  *	This ConsInfo describes columns in a referenced table. What are
	  *	their names?
	  *
	  *	@return	array of referenced column names
	  */
	public String[] getReferencedColumnNames();

	/**
	  *	Get the name of the table that these column live in.
	  *
	  *	@return	referenced table name
	  */
	public String getReferencedTableName();


	/**
	  *	Get the referential Action for an Update.
	  *
	  *	@return	referential Action for update
	  */

	public int getReferentialActionUpdateRule();
	
	/**
	  *	Get the referential Action for a Delete.
	  *
	  *	@return	referential Action Delete rule
	  */
	public int getReferentialActionDeleteRule();

}




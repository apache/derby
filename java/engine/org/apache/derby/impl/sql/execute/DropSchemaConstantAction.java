/*

   Derby - Class org.apache.derby.impl.sql.execute.DropSchemaConstantAction

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.catalog.UUID;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	DROP SCHEMA Statement at Execution time.
 *
 *	@author jamie
 */

class DropSchemaConstantAction extends DDLConstantAction
{


	private final String				schemaName;


	// CONSTRUCTORS

	/**
	 *	Make the ConstantAction for a DROP TABLE statement.
	 *
	 *	@param	schemaName			Table name.
	 *
	 */
	DropSchemaConstantAction(String	schemaName)
	{
		this.schemaName = schemaName;
	}

	///////////////////////////////////////////////
	//
	// OBJECT SHADOWS
	//
	///////////////////////////////////////////////

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return "DROP SCHEMA " + schemaName;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for DROP TABLE.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		SchemaDescriptor	sd;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

		/*
		** Inform the data dictionary that we are about to write to it.
		** There are several calls to data dictionary "get" methods here
		** that might be done in "read" mode in the data dictionary, but
		** it seemed safer to do this whole operation in "write" mode.
		**
		** We tell the data dictionary we're done writing at the end of
		** the transaction.
		*/
		dd.startWriting(lcc);

		sd = dd.getSchemaDescriptor(schemaName, null, true);

    //If user is attempting to drop SESSION schema and there is no physical SESSION schema, then throw an exception
    //Need to handle it this special way is because SESSION schema is also used for temporary tables. If there is no
    //physical SESSION schema, we internally generate an in-memory SESSION schema in order to support temporary tables
    //But there is no way for the user to access that in-memory SESSION schema. Following if will be true if there is
    //no physical SESSION schema and hence getSchemaDescriptor has returned an in-memory SESSION schema
    if (schemaName.equals(SchemaDescriptor.STD_DECLARED_GLOBAL_TEMPORARY_TABLES_SCHEMA_NAME) && (sd != null) && (sd.getUUID() == null))
			throw StandardException.newException(SQLState.LANG_SCHEMA_DOES_NOT_EXIST, schemaName);

		/*
		** Make sure the schema is empty.
		** In the future we want to drop everything
		** in the schema if it is CASCADE.
		*/
		if (!dd.isSchemaEmpty(sd))
		{
			throw StandardException.newException(SQLState.LANG_SCHEMA_NOT_EMPTY, schemaName);
		} 

		/* Prepare all dependents to invalidate.  (This is there chance
		 * to say that they can't be invalidated.  For example, an open
		 * cursor referencing a table/view that the user is attempting to
		 * drop.) If no one objects, then invalidate any dependent objects.
		 * We check for invalidation before we drop the table descriptor
		 * since the table descriptor may be looked up as part of
		 * decoding tuples in SYSDEPENDS.
		 */
		dm.invalidateFor(sd, DependencyManager.DROP_SCHEMA, lcc);

		dd.dropSchemaDescriptor(schemaName, tc);

		/*
		** If we have dropped the current default schema,
		** then we will set the default to null.  The
		** LCC is free to set the new default schema to 
	 	** some system defined default.
		*/
		sd = lcc.getDefaultSchema();
		if ((sd != null) &&
			schemaName.equals(sd.getSchemaName()))
		{
			lcc.setDefaultSchema((SchemaDescriptor)null);
		}

	}

}

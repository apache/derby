/*

   Derby - Class org.apache.derby.impl.sql.execute.DropSchemaConstantAction

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.store.access.TransactionController;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	DROP SCHEMA Statement at Execution time.
 *
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
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		TransactionController tc = lcc.getTransactionExecute();
//IC see: https://issues.apache.org/jira/browse/DERBY-4822

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

        SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, true);
//IC see: https://issues.apache.org/jira/browse/DERBY-4822

        sd.drop(lcc, activation);
//IC see: https://issues.apache.org/jira/browse/DERBY-3327
//IC see: https://issues.apache.org/jira/browse/DERBY-1331

	}

}

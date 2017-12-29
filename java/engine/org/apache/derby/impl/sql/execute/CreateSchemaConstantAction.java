/*

   Derby - Class org.apache.derby.impl.sql.execute.CreateSchemaConstantAction

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

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.catalog.UUID;

/**
 *	This class describes actions that are ALWAYS performed for a
 *	CREATE SCHEMA Statement at Execution time.
 *
 */

class CreateSchemaConstantAction extends DDLConstantAction
{

	private final String					aid;	// authorization id
	private final String					schemaName;
	

	// CONSTRUCTORS

	/**
	 * Make the ConstantAction for a CREATE SCHEMA statement.
	 * When executed, will set the default schema to the
	 * new schema if the setToDefault parameter is set to
	 * true.
	 *
	 *  @param schemaName	Name of table.
	 *  @param aid			Authorizaton id
	 */
	CreateSchemaConstantAction(
								String			schemaName,
								String			aid)
	{
		this.schemaName = schemaName;
		this.aid = aid;
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
		return "CREATE SCHEMA " + schemaName;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for CREATE SCHEMA.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		TransactionController tc = activation.
			getLanguageConnectionContext().getTransactionExecute();

		executeConstantActionMinion(activation, tc);
	}

	/**
	 *	This is the guts of the Execution-time logic for CREATE SCHEMA.
	 *  This is variant is used when we to pass in a tc other than the default
	 *  used in executeConstantAction(Activation).
	 *
	 * @param activation current activation
	 * @param tc transaction controller
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction(Activation activation,
									  TransactionController tc)
			throws StandardException {

		executeConstantActionMinion(activation, tc);
	}

	private void executeConstantActionMinion(Activation activation,
											 TransactionController tc)
			throws StandardException {

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, lcc.getTransactionExecute(), false);

		//if the schema descriptor is an in-memory schema, we donot throw schema already exists exception for it.
		//This is to handle in-memory SESSION schema for temp tables
		if ((sd != null) && (sd.getUUID() != null))
		{
			throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS, "Schema" , schemaName);
		}

		UUID tmpSchemaId = dd.getUUIDFactory().createUUID();

		/*
		** AID defaults to connection authorization if not 
		** specified in CREATE SCHEMA (if we had module
	 	** authorizations, that would be the first check
		** for default, then session aid).
		*/
		String thisAid = aid;
		if (thisAid == null)
		{
            thisAid = lcc.getCurrentUserId(activation);
		}

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

		sd = ddg.newSchemaDescriptor(schemaName,
									thisAid,
									tmpSchemaId);

		dd.addDescriptor(sd, null, DataDictionary.SYSSCHEMAS_CATALOG_NUM, false, tc);
	}
}

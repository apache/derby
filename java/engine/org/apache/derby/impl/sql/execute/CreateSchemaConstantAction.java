/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.catalog.UUID;

/**
 *	This class describes actions that are ALWAYS performed for a
 *	CREATE SCHEMA Statement at Execution time.
 *
 *	@author jamie 
 */

class CreateSchemaConstantAction extends DDLConstantAction
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

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
	 *  @param schemaId		ID of table. If null, we allocate one.
	 *	@param setToDefault	if true, set the default schema to
	 *			the new schema once it is created.
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
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		TransactionController tc = lcc.getTransactionExecute();
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
			thisAid = lcc.getAuthorizationId();
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

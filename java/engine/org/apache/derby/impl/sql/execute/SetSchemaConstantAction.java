/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;


import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.StatementType;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.reference.SQLState;

/**
 *	This class describes actions that are ALWAYS performed for a
 *	SET SCHEMA Statement at Execution time.
 *
 *	@author jamie 
 */

class SetSchemaConstantAction extends GenericConstantAction
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	private final String					schemaName;
	private final int						type;	
	
	// CONSTRUCTORS

	/**
	 * Make the ConstantAction for a SET SCHEMA statement.
	 *
	 *  @param schemaName	Name of schema.
	 *  @param type		type of set schema (e.g. SET_SCHEMA_DYNAMIC, SET_SCHEMA_USER)
	 */
	SetSchemaConstantAction(String schemaName, int type)
	{
		this.schemaName = schemaName;
		this.type = type;
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
		// if the error happens after we have figured out the schema name for
		// dynamic we want to use it rather than ?
		return "SET SCHEMA " + ((type == StatementType.SET_SCHEMA_USER) ? "USER" : 
				((type == StatementType.SET_SCHEMA_DYNAMIC && schemaName == null) ? "?" : schemaName));
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
		LanguageConnectionContext 	lcc;
		DataDictionary 				dd;

		// find the language context.
		lcc = activation.getLanguageConnectionContext();

		dd = lcc.getDataDictionary();
		String thisSchemaName = schemaName;
		if (type == StatementType.SET_SCHEMA_DYNAMIC)
		{
			ParameterValueSet pvs = activation.getParameterValueSet();
			DataValueDescriptor dvs = pvs.getParameter(0);
			thisSchemaName = dvs.getString();
			//null parameter is not allowed
			if (thisSchemaName == null || thisSchemaName.length() > 128)
				throw StandardException.newException(SQLState.LANG_DB2_REPLACEMENT_ERROR, "CURRENT SCHEMA");
		}
		else if (type == StatementType.SET_SCHEMA_USER)
		{
			thisSchemaName = lcc.getAuthorizationId();
		}
		// if schemaName is null, sd will be null and default schema will be used
		SchemaDescriptor sd = dd.getSchemaDescriptor(thisSchemaName, null, true);
		lcc.setDefaultSchema(sd);
	}
}

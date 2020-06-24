/*

   Derby - Class org.apache.derby.impl.sql.execute.SetSchemaConstantAction

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

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.StatementType;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.shared.common.reference.Limits;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.shared.common.reference.SQLState;

/**
 *	This class describes actions that are ALWAYS performed for a
 *	SET SCHEMA Statement at Execution time.
 *
 */

class SetSchemaConstantAction implements ConstantAction
{

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
	 *	This is the guts of the Execution-time logic for SET SCHEMA.
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
//IC see: https://issues.apache.org/jira/browse/DERBY-104
			if (thisSchemaName == null || thisSchemaName.length() > Limits.MAX_IDENTIFIER_LENGTH)
				throw StandardException.newException(SQLState.LANG_DB2_REPLACEMENT_ERROR, "CURRENT SCHEMA");
		}
		else if (type == StatementType.SET_SCHEMA_USER)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-4551
            thisSchemaName = lcc.getCurrentUserId(activation);
		}

                SchemaDescriptor sd = dd.getSchemaDescriptor(thisSchemaName,
                        lcc.getTransactionExecute(), true);
//IC see: https://issues.apache.org/jira/browse/DERBY-3327
//IC see: https://issues.apache.org/jira/browse/DERBY-1331
		lcc.setDefaultSchema(activation, sd);
	}
}

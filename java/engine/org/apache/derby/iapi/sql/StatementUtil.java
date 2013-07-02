/*

   Derby - Class org.apache.derby.iapi.sql.StatementUtil

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

package org.apache.derby.iapi.sql;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

/**
 * Utilities for dealing with statements.
 *
 */
public class StatementUtil
{
	private StatementUtil(){};	// Do not instantiate

	public static String typeName(int typeNumber)
	{
		String retval;

		switch (typeNumber)
		{
		  case StatementType.INSERT:
		  case StatementType.BULK_INSERT_REPLACE:
		  case StatementType.UPDATE:
		  case StatementType.DELETE:
		  case StatementType.ENABLED:
		  case StatementType.DISABLED:
			retval = TypeNames[typeNumber];
			break;

		  default:
			retval = "UNKNOWN";
			break;
		}

		return retval;
	}

	private static final String[] TypeNames = 
				{ 
					"",
					"INSERT",
					"INSERT",
					"UPDATE",
					"DELETE",
					"ENABLED",
					"DISABLED"
				};

    /**
     * Get the descriptor for the named schema. If the schemaName
     * parameter is NULL, it gets the descriptor for the current
     * compilation schema.
     * 
     * @param schemaName The name of the schema we're interested in.
     * If the name is NULL, get the descriptor for the current compilation schema.
     * @param raiseError True to raise an error if the schema does not exist,
     * false to return null if the schema does not exist.
     * @return Valid SchemaDescriptor or null if raiseError is false and the
     * schema does not exist. 
     * @throws StandardException Schema does not exist and raiseError is true.
     */
	public static SchemaDescriptor	getSchemaDescriptor
        (
         String schemaName,
         boolean raiseError,
         DataDictionary dataDictionary,
         LanguageConnectionContext lcc,
         CompilerContext cc
         )
		throws StandardException
	{
		/*
		** Check for a compilation context.  Sometimes
		** there is a special compilation context in
	 	** place to recompile something that may have
		** been compiled against a different schema than
		** the current schema (e.g views):
	 	**
	 	** 	CREATE SCHEMA x
	 	** 	CREATE TABLE t
		** 	CREATE VIEW vt as SEELCT * FROM t
		** 	SET SCHEMA app
		** 	SELECT * FROM X.vt 
		**
		** In the above view vt must be compiled against
		** the X schema.
		*/


		SchemaDescriptor sd = null;
		boolean isCurrent = false;
		boolean isCompilation = false;
		if (schemaName == null) {

			sd = cc.getCompilationSchema();

			if (sd == null) {
				// Set the compilation schema to be the default,
				// notes that this query has schema dependencies.
				sd = lcc.getDefaultSchema();

				isCurrent = true;

				cc.setCompilationSchema(sd);
			}
			else
			{
				isCompilation = true;
			}
			schemaName = sd.getSchemaName();
		}

		SchemaDescriptor sdCatalog = dataDictionary.getSchemaDescriptor(schemaName,
			lcc.getTransactionCompile(), raiseError);

		if (isCurrent || isCompilation) {
			//if we are dealing with a SESSION schema and it is not physically
			//created yet, then it's uuid is going to be null. DERBY-1706
			//Without the getUUID null check below, following will give NPE
			//set schema session; -- session schema has not been created yet
			//create table t1(c11 int);
			if (sdCatalog != null && sdCatalog.getUUID() != null)
			{
				// different UUID for default (current) schema than in catalog,
				// so reset default schema.
				if (!sdCatalog.getUUID().equals(sd.getUUID()))
				{
					if (isCurrent) { lcc.setDefaultSchema(sdCatalog); }
					cc.setCompilationSchema(sdCatalog);
				}
			}
			else
			{
				// this schema does not exist, so ensure its UUID is null.
				sd.setUUID(null);
				sdCatalog = sd;
			}
		}
		return sdCatalog;
	}

}

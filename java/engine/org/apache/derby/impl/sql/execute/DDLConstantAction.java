/*

   Derby - Class org.apache.derby.impl.sql.execute.DDLConstantAction

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

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.iapi.sql.depend.Dependency;
import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.depend.DependencyManager;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.catalog.UUID;

/**
 * Abstract class that has actions that are across
 * all DDL actions.
 *
 * @author jamie
 */
public abstract class DDLConstantAction extends GenericConstantAction
{
	//TransactionController 		tc;
	//protected LanguageConnectionContext 	lcc;
	//DataDescriptorGenerator 	ddg;
	//DataDictionary 				dd;
	//DependencyManager			dm;

	/**
	 * Set up the "environment variables" for this
	 * constant action.
	 */
	//protected void setEnvironmentVariables(Activation activation)
	//{
		/* find the language context.
		 * NOTE: The activation could be null if
		 * we are creating the SPSs for the metadata
		 * queries in the background, so we get
		 * the lcc from the ContextService.
		 */
		//lcc = (activation == null) ?
		//		(LanguageConnectionContext)
		//			ContextService.getContext(LanguageConnectionContext.CONTEXT_ID):
		//		activation.getLanguageConnectionContext();


        // Get the current transaction controller
        //tc = lcc.getTransactionExecute();

		//dd = lcc.getDataDictionary();
		//dm = dd.getDependencyManager();
		//ddg = dd.getDataDescriptorGenerator();
//	}

	/**
	 * Get the schema descriptor for the schemaid.
	 *
	 * @param dd the data dictionary
	 * @param schemaId the schema id
	 * @param statementType string describing type of statement for error
	 *	reporting.  e.g. "ALTER STATEMENT"
	 *
	 * @return the schema descriptor
	 *
	 * @exception StandardException if schema is system schema
	 */
	static SchemaDescriptor getAndCheckSchemaDescriptor(
						DataDictionary		dd,
						UUID				schemaId,
						String				statementType)
		throws StandardException
	{
		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaId, null);
		return sd;
	}

	/**
	 * Get the schema descriptor in the creation of an object in
	   the passed in schema.
	 *
	 * @param dd the data dictionary
	   @param activation activation
	   @param schemaName name of the schema
	 *
	 * @return the schema descriptor
	 *
	 * @exception StandardException if the schema does not exist
	 */
	static SchemaDescriptor getSchemaDescriptorForCreate(
						DataDictionary		dd,
						Activation activation,
						String schemaName)
		throws StandardException
	{
		TransactionController tc = activation.getLanguageConnectionContext().getTransactionExecute();
		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, false);

		if (sd == null || sd.getUUID() == null) {
			ConstantAction csca = new CreateSchemaConstantAction(schemaName, (String) null);
			csca.executeConstantAction(activation);

			sd = dd.getSchemaDescriptor(schemaName, tc, true);
		}

		return sd;
	}

	/**
	 * Lock the table in exclusive or share mode to prevent deadlocks.
	 *
	 * @param tc						The TransactionController
	 * @param heapConglomerateNumber	The conglomerate number for the heap.
	 * @param exclusiveMode				Whether or not to lock the table in exclusive mode.
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException if schema is system schema
	 */
	final void lockTableForDDL(TransactionController tc,
						 long heapConglomerateNumber, boolean exclusiveMode)
		throws StandardException
	{
		ConglomerateController cc;

		cc = tc.openConglomerate(
					heapConglomerateNumber,
                    false,
					(exclusiveMode) ?
						(TransactionController.OPENMODE_FORUPDATE | 
							TransactionController.OPENMODE_FOR_LOCK_ONLY) :
						TransactionController.OPENMODE_FOR_LOCK_ONLY,
			        TransactionController.MODE_TABLE,
                    TransactionController.ISOLATION_SERIALIZABLE);
		cc.close();
	}

	/**
	 * Does this constant action modify the passed in table
	 * uuid?  By modify we mean add or drop things tied to
	 * this table (e.g. index, trigger, constraint).  Things
	 * like views or spses that reference this table don't
	 * count.
	 *
	 * @param tableId the table id
 	 *
	 * @exception StandardException on error
	 */
	public boolean modifiesTableId(UUID tableId) throws StandardException
	{
		// by default, assume we don't modify it
		return false;
	}


	protected String constructToString(
						String				statementType,
						String              objectName)
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.

		return statementType + objectName;
	}
}


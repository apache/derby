/*

   Derby - Class org.apache.derby.impl.sql.execute.CreateSPSConstantAction

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

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;

import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.catalog.UUID;

/**
 * This class  describes actions that are ALWAYS performed for a
 * CREATE STATEMENT Statement at Execution time.  There is a
 * little special logic to allow users to control whether
 * it is ok to create a statement in the SYS schema or not	
 *
 *	@author Jamie
 */
class CreateSPSConstantAction extends GenericSPSConstantAction
{

	private String					schemaName;
	private String					spsName;
	private UUID					schemaId;
	private UUID					compSchemaId;
	private String					spsText;
	private String					usingText;
	private boolean					okInSys;
	private boolean					nocompile;


	// CONSTRUCTORS

	/**
	 * Make the ConstantAction for a CREATE STORED PREPARED STATEMENT statement.
	 * Adds an extra parameter that allows the user to designate whether
	 * this sps can be created in the SYS schema.
	 *
	 *  @param sd			descriptor for the schema that table lives in.
	 *  @param sysID		ID of table. If null, we allocate one on execute.
	 *  @param spsName		Name of statement
	 *	@param spsText		Text of query expression for sps definition
	 *	@param usingText	the text of the USING clause
	 *	@param compSchemaId	The schema this is to be compiled against
	 *	@param okInSys		ok to create in sys schema
	 *	@param nocompile	don't compile it	
	 */
	CreateSPSConstantAction(
								String				schemaName,
								String				spsName,
								String				spsText,
								String				usingText,
								UUID				compSchemaId,
								boolean				okInSys,
								boolean				nocompile)
	{
		this.schemaName = schemaName;
		this.spsName = spsName;
		this.spsText = spsText;
		this.usingText = usingText;
		this.okInSys = okInSys;
		this.nocompile = nocompile;
		this.compSchemaId = compSchemaId;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(schemaName != null, "Schema name is null");
			// SanityManager.ASSERT(compSchemaId != null, "compSchemaId is null");
		}
	}

	///////////////////////////////////////////////
	//
	// OBJECT SHADOWS
	//
	///////////////////////////////////////////////

	public	String	toString()
	{
		return constructToString("CREATE STATEMENT ", spsName);
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for CREATE STATEMENT.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		SPSDescriptor 				spsd; 
		SchemaDescriptor			schemaDescriptor;
		Object[]					paramDefaults = null;

		LanguageConnectionContext lcc;
		if (activation != null)
		{
			lcc = activation.getLanguageConnectionContext();
		}
		else // only for direct executions by the database meta data
		{
			lcc = (LanguageConnectionContext) ContextService.getContext
          				(LanguageConnectionContext.CONTEXT_ID);
		}
		TransactionController tc= lcc.getTransactionExecute();
		DataDictionary dd = lcc.getDataDictionary();

		SchemaDescriptor sd;
		if (okInSys)
		{
			sd = dd.getSchemaDescriptor(schemaName, tc, true);
		}
		else
		{
			sd = getSchemaDescriptorForCreate(dd, activation, schemaName);
		}

		if (usingText != null)
		{
			paramDefaults = getUsingResults(usingText);
		}

		/*	
		** Create a new SPS descriptor
		*/ 
		spsd = new SPSDescriptor(dd, spsName,
									dd.getUUIDFactory().createUUID(),
									sd.getUUID(),
									compSchemaId == null ?
										lcc.getDefaultSchema().getUUID() :
										compSchemaId,
									SPSDescriptor.SPS_TYPE_REGULAR,
									!nocompile,		// it is valid, unless nocompile
									spsText,
									usingText,
									paramDefaults,
									!nocompile );

		if (!nocompile)
		{
			/*
			** Prepared the stored prepared statement
			** and release the activation class -- we
			** know we aren't going to execute statement
			** after create it, so for now we are finished.
			*/
			spsd.prepareAndRelease(lcc);
		}		


		/*
		** Indicate that we are about to modify the data dictionary.
		** 
		** We tell the data dictionary we're done writing at the end of
		** the transaction.
		*/
		dd.startWriting(lcc);
		if (activation == null)
		{
			TransactionController subtc = null;
			try
			{
				subtc = tc.startNestedUserTransaction(false); //metadata call, nested tran
			} catch (StandardException se) {};
			if (subtc != null)
			{
				try
				{
					dd.addSPSDescriptor(spsd, subtc, false);
					subtc.commit();
					subtc.destroy();
					return;
				} catch (StandardException se)
				{
					subtc.abort();
					subtc.destroy();
					if (! se.getMessageId().equals(SQLState.LOCK_TIMEOUT))
						throw se;
				}
			}
		}

		dd.addSPSDescriptor(spsd, tc, true);
	}


}

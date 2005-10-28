/*

   Derby - Class org.apache.derby.impl.sql.execute.CreateConstraintConstantAction

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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


import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.DDUtils;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.depend.ProviderInfo;

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.services.loader.ClassFactory;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	constraint creation at Execution time.
 *
 *	@version 0.1
 *	@author Jerry Brenner
 */

public class CreateConstraintConstantAction extends ConstraintConstantAction
{
	public	String[]		columnNames;
	public	String			constraintText;

	private ConstraintInfo	otherConstraintInfo;
	private	ClassFactory	cf;

	/*
	** Is this constraint to be created as enabled or not.
	** The only way to create a disabled constraint is by
	** publishing a disabled constraint.
	*/
	private boolean			enabled;

	private ProviderInfo[] providerInfo;

	// CONSTRUCTORS

	/**
	 *	Make one of these puppies.
	 *
	 *  @param constraintName	Constraint name.
	 *  @param constraintType	Constraint type.
	 *  @param tableName		Table name.
	 *	@param tableId			UUID of table.
	 *  @param schemaName		the schema that table and constraint lives in.
	 *  @param columnNames		String[] for column names
	 *  @param indexAction		IndexConstantAction for constraint (if necessary)
	 *  @param constraintText	Text for check constraint
	 *  RESOLVE - the next parameter should go away once we use UUIDs
	 *			  (Generated constraint names will be based off of uuids)
	 *	@param enabled			Should the constraint be created as enabled 
	 *							(enabled == true), or disabled (enabled == false).
	 *	@param otherConstraint 	information about the constraint that this references
	 *  @param providerInfo Information on all the Providers
	 */
	CreateConstraintConstantAction(
		               String	constraintName,
					   int		constraintType,
		               String	tableName,
					   UUID		tableId,
					   String	schemaName,
					   String[]	columnNames,
					   IndexConstantAction indexAction,
					   String	constraintText,
					   boolean	enabled,
				       ConstraintInfo	otherConstraint,
					   ProviderInfo[] providerInfo)
	{
		super(constraintName, constraintType, tableName, 
			  tableId, schemaName, indexAction);
		this.columnNames = columnNames;
		this.constraintText = constraintText;
		this.enabled = enabled;
		this.otherConstraintInfo = otherConstraint;
		this.providerInfo = providerInfo;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for CREATE CONSTRAINT.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		boolean						forCreateTable;
		ConglomerateDescriptor		conglomDesc = null;
		ConglomerateDescriptor[]	conglomDescs = null;
		ConstraintDescriptor		conDesc = null;
		TableDescriptor				td = null;
		UUID						indexId = null;
		String						uniqueName;
		String						backingIndexName;

		/* RESOLVE - blow off not null constraints for now (and probably for ever) */
		if (constraintType == DataDictionary.NOTNULL_CONSTRAINT)
		{
			return;
		}

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

		cf = lcc.getLanguageConnectionFactory().getClassFactory();

		/* Remember whether or not we are doing a create table */
		forCreateTable = activation.getForCreateTable();

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

		/* Table gets locked in AlterTableConstantAction */

		/*
		** If the schema descriptor is null, then
		** we must have just read ourselves in.  
		** So we will get the corresponding schema
		** descriptor from the data dictionary.
		*/

		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, true);
		
		/* Try to get the TableDescriptor from
		 * the Activation. We will go to the
		 * DD if not there. (It should always be
		 * there except when in a target.)
		 */
		td = activation.getDDLTableDescriptor();

		if (td == null)
		{
			/* tableId will be non-null if adding a
			 * constraint to an existing table.
			 */
			if (tableId != null)
			{
				td = dd.getTableDescriptor(tableId);
			}
			else
			{
				td = dd.getTableDescriptor(tableName, sd);
			}

			if (td == null)
			{
				throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
			}
			activation.setDDLTableDescriptor(td);
		}

		/* Generate the UUID for the backing index.  This will become the
		 * constraint's name, if no name was specified.
		 */
		UUIDFactory uuidFactory = dd.getUUIDFactory();

		/* Create the index, if there's one for this constraint */
		if (indexAction != null)
		{
			if ( indexAction.getIndexName() == null )
			{
				/* Set the index name */
				backingIndexName =  uuidFactory.createUUID().toString();
				indexAction.setIndexName(backingIndexName);
			}
			else { backingIndexName = indexAction.getIndexName(); }

			/* Create the index */
			indexAction.executeConstantAction(activation);

			/* Get the conglomerate descriptor for the backing index */
			conglomDescs = td.getConglomerateDescriptors();

			for (int index = 0; index < conglomDescs.length; index++)
			{
				conglomDesc = conglomDescs[index];

				/* Check for conglomerate being an index first, since
				 * name is null for heap.
				 */
				if (conglomDesc.isIndex() && 
					backingIndexName.equals(conglomDesc.getConglomerateName()))
				{
					break;
				}
			}

			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(conglomDesc != null,
					"conglomDesc is expected to be non-null after search for backing index");
				SanityManager.ASSERT(conglomDesc.isIndex(),
					"conglomDesc is expected to be indexable after search for backing index");
				SanityManager.ASSERT(conglomDesc.getConglomerateName().equals(backingIndexName),
			   "conglomDesc name expected to be the same as backing index name after search for backing index");
			}

			indexId = conglomDesc.getUUID();
		}

		// if no constraintId was specified, we should generate one. this handles
		// the two cases of Source creation and Target replication. At the source
		// database, we allocate a new UUID. At the Target, we just use the UUID that
		// the Source sent along.
		UUID constraintId = uuidFactory.createUUID();

		/* Now, lets create the constraint descriptor */
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
		switch (constraintType)
		{
			case DataDictionary.PRIMARYKEY_CONSTRAINT:
				conDesc = ddg.newPrimaryKeyConstraintDescriptor(
								td, constraintName,
								false, //deferable,
								false, //initiallyDeferred,
								genColumnPositions(td, false), //int[],
								constraintId, 
								indexId, 
								sd,
								enabled,
								0				// referenceCount
								);
				dd.addConstraintDescriptor(conDesc, tc);
				break;

			case DataDictionary.UNIQUE_CONSTRAINT:
				conDesc = ddg.newUniqueConstraintDescriptor(
								td, constraintName,
								false, //deferable,
								false, //initiallyDeferred,
								genColumnPositions(td, false), //int[],
								constraintId, 
								indexId, 
								sd,
								enabled,
								0				// referenceCount
								);
				dd.addConstraintDescriptor(conDesc, tc);
				break;

			case DataDictionary.CHECK_CONSTRAINT:
				conDesc = ddg.newCheckConstraintDescriptor(
								td, constraintName,
								false, //deferable,
								false, //initiallyDeferred,
								constraintId, 
								constraintText, 
								new ReferencedColumnsDescriptorImpl(genColumnPositions(td, false)), //int[],
								sd,
								enabled
								);
				dd.addConstraintDescriptor(conDesc, tc);
				break;

			case DataDictionary.FOREIGNKEY_CONSTRAINT:
				ReferencedKeyConstraintDescriptor referencedConstraint = DDUtils.locateReferencedConstraint
					( dd, td, constraintName, columnNames, otherConstraintInfo );
				DDUtils.validateReferentialActions(dd, td, constraintName, otherConstraintInfo,columnNames);
				
				conDesc = ddg.newForeignKeyConstraintDescriptor(
								td, constraintName,
								false, //deferable,
								false, //initiallyDeferred,
								genColumnPositions(td, false), //int[],
								constraintId,
								indexId,
								sd,
								referencedConstraint,
								enabled,
								otherConstraintInfo.getReferentialActionDeleteRule(),
								otherConstraintInfo.getReferentialActionUpdateRule()
								);

				// try to create the constraint first, because it
				// is expensive to do the bulk check, find obvious
				// errors first
				dd.addConstraintDescriptor(conDesc, tc);

				/* No need to do check if we're creating a 
				 * table.
				 */
				if ( (! forCreateTable) && 
					 dd.activeConstraint( conDesc ) )
				{
					validateFKConstraint(tc, 
										 dd, 
										 (ForeignKeyConstraintDescriptor)conDesc, 
										 referencedConstraint,
										 ((CreateIndexConstantAction)indexAction).getIndexTemplateRow());
				}
				
				/* Create stored dependency on the referenced constraint */
				dm.addDependency(conDesc, referencedConstraint, lcc.getContextManager());
				break;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("contraintType (" + constraintType + 
						") has unexpected value");
				}
				break;
		}

		/* Create stored dependencies for each provider */
		if (providerInfo != null)
		{
			for (int ix = 0; ix < providerInfo.length; ix++)
			{
				Provider provider = null;
	
				/* We should always be able to find the Provider */
				try 
				{
					provider = (Provider) providerInfo[ix].
											getDependableFinder().
												getDependable(
													providerInfo[ix].getObjectId());
				}
				catch(java.sql.SQLException te)
				{
					if (SanityManager.DEBUG)
					{
						SanityManager.THROWASSERT("unexpected java.sql.SQLException - " + te);
					}
				}
				dm.addDependency(conDesc, provider, lcc.getContextManager());
			}
		}

		/* Finally, invalidate off of the table descriptor(s)
		 * to ensure that any dependent statements get
		 * re-compiled.
		 */
		if (! forCreateTable)
		{
			dm.invalidateFor(td, DependencyManager.CREATE_CONSTRAINT, lcc);
		}
		if (constraintType == DataDictionary.FOREIGNKEY_CONSTRAINT)
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(conDesc != null,
					"conDesc expected to be non-null");

				if (! (conDesc instanceof ForeignKeyConstraintDescriptor))
				{
					SanityManager.THROWASSERT(
						"conDesc expected to be instance of ForeignKeyConstraintDescriptor, not " +
						conDesc.getClass().getName());
				}
			}
			dm.invalidateFor(
				((ForeignKeyConstraintDescriptor)conDesc).
					getReferencedConstraint().
						getTableDescriptor(),
				DependencyManager.CREATE_CONSTRAINT, lcc);
		}
	}

	/**
	 * Is the constant action for a foreign key
	 *
	 * @return true/false
	 */
	public boolean isForeignKeyConstraint()
	{ 
		return (constraintType == DataDictionary.FOREIGNKEY_CONSTRAINT);
	}

	/**
	 * Generate an array of column positions for the column list in
	 * the constraint.
	 *
	 * @param td	The TableDescriptor for the table in question
	 * @param columnsMustBeOrderable	true for primaryKey and unique constraints
	 *
	 * @return int[]	The column positions.
	 */
	private int[] genColumnPositions(TableDescriptor td,
									 boolean columnsMustBeOrderable)
		throws StandardException
	{
		int[] baseColumnPositions;

		// Translate the base column names to column positions
		baseColumnPositions = new int[columnNames.length];
		for (int i = 0; i < columnNames.length; i++)
		{
			ColumnDescriptor columnDescriptor;

			// Look up the column in the data dictionary
			columnDescriptor = td.getColumnDescriptor(columnNames[i]);
			if (columnDescriptor == null)
			{
				throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, 
															columnNames[i],
															tableName);
			}

			// Don't allow a column to be created on a non-orderable type
			// (for primaryKey and unique constraints)
			if ( columnsMustBeOrderable &&
				 ( ! columnDescriptor.getType().getTypeId().orderable(
															cf))
			   )
			{
				throw StandardException.newException(SQLState.LANG_COLUMN_NOT_ORDERABLE_DURING_EXECUTION, 
					columnDescriptor.getType().getTypeId().getSQLTypeName());
			}

			// Remember the position in the base table of each column
			baseColumnPositions[i] = columnDescriptor.getPosition();
		}

		return baseColumnPositions;
	}
	///////////////////////////////////////////////////////////////////////
	//
	//	ACCESSORS
	//
	///////////////////////////////////////////////////////////////////////

	/**
	  *	Get the names of the columns touched by this constraint.
	  *
	  *	@return	the array of touched column names.
	  */
    public	String[]	getColumnNames() { return columnNames; }


	/**
	  *	Get the text defining this constraint.
	  *
	  *	@return	constraint text
	  */
    public	String	getConstraintText() { return constraintText; }

	public String toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		StringBuffer strbuf = new StringBuffer();
		strbuf.append( "CREATE CONSTRAINT " + constraintName );
		strbuf.append("\n=========================\n");

		if (columnNames == null)
		{
			strbuf.append("columnNames == null\n");
		}
		else
		{
			for (int ix=0; ix < columnNames.length; ix++)
			{
				strbuf.append("\n\tcol["+ix+"]"+columnNames[ix].toString());
			}
		}
		
		strbuf.append("\n");
		strbuf.append(constraintText);
		strbuf.append("\n");
		if (otherConstraintInfo != null)
		{
			strbuf.append(otherConstraintInfo.toString());
		}
		strbuf.append("\n");
		return strbuf.toString();
	}
}

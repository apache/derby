/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.*;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.services.uuid.UUIDFactory;

import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.DefaultInfo;
import org.apache.derby.catalog.Dependable;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.ReferencedColumns;
import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.Statistics;
import java.sql.Timestamp;
import java.io.InputStream;

/**
 * This is an implementation of the DataDescriptorGenerator interface
 * that lives in the DataDictionary protocol.  See that interface for
 * a description of what this class is supposed to do.
 *
 * @version 0.1
 * @author Jeff Lichtman
 */

public class DataDescriptorGenerator 
{
	private 	UUIDFactory uuidf;

    protected	final DataDictionary	dataDictionary; // the data dictionary that this generator operates on

	/**
	  *	Make a generator. Specify the data dictionary that it operates on.
	  *
	  *	@param	dataDictionary	the data dictionary that this generator makes objects for
	  */
	public	DataDescriptorGenerator( DataDictionary dataDictionary )
	{
		this.dataDictionary = dataDictionary;
	}

	/**
	 * Create a descriptor for the named schema with a null UUID. 
	 *
	 * @param schemaName	The name of the schema we're interested in.
	 *			If the name is NULL, get the descriptor for the
	 *			current schema.
	 * @param aid	The authorization ID associated with the schema.
	 *		The owner of the schema.
	 *
	 * @param oid	The object ID 
	 *
	 * @return	The descriptor for the schema.
	 * @exception StandardException		Thrown on failure
	 */
	public SchemaDescriptor	newSchemaDescriptor(String schemaName, 
		String aid, UUID oid)
		throws StandardException
	{
		return new SchemaDescriptor(
            dataDictionary, schemaName, aid, oid, 
            dataDictionary.isSystemSchemaName(schemaName));
	}

	/**
	 * Create a descriptor for the named table within the given schema.
	 * If the schema parameter is NULL, it creates a schema descriptor
	 * using the current default schema.
	 *
	 * @param tableName	The name of the table to get the descriptor for
	 * @param schema	The descriptor for the schema the table lives in.
	 *			If null, use the current (default) schema.
	 * @param tableType	The type of the table: base table or view.
	 * @param lockGranularity	The lock granularity.
	 *
	 * @return	The descriptor for the table.
	 */
	public TableDescriptor	newTableDescriptor
	(
		String 				tableName,
		SchemaDescriptor	schema,
		int					tableType,
		char				lockGranularity
    )
	{
		return new TableDescriptor
			(dataDictionary, tableName, schema, tableType, lockGranularity);
	}

	/**
	 * Create a descriptor for the temporary table within the given schema.
	 *
	 * @param tableName	The name of the temporary table to get the descriptor for
	 * @param schema	The descriptor for the schema the table lives in.
	 * @param tableType	The type of the table: temporary table
	 * @param onCommitDeleteRows	If true, on commit delete rows else on commit preserve rows of temporary table.
	 * @param onRollbackDeleteRows	If true, on rollback, delete rows from temp tables which were logically modified. true is the only supported value
	 *
	 * @return	The descriptor for the table.
	 */
	public TableDescriptor	newTableDescriptor
	(
		String 				tableName,
		SchemaDescriptor	schema,
		int					tableType,
		boolean				onCommitDeleteRows,
		boolean				onRollbackDeleteRows
    )
	{
		return new TableDescriptor
			(dataDictionary, tableName, schema, tableType, onCommitDeleteRows, onRollbackDeleteRows);
	}

	/**
	 * Create a viewDescriptor for the view with the given UUID.
	 *
	 * @param viewID	the UUID for the view.
	 * @param viewName	the name of the view
	 * @param viewText	the text of the view's query.
	 * @param checkOption	int for check option type 
	 * @param compSchemaId	the UUID of the schema this was compiled in
	 *
	 * @return	A descriptor for the view
	 */
	public ViewDescriptor newViewDescriptor(UUID viewID,
				String viewName, String viewText, int checkOption,
				UUID compSchemaId)
	{
		return new ViewDescriptor(dataDictionary, viewID, viewName, 
				viewText, checkOption, compSchemaId);
	}


	/**
	 * @see DataDescriptorGenerator#newUniqueConstraintDescriptor
	 */
	public ReferencedKeyConstraintDescriptor	newUniqueConstraintDescriptor(
						TableDescriptor table,
						String constraintName,
						boolean deferrable,
						boolean initiallyDeferred,
						int[] referencedColumns,
						UUID		constraintId,
						UUID		indexId,
						SchemaDescriptor schemaDesc,
						boolean isEnabled,
						int referenceCount
						)
	{
		return new ReferencedKeyConstraintDescriptor(DataDictionary.UNIQUE_CONSTRAINT,
			dataDictionary, table, constraintName,
				deferrable, initiallyDeferred, 
				referencedColumns, constraintId, 
				indexId, schemaDesc, isEnabled, referenceCount);
	}

	/**
	 * @see DataDescriptorGenerator#newPrimaryKeyConstraintDescriptor
	 */
	public ReferencedKeyConstraintDescriptor	newPrimaryKeyConstraintDescriptor(
						TableDescriptor table,
						String constraintName,
						boolean deferrable,
						boolean initiallyDeferred,
						int[] referencedColumns,
						UUID		constraintId,
						UUID indexId,
						SchemaDescriptor schemaDesc,
						boolean isEnabled,
						int referenceCount
						)
	{
		return new ReferencedKeyConstraintDescriptor(DataDictionary.PRIMARYKEY_CONSTRAINT,
			dataDictionary, table, constraintName,
				deferrable, initiallyDeferred, 
				referencedColumns, constraintId, 
				indexId, schemaDesc, isEnabled, referenceCount);
	}

	/**
	 * @see DataDescriptorGenerator#newForeignKeyConstraintDescriptor
	 */
	public ForeignKeyConstraintDescriptor	newForeignKeyConstraintDescriptor(
						TableDescriptor table,
						String constraintName,
						boolean deferrable,
						boolean initiallyDeferred,
						int[] fkColumns,
						UUID	constraintId,
						UUID indexId,
						SchemaDescriptor schemaDesc,
						ReferencedKeyConstraintDescriptor	referencedConstraintDescriptor,
						boolean isEnabled,
						int raDeleteRule,
						int raUpdateRule
						)
	{
		return new ForeignKeyConstraintDescriptor(dataDictionary, table, constraintName,
				deferrable, initiallyDeferred, 
				fkColumns, constraintId, 
				indexId, schemaDesc, 
				referencedConstraintDescriptor, isEnabled, raDeleteRule, raUpdateRule);
	}


	/**
	 * @see DataDescriptorGenerator#newForeignKeyConstraintDescriptor
	 */
	public ForeignKeyConstraintDescriptor	newForeignKeyConstraintDescriptor(
						TableDescriptor table,
						String constraintName,
						boolean deferrable,
						boolean initiallyDeferred,
						int[] fkColumns,
						UUID	constraintId,
						UUID indexId,
						SchemaDescriptor schemaDesc,
						UUID	referencedConstraintId,
						boolean isEnabled,
						int raDeleteRule,
						int raUpdateRule
						)
	{
		return new ForeignKeyConstraintDescriptor(dataDictionary, table, constraintName,
				deferrable, initiallyDeferred, 
				fkColumns, constraintId, 
				indexId, schemaDesc, 
				referencedConstraintId, isEnabled, raDeleteRule, raUpdateRule);
	}

	/**
	 * @see DataDescriptorGenerator#newCheckConstraintDescriptor
	 */
	public CheckConstraintDescriptor	newCheckConstraintDescriptor(
						TableDescriptor table,
						String constraintName,
						boolean deferrable,
						boolean initiallyDeferred,
						UUID		constraintId,
						String constraintText,
						ReferencedColumns referencedColumns,
						SchemaDescriptor schemaDesc,
						boolean isEnabled
						)
	{
		return new CheckConstraintDescriptor(dataDictionary, table, constraintName,
				deferrable, initiallyDeferred, 
				constraintId, 
				constraintText, referencedColumns, schemaDesc, isEnabled);
	}

	public CheckConstraintDescriptor	newCheckConstraintDescriptor(
						TableDescriptor table,
						String constraintName,
						boolean deferrable,
						boolean initiallyDeferred,
						UUID		constraintId,
						String constraintText,
						int[] refCols,
						SchemaDescriptor schemaDesc,
						boolean isEnabled
						)
	{
		ReferencedColumns referencedColumns = new ReferencedColumnsDescriptorImpl(refCols);
		return new CheckConstraintDescriptor(dataDictionary, table, constraintName,
				deferrable, initiallyDeferred, 
				constraintId, 
				constraintText, referencedColumns, schemaDesc, isEnabled);
	}

	/**
	 * Create a conglomerate descriptor for the given conglomerate id.
	 *
	 * @param conglomerateID	The identifier for the conglomerate
	 *				we're interested in
	 * @param name			The name of the conglomerate, if any
	 * @param indexable		TRUE means the conglomerate is indexable,
	 *				FALSE means it isn't
	 * @param indexRowGenerator	The IndexRowGenerator for the conglomerate,
	 *							null if it's a heap
	 * @param isConstraint	TRUE means the conglomerate is an index backing 
	 *						up a constraint, FALSE means it isn't
	 *
	 * @param uuid	UUID  for this conglomerate
	 * @param tableID	UUID for the table that this conglomerate belongs to
	 * @param schemaID	UUID for the schema that conglomerate belongs to
	 *
	 * @return	A ConglomerateDescriptor describing the 
	 *		conglomerate. 
	 */
	public ConglomerateDescriptor	newConglomerateDescriptor(
						long conglomerateId,
						String name,
						boolean indexable,
						IndexRowGenerator indexRowGenerator,
						boolean isConstraint,
						UUID uuid,
						UUID tableID,
						UUID schemaID
						)
	{
		return (ConglomerateDescriptor)
				new ConglomerateDescriptor(dataDictionary, conglomerateId,
												name,
												indexable,
												indexRowGenerator,
												isConstraint,
												uuid,
											    tableID,
												schemaID);
	}

	/**
	 * Create a new trigger descriptor.
	 *
	 * @param sd	the schema descriptor for this trigger
	 * @param id	the trigger id
	 * @param name	the trigger name
	 * @param eventMask	TriggerDescriptor.TRIGGER_EVENT_XXXX
	 * @param isBefore	is this a before (as opposed to after) trigger 
	 * @param isRow		is this a row trigger or statement trigger
	 * @param isEnabled	is this trigger enabled or disabled
	 * @param td		the table upon which this trigger is defined
	 * @param whenSPSId	the sps id for the when clause (may be null)
	 * @param actionSPSId	the spsid for the trigger action (may be null)
	 * @param creationTimestamp	when was this trigger created?
	 * @param referencedCols	what columns does this trigger reference (may be null)
	 * @param triggerDefinition The original user text of the trigger action
	 * @param referencingOld whether or not OLD appears in REFERENCING clause
	 * @param referencingNew whether or not NEW appears in REFERENCING clause
	 * @param oldReferencingName old referencing table name, if any, that appears in REFERCING clause
	 * @param newReferencingName new referencing table name, if any, that appears in REFERCING clause
	 *
	 * @exception StandardException on error
	 */
	public TriggerDescriptor newTriggerDescriptor
	(
		SchemaDescriptor	sd,
		UUID				uuid,
		String				name,
		int					eventMask,
		boolean				isBefore,
		boolean 			isRow,
		boolean 			isEnabled,
		TableDescriptor		td,
		UUID				whenSPSId,
		UUID				actionSPSId,
		Timestamp			creationTimestamp,
		int[]				referencedCols,
		String				triggerDefinition,
		boolean				referencingOld,
		boolean				referencingNew,
		String				oldReferencingName,
		String				newReferencingName
	) throws StandardException
	{
		return new TriggerDescriptor(
					dataDictionary,
					sd,
					uuid,
					name,
					eventMask,
					isBefore,
					isRow,
					isEnabled,
					td,
					whenSPSId,
					actionSPSId,
					creationTimestamp,
					referencedCols,
					triggerDefinition,
					referencingOld,
					referencingNew,
					oldReferencingName,
					newReferencingName
					);
	}
		
	/*
	  get a UUIDFactory. This uses the Monitor to get one the
	  first time and holds onto it for later.
	  */
	protected UUIDFactory getUUIDFactory()
	{
		if (uuidf == null)
			uuidf = Monitor.getMonitor().getUUIDFactory();
		return uuidf;
	}

	/**
	  @see DataDescriptorGenerator#newFileInfoDescriptor
	  */
	public FileInfoDescriptor newFileInfoDescriptor(
								UUID             id,
								SchemaDescriptor sd,
								String           SQLName,
								long              generationId
								)
	{
		if (id == null) id = getUUIDFactory().createUUID();
		return new FileInfoDescriptor(dataDictionary, id,sd,SQLName,generationId);
	}
	 	
}

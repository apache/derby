/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator

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

package org.apache.derby.iapi.sql.dictionary;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Timestamp;
import org.apache.derby.catalog.ReferencedColumns;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.types.DataTypeDescriptor;

/**
 * This is an implementation of the DataDescriptorGenerator interface
 * that lives in the DataDictionary protocol.  See that interface for
 * a description of what this class is supposed to do.
 *
 * @version 0.1
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
	 * @param conglomerateId	The identifier for the conglomerate
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
        return new ConglomerateDescriptor(dataDictionary,
                                          conglomerateId,
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
	 * @param uuid	the trigger id
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
	 * @param referencedColsInTriggerAction	what columns does the trigger 
	 *						action reference through old/new transition variables
	 *						(may be null)
	 * @param triggerDefinition The original user text of the trigger action
	 * @param referencingOld whether or not OLD appears in REFERENCING clause
	 * @param referencingNew whether or not NEW appears in REFERENCING clause
	 * @param oldReferencingName old referencing table name, if any, that appears in REFERCING clause
	 * @param newReferencingName new referencing table name, if any, that appears in REFERCING clause
     * @param whenClauseText the SQL text of the WHEN clause (may be null)
     * @return a trigger descriptor
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
		int[]				referencedColsInTriggerAction,
		String				triggerDefinition,
		boolean				referencingOld,
		boolean				referencingNew,
		String				oldReferencingName,
        String              newReferencingName,
        String              whenClauseText
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
					referencedColsInTriggerAction,
					triggerDefinition,
					referencingOld,
					referencingNew,
					oldReferencingName,
                    newReferencingName,
                    whenClauseText
					);
	}
		
	/*
	  get a UUIDFactory. This uses the Monitor to get one the
	  first time and holds onto it for later.
	  */
	protected UUIDFactory getUUIDFactory()
	{
		if (uuidf == null)
			uuidf = getMonitor().getUUIDFactory();
		return uuidf;
	}

    /**
     * Create  a new {@code FileInfoDescriptor} using the supplied arguments.
     * 
     * id unique id to be used for the new file descriptor
     * sd schema of the new file to be stored in the database
     * SQLName the SQL name of the new schema object representing the file
     * generationID version numberof the file the descriptor describes
     * 
     * @return the newly created file info descriptor
     */
    public FileInfoDescriptor newFileInfoDescriptor(
                                UUID             id,
                                SchemaDescriptor sd,
                                String           sqlName,
                                long             generationId
                                )
    {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(id != null);
        }

        return new FileInfoDescriptor(
                dataDictionary, id, sd, sqlName, generationId);
    }

	public UserDescriptor newUserDescriptor
        (
         String userName,
         String hashingScheme,
         char[] password,
         Timestamp lastModified
         )
	{
		return new UserDescriptor( dataDictionary, userName, hashingScheme, password, lastModified );
	}
	 	
    public TablePermsDescriptor newTablePermsDescriptor( TableDescriptor td,
                                                         String selectPerm,
                                                         String deletePerm,
                                                         String insertPerm,
                                                         String updatePerm,
                                                         String referencesPerm,
                                                         String triggerPerm,
                                                         String grantor)
	throws StandardException
    {
        if( "N".equals( selectPerm) && "N".equals( deletePerm) && "N".equals( insertPerm)
            && "N".equals( updatePerm) && "N".equals( referencesPerm) && "N".equals( triggerPerm))
            return null;
        
        return new TablePermsDescriptor( dataDictionary,
                                         (String) null,
                                         grantor,
                                         td.getUUID(),
                                         selectPerm,
                                         deletePerm,
                                         insertPerm,
                                         updatePerm,
                                         referencesPerm,
                                         triggerPerm);
    }

    /**
     * Manufacture a new ColPermsDescriptor.
     *
     * @param td The descriptor of the table.
     * @param type The action type:
     *<ol>
     *<li>"s" - select without grant
     *<li>"S" - select with grant
     *<li>"u" - update without grant
     *<li>"U" - update with grant
     *<li>"r" - references without grant
     *<li>"R" - references with grant
     *</ol>
     * @param columns the set of columns
     */
    public ColPermsDescriptor newColPermsDescriptor( TableDescriptor td,
                                                     String type,
                                                     FormatableBitSet columns,
                                                     String grantor) throws StandardException
    {
        return new ColPermsDescriptor( dataDictionary,
                                       (String) null,
                                       grantor,
                                       td.getUUID(),
                                       type,
                                       columns);
    }

    /**
     * Create a new routine permissions descriptor
     *
     * @param ad The routine's alias descriptor
     * @param grantor
     */
    public RoutinePermsDescriptor newRoutinePermsDescriptor( AliasDescriptor ad, String grantor)
	throws StandardException
    {
        return new RoutinePermsDescriptor( dataDictionary,
                                           (String) null,
                                           grantor,
                                           ad.getUUID());
    }


    /**
     * Create a new role grant descriptor
     *
	 * @param uuid unique identifier for this role grant descriptor in
	 *        time and space
     * @param roleName the name of the role for which a new descriptor
     *                 is created
     * @param grantee authorization identifier of grantee
     * @param grantor authorization identifier of grantor
	 * @param withadminoption if true, WITH ADMIN OPTION is set for
	 *        this descriptor
     * @param isDef if true, this descriptor represents a role
     *              definition, otherwise it represents a grant.
     */
    public RoleGrantDescriptor newRoleGrantDescriptor(UUID uuid,
													  String roleName,
													  String grantee,
													  String grantor,
													  boolean withadminoption,
													  boolean isDef)
        throws StandardException
    {
        return new RoleGrantDescriptor(dataDictionary,
									   uuid,
									   roleName,
									   grantee,
									   grantor,
									   withadminoption,
									   isDef);
    }

    /**
     * Create a new sequence descriptor
     * @param uuid
     * @param sequenceName
     * @return SequenceDescriptor
     */
    public SequenceDescriptor newSequenceDescriptor(
            SchemaDescriptor sd,
            UUID uuid,
            String sequenceName,
            DataTypeDescriptor dataType,
            Long currentValue,
            long startValue,
            long minimumValue,
            long maximumValue,
            long increment,
            boolean cycle) {
        return new SequenceDescriptor(
                dataDictionary,
                sd,
                uuid,
                sequenceName,
                dataType,
                currentValue,
                startValue,
                minimumValue,
                maximumValue,
                increment,
                cycle);
    }

    public PermDescriptor newPermDescriptor(
            UUID uuid,
            String objectType,
            UUID permObjectId,
            String permission,
            String grantor,
            String grantee,
            boolean grantable) {
        return new PermDescriptor(dataDictionary,
                uuid,
                objectType,
                permObjectId,
                permission,
                grantor,
                grantee,
                grantable);
    }
    
    /**
     * Privileged Monitor lookup. Must be package private so that user code
     * can't call this entry point.
     */
    static  ModuleFactory  getMonitor()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ModuleFactory>()
             {
                 public ModuleFactory run()
                 {
                     return Monitor.getMonitor();
                 }
             }
             );
    }

}

/*

   Derby - Class org.apache.derby.impl.storeless.EmptyDictionary

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derby.impl.storeless;

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;

import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.db.Database;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.EngineType;
import org.apache.derby.iapi.services.daemon.IndexStatisticsDaemon;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.BulkInsertCounter;
import org.apache.derby.iapi.sql.dictionary.ColPermsDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DependencyDescriptor;
import org.apache.derby.iapi.sql.dictionary.FileInfoDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptorList;
import org.apache.derby.iapi.sql.dictionary.PasswordHasher;
import org.apache.derby.iapi.sql.dictionary.PermissionsDescriptor;
import org.apache.derby.iapi.sql.dictionary.RoutinePermsDescriptor;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.RoleGrantDescriptor;
import org.apache.derby.iapi.sql.dictionary.SequenceDescriptor;
import org.apache.derby.iapi.sql.dictionary.StatisticsDescriptor;
import org.apache.derby.iapi.sql.dictionary.PermDescriptor;
import org.apache.derby.iapi.sql.dictionary.RoleClosureIterator;
import org.apache.derby.iapi.sql.dictionary.SubKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TablePermsDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.dictionary.UserDescriptor;
import org.apache.derby.iapi.sql.dictionary.ViewDescriptor;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.RowLocation;

/**
 * DataDictionary implementation that does nothing!
 * Used for the storeless system.
 *
 */
public class EmptyDictionary implements DataDictionary, ModuleSupportable {

	public void clearCaches( boolean clearSequenceCaches ) {}
    
	public void clearCaches() throws StandardException {
		// Auto-generated method stub

	}

	public void clearSequenceCaches() throws StandardException {
		// Auto-generated method stub

	}

	public int startReading(LanguageConnectionContext lcc)
			throws StandardException {
		// Auto-generated method stub
		return 0;
	}

	public void doneReading(int mode, LanguageConnectionContext lcc)
			throws StandardException {
		// Auto-generated method stub

	}

	public void startWriting(LanguageConnectionContext lcc)
			throws StandardException {
		// Auto-generated method stub

	}

	public void transactionFinished() throws StandardException {
		// Auto-generated method stub

	}

	public ExecutionFactory getExecutionFactory() {
		// Auto-generated method stub
		return null;
	}

	public DataValueFactory getDataValueFactory() {
		// Auto-generated method stub
		return null;
	}

	public DataDescriptorGenerator getDataDescriptorGenerator() {
		// Auto-generated method stub
		return null;
	}

	public String getAuthorizationDatabaseOwner() {
		// Auto-generated method stub
		return null;
	}

	public boolean usesSqlAuthorization() {
		// Auto-generated method stub
		return false;
	}

	public SchemaDescriptor getSchemaDescriptor(String schemaName,
			TransactionController tc, boolean raiseError)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public SchemaDescriptor getSchemaDescriptor(UUID schemaId,
			TransactionController tc) throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public SchemaDescriptor	getSchemaDescriptor(UUID schemaId,
												int isolationLevel,
//IC see: https://issues.apache.org/jira/browse/DERBY-3678
												TransactionController tc)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public boolean existsSchemaOwnedBy(String authid,
//IC see: https://issues.apache.org/jira/browse/DERBY-3673
									   TransactionController tc)
			throws StandardException {
		// Auto-generated method stub
		return false;
	}

    public  PasswordHasher  makePasswordHasher( Dictionary props )
        throws StandardException {
		// Auto-generated method stub
		return null;
	}


	public SchemaDescriptor getSystemSchemaDescriptor()
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public SchemaDescriptor getSysIBMSchemaDescriptor()
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public SchemaDescriptor getDeclaredGlobalTemporaryTablesSchemaDescriptor()
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public boolean isSystemSchemaName(String name) throws StandardException {
		// Auto-generated method stub
		return false;
	}

	public void	dropRoleGrant(String roleName,
//IC see: https://issues.apache.org/jira/browse/DERBY-3137
//IC see: https://issues.apache.org/jira/browse/DERBY-3137
							  String grantee,
							  String grantor,
							  TransactionController tc)
			throws StandardException {
		// Auto-generated method stub
	}

	public void	dropRoleGrantsByGrantee(String grantee,
										TransactionController tc)
			throws StandardException {
		// Auto-generated method stub
	}

	public void	dropRoleGrantsByName(String roleName,
									 TransactionController tc)
			throws StandardException {
		// Auto-generated method stub
	}

	public RoleClosureIterator createRoleClosureIterator
//IC see: https://issues.apache.org/jira/browse/DERBY-3722
		(TransactionController tc,
		 String role,
		 boolean inverse
		) throws StandardException {
		// Auto-generated method stub
		return (RoleClosureIterator)null;
	}

	public void	dropAllPermsByGrantee(String authid,
									  TransactionController tc)
			throws StandardException {
		// Auto-generated method stub
	}

	public void dropSchemaDescriptor(String schemaName, TransactionController tc)
			throws StandardException {
		// Auto-generated method stub

	}

	public boolean isSchemaEmpty(SchemaDescriptor sd) throws StandardException {
		// Auto-generated method stub
		return false;
	}

	public RoleGrantDescriptor getRoleDefinitionDescriptor(String roleName)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}


	public RoleGrantDescriptor getRoleGrantDescriptor(UUID uuid)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}


	public RoleGrantDescriptor getRoleGrantDescriptor(String roleName,
//IC see: https://issues.apache.org/jira/browse/DERBY-3137
													  String grantee,
													  String grantor)
		throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public boolean existsGrantToAuthid(String authId,
//IC see: https://issues.apache.org/jira/browse/DERBY-3673
									   TransactionController tc)
			throws StandardException {
		// Auto-generated method stub
		return false;
	}

	public TableDescriptor getTableDescriptor(String tableName,
//IC see: https://issues.apache.org/jira/browse/DERBY-3012
			SchemaDescriptor schema, TransactionController tc) throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public TableDescriptor getTableDescriptor(UUID tableID)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public void dropTableDescriptor(TableDescriptor td,
			SchemaDescriptor schema, TransactionController tc)
			throws StandardException {
		// Auto-generated method stub

	}

	public void updateLockGranularity(TableDescriptor td,
			SchemaDescriptor schema, char lockGranularity,
			TransactionController tc) throws StandardException {
		// Auto-generated method stub

	}

	public ColumnDescriptor getColumnDescriptorByDefaultId(UUID uuid)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public void dropColumnDescriptor(UUID tableID, String columnName,
			TransactionController tc) throws StandardException {
		// Auto-generated method stub

	}

	public void dropAllColumnDescriptors(UUID tableID, TransactionController tc)
			throws StandardException {
		// Auto-generated method stub

	}

	public void dropAllTableAndColPermDescriptors(UUID tableID,
			TransactionController tc) throws StandardException {
		// Auto-generated method stub

	}

	public void updateSYSCOLPERMSforAddColumnToUserTable(UUID tableID,
			TransactionController tc) throws StandardException {
		// Auto-generated method stub

	}

	public void updateSYSCOLPERMSforDropColumn(UUID tableID,
		TransactionController tc, ColumnDescriptor columnDescriptor)
	    throws StandardException
	{
	}

	public void dropAllRoutinePermDescriptors(UUID routineID,
			TransactionController tc) throws StandardException {
		// Auto-generated method stub

	}

	public ViewDescriptor getViewDescriptor(UUID uuid) throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public ViewDescriptor getViewDescriptor(TableDescriptor td)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public void dropViewDescriptor(ViewDescriptor viewDescriptor,
			TransactionController tc) throws StandardException {
		// Auto-generated method stub

	}

	public ConstraintDescriptor getConstraintDescriptor(UUID uuid)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public ConstraintDescriptor getConstraintDescriptor(String constraintName,
			UUID schemaID) throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public ConstraintDescriptorList getConstraintDescriptors(TableDescriptor td)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public ConstraintDescriptorList getActiveConstraintDescriptors(
			ConstraintDescriptorList cdl) throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public boolean activeConstraint(ConstraintDescriptor constraint)
			throws StandardException {
		// Auto-generated method stub
		return false;
	}

	public ConstraintDescriptor getConstraintDescriptor(TableDescriptor td,
			UUID uuid) throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public ConstraintDescriptor getConstraintDescriptorById(TableDescriptor td,
			UUID uuid) throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public ConstraintDescriptor getConstraintDescriptorByName(
			TableDescriptor td, SchemaDescriptor sd, String constraintName,
			boolean forUpdate) throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public TableDescriptor getConstraintTableDescriptor(UUID constraintId)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public ConstraintDescriptorList getForeignKeys(UUID constraintId)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public void addConstraintDescriptor(ConstraintDescriptor descriptor,
			TransactionController tc) throws StandardException {
		// Auto-generated method stub

	}

	public void dropConstraintDescriptor(
            ConstraintDescriptor descriptor, TransactionController tc)
			throws StandardException {
		// Auto-generated method stub

	}

	public void dropAllConstraintDescriptors(TableDescriptor table,
			TransactionController tc) throws StandardException {
		// Auto-generated method stub

	}

	public void updateConstraintDescriptor(ConstraintDescriptor cd,
			UUID formerUUID, int[] colsToSet, TransactionController tc)
			throws StandardException {
		// Auto-generated method stub

	}

	public SubKeyConstraintDescriptor getSubKeyConstraint(UUID constraintId,
			int type) throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public SPSDescriptor getSPSDescriptor(UUID uuid) throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public SPSDescriptor getSPSDescriptor(String name, SchemaDescriptor sd)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

    public List<SPSDescriptor> getAllSPSDescriptors() throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public DataTypeDescriptor[] getSPSParams(SPSDescriptor spsd, List defaults)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public void addSPSDescriptor(SPSDescriptor descriptor,
			TransactionController tc) throws StandardException {
		// Auto-generated method stub

	}

	public void updateSPS(SPSDescriptor spsd, TransactionController tc,
            boolean recompile) throws StandardException {
		// Auto-generated method stub

	}

	public void dropSPSDescriptor(SPSDescriptor descriptor,
			TransactionController tc) throws StandardException {
		// Auto-generated method stub

	}

	public void dropSPSDescriptor(UUID uuid, TransactionController tc)
			throws StandardException {
		// Auto-generated method stub

	}

	public void invalidateAllSPSPlans() throws StandardException {
		// Auto-generated method stub

	}

	public void invalidateAllSPSPlans(LanguageConnectionContext lcc) 
//IC see: https://issues.apache.org/jira/browse/DERBY-5578
			throws StandardException{
		// Auto-generated method stub

	}

	public int[] examineTriggerNodeAndCols(
//IC see: https://issues.apache.org/jira/browse/DERBY-6783
			Visitable actionStmt,
			String oldReferencingName,
			String newReferencingName,
			String triggerDefinition,
			int[] referencedCols,
			int[] referencedColsInTriggerAction,
			int actionOffset,
			TableDescriptor triggerTableDescriptor,
			int triggerEventMask,
            boolean createTriggerTime,
            List<int[]> replacements
			) throws StandardException
	{
		return null;
	}

	public String getTriggerActionString(
//IC see: https://issues.apache.org/jira/browse/DERBY-4845
			Visitable actionStmt,
			String oldReferencingName,
			String newReferencingName,
			String triggerDefinition,
			int[] referencedCols,
			int[] referencedColsInTriggerAction,
			int actionOffset,
			TableDescriptor td,
			int triggerEventMask,
            boolean createTriggerTime,
//IC see: https://issues.apache.org/jira/browse/DERBY-6783
            List<int[]> replacements,
            int[] cols)
	throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public TriggerDescriptor getTriggerDescriptor(UUID uuid)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public TriggerDescriptor getTriggerDescriptor(String name,
			SchemaDescriptor sd) throws StandardException {
		// Auto-generated method stub
		return null;
	}

    public TriggerDescriptorList getTriggerDescriptors(TableDescriptor td)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public void updateTriggerDescriptor(TriggerDescriptor triggerd,
			UUID formerUUID, int[] colsToSet, TransactionController tc)
			throws StandardException {
		// Auto-generated method stub

	}

	public void dropTriggerDescriptor(TriggerDescriptor descriptor,
			TransactionController tc) throws StandardException {
		// Auto-generated method stub

	}

    @SuppressWarnings("UseOfObsoleteCollectionType")
    public Hashtable<Long, ConglomerateDescriptor>
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        hashAllConglomerateDescriptorsByNumber(TransactionController tc)
            throws StandardException {
		// Auto-generated method stub
		return null;
	}

    @SuppressWarnings("UseOfObsoleteCollectionType")
    public Hashtable<UUID, TableDescriptor>
        hashAllTableDescriptorsByTableId(TransactionController tc)
            throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public ConglomerateDescriptor getConglomerateDescriptor(UUID uuid)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public ConglomerateDescriptor[] getConglomerateDescriptors(UUID uuid)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public ConglomerateDescriptor getConglomerateDescriptor(
			long conglomerateNumber) throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public ConglomerateDescriptor[] getConglomerateDescriptors(
			long conglomerateNumber) throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public ConglomerateDescriptor getConglomerateDescriptor(String indexName,
			SchemaDescriptor sd, boolean forUpdate) throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public void dropConglomerateDescriptor(ConglomerateDescriptor conglomerate,
			TransactionController tc) throws StandardException {
		// Auto-generated method stub

	}

	public void dropAllConglomerateDescriptors(TableDescriptor td,
			TransactionController tc) throws StandardException {
		// Auto-generated method stub

	}

	public void updateConglomerateDescriptor(ConglomerateDescriptor[] cds,
			long conglomerateNumber, TransactionController tc)
			throws StandardException {
		// Auto-generated method stub

	}

	public void updateConglomerateDescriptor(ConglomerateDescriptor cd,
			long conglomerateNumber, TransactionController tc)
			throws StandardException {
		// Auto-generated method stub

	}

	public List<DependencyDescriptor> getDependentsDescriptorList(String dependentID)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public List<DependencyDescriptor> getProvidersDescriptorList(String providerID)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

    public List<TupleDescriptor> getAllDependencyDescriptorsList()
            throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public void dropStoredDependency(DependencyDescriptor dd,
			TransactionController tc) throws StandardException {
		// Auto-generated method stub

	}

	public void dropDependentsStoredDependencies(UUID dependentsUUID,
			TransactionController tc) throws StandardException {
		// Auto-generated method stub

	}

	public UUIDFactory getUUIDFactory() {
		// Auto-generated method stub
		return null;
	}

    public AliasDescriptor getAliasDescriptorForUDT( TransactionController tc, DataTypeDescriptor dtd )
        throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public AliasDescriptor getAliasDescriptor(UUID uuid)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public AliasDescriptor getAliasDescriptor(String schemaID,
			String aliasName, char nameSpace) throws StandardException {
		// Auto-generated method stub
		return null;
	}

    public List<AliasDescriptor> getRoutineList(String schemaID,
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
                                                String routineName,
                                                char nameSpace)
            throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public void dropAliasDescriptor(AliasDescriptor ad, TransactionController tc)
			throws StandardException {
		// Auto-generated method stub

	}

	public void updateUser( UserDescriptor newDescriptor,TransactionController tc )
		throws StandardException
	{
		// Auto-generated method stub
    }

	public void dropUser( String userName,TransactionController tc )
		throws StandardException
	{
		// Auto-generated method stub
    }

	public UserDescriptor getUser( String userName )
			throws StandardException
	{
		// Auto-generated method stub
        return null;
    }

	public int getEngineType() {
		// Auto-generated method stub
		return 0;
	}
	
	public int getCollationTypeOfSystemSchemas(){
		// Auto-generated method stub
		return 0;
	}

	public int getCollationTypeOfUserSchemas(){
		// Auto-generated method stub
		return 0;
	}
	
	public FileInfoDescriptor getFileInfoDescriptor(UUID id)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public FileInfoDescriptor getFileInfoDescriptor(SchemaDescriptor sd,
			String name) throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public void dropFileInfoDescriptor(FileInfoDescriptor fid)
			throws StandardException {
		// Auto-generated method stub

	}

	public RowLocation[] computeAutoincRowLocations(TransactionController tc,
			TableDescriptor td) throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public void computeSequenceRowLocation
        ( TransactionController tc, String sequenceIDstring, RowLocation[] rowLocation, SequenceDescriptor[] sequenceDescriptor )
		throws StandardException
    {
		// Auto-generated method stub
    }

    public  boolean updateCurrentSequenceValue
        ( TransactionController tc, RowLocation rowLocation, boolean wait, Long oldValue, Long newValue )
        throws StandardException
    {
		// Auto-generated method stub
        return true;
    }
               
    public void getCurrentValueAndAdvance
//IC see: https://issues.apache.org/jira/browse/DERBY-5687
//IC see: https://issues.apache.org/jira/browse/DERBY-4437
        ( String sequenceUUIDstring, NumberDataValue returnValue )
        throws StandardException
    {
		// Auto-generated method stub
    }
    
    public Long peekAtIdentity( String schemaName, String tableName )
        throws StandardException
    {
		return null;
    }
    
    public Long peekAtSequence( String schemaName, String sequenceName )
        throws StandardException
    {
		return null;
    }
    
	public RowLocation getRowLocationTemplate(LanguageConnectionContext lcc,
			TableDescriptor td) throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public NumberDataValue getSetAutoincrementValue(RowLocation rl,
//IC see: https://issues.apache.org/jira/browse/DERBY-5687
//IC see: https://issues.apache.org/jira/browse/DERBY-4437
			TransactionController tc, boolean doUpdate,
			NumberDataValue newValue, boolean wait) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public void setAutoincrementValue(TransactionController tc, UUID tableUUID,
			String columnName, long aiValue, boolean incrementNeeded)
			throws StandardException {
		// Auto-generated method stub

	}

	public List<StatisticsDescriptor> getStatisticsDescriptors(TableDescriptor td)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public void dropStatisticsDescriptors(UUID tableUUID, UUID referenceUUID,
			TransactionController tc) throws StandardException {
		// Auto-generated method stub

	}

	public DependencyManager getDependencyManager() {
		// Auto-generated method stub
		return null;
	}

	public int getCacheMode() {
		// Auto-generated method stub
		return 0;
	}

	public String getSystemSQLName() {
		// Auto-generated method stub
		return null;
	}

	public void addDescriptor(TupleDescriptor tuple, TupleDescriptor parent,
			int catalogNumber, boolean allowsDuplicates,
			TransactionController tc) throws StandardException {
		// Auto-generated method stub

	}

	public void addDescriptorArray(TupleDescriptor[] tuple,
			TupleDescriptor parent, int catalogNumber,
			boolean allowsDuplicates, TransactionController tc)
			throws StandardException {
		// Auto-generated method stub

	}

	public boolean checkVersion(int majorVersion, String feature)
			throws StandardException {
		// Auto-generated method stub
		return false;
	}

    public boolean isReadOnlyUpgrade() {
//IC see: https://issues.apache.org/jira/browse/DERBY-4845
        return false;
    }

	public boolean addRemovePermissionsDescriptor(boolean add,
			PermissionsDescriptor perm, String grantee, TransactionController tc)
			throws StandardException {
		// Auto-generated method stub
		return false;
	}

	public TablePermsDescriptor getTablePermissions(UUID tableUUID,
			String authorizationId) throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public TablePermsDescriptor getTablePermissions(UUID tablePermsUUID)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public ColPermsDescriptor getColumnPermissions(UUID tableUUID,
			int privType, boolean forGrant, String authorizationId)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public ColPermsDescriptor getColumnPermissions(UUID tableUUID,
			String privTypeStr, boolean forGrant, String authorizationId)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public ColPermsDescriptor getColumnPermissions(UUID colPermsUUID)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public RoutinePermsDescriptor getRoutinePermissions(UUID routineUUID,
			String authorizationId) throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public RoutinePermsDescriptor getRoutinePermissions(UUID routinePermsUUID)
			throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public void dropDependentsStoredDependencies(UUID dependentsUUID,
			TransactionController tc, boolean wait) throws StandardException {
		// Auto-generated method stub

	}

	public boolean canSupport(Properties properties) {
		return Monitor.isDesiredType(properties,
                EngineType.STORELESS_ENGINE);
	}

    public String getVTIClass(TableDescriptor td, boolean asTableFunction) throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public String getBuiltinVTIClass(TableDescriptor td, boolean asTableFunction) throws StandardException {
        // Auto-generated method stub
        return null;
    }

	public void updateMetadataSPSes(TransactionController tc) throws StandardException {
		// Auto-generated method stub		
	}

    public void dropSequenceDescriptor(SequenceDescriptor sequenceDescriptor,
                                       TransactionController tc) throws StandardException {
        // Auto-generated method stub
    }

    public SequenceDescriptor getSequenceDescriptor(UUID uuid) throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public SequenceDescriptor getSequenceDescriptor(SchemaDescriptor sd, String sequenceName)
            throws StandardException {
        // Auto-generated method stub
        return null;
    }   

    public PermDescriptor getGenericPermissions(UUID permUUID) throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public PermDescriptor getGenericPermissions(UUID objectID, String objectType, String privilege, String granteeAuthId) 
            throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public void dropAllPermDescriptors(UUID objectID, TransactionController tc)
//IC see: https://issues.apache.org/jira/browse/DERBY-673
            throws StandardException {
        // Auto-generated method stub
    }

    public IndexStatisticsDaemon getIndexStatsRefresher(boolean asDaemon) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4936
        return null;
    }

    public void disableIndexStatsRefresher() {
        // Do nothing here...
    }

    public boolean doCreateIndexStatsRefresher() {
        return false;
    }

    public void createIndexStatsRefresher(Database db, String dbName) {
        // Do nothing
    }

    public DependableFinder getDependableFinder(int formatId) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4845
        return null;
    }

    public DependableFinder getColumnDependableFinder(
            int formatId, byte[] columnBitMap) {
        return null;
    }

    public  BulkInsertCounter   getBulkInsertCounter
        ( String sequenceUUIDString, boolean restart )
    { return null; }
    
    public  void   flushBulkInsertCounter
        ( String sequenceUUIDString, BulkInsertCounter bic )
        throws StandardException
    {}
}

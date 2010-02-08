/*

   Derby - Class org.apache.impl.storeless.EmptyDictionary

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

import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.EngineType;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColPermsDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DependencyDescriptor;
import org.apache.derby.iapi.sql.dictionary.FileInfoDescriptor;
import org.apache.derby.iapi.sql.dictionary.GenericDescriptorList;
import org.apache.derby.iapi.sql.dictionary.PermissionsDescriptor;
import org.apache.derby.iapi.sql.dictionary.RoutinePermsDescriptor;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.RoleGrantDescriptor;
import org.apache.derby.iapi.sql.dictionary.SequenceDescriptor;
import org.apache.derby.iapi.sql.dictionary.PermDescriptor;
import org.apache.derby.iapi.sql.dictionary.RoleClosureIterator;
import org.apache.derby.iapi.sql.dictionary.SubKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TablePermsDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
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

	public void clearCaches() throws StandardException {
		// TODO Auto-generated method stub

	}

	public int startReading(LanguageConnectionContext lcc)
			throws StandardException {
		// TODO Auto-generated method stub
		return 0;
	}

	public void doneReading(int mode, LanguageConnectionContext lcc)
			throws StandardException {
		// TODO Auto-generated method stub

	}

	public void startWriting(LanguageConnectionContext lcc)
			throws StandardException {
		// TODO Auto-generated method stub

	}

	public void transactionFinished() throws StandardException {
		// TODO Auto-generated method stub

	}

	public ExecutionFactory getExecutionFactory() {
		// TODO Auto-generated method stub
		return null;
	}

	public DataValueFactory getDataValueFactory() {
		// TODO Auto-generated method stub
		return null;
	}

	public DataDescriptorGenerator getDataDescriptorGenerator() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getAuthorizationDatabaseOwner() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean usesSqlAuthorization() {
		// TODO Auto-generated method stub
		return false;
	}

	public SchemaDescriptor getSchemaDescriptor(String schemaName,
			TransactionController tc, boolean raiseError)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public SchemaDescriptor getSchemaDescriptor(UUID schemaId,
			TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public SchemaDescriptor	getSchemaDescriptor(UUID schemaId,
												int isolationLevel,
												TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean existsSchemaOwnedBy(String authid,
									   TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub
		return false;
	}

	public SchemaDescriptor getSystemSchemaDescriptor()
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public SchemaDescriptor getSysIBMSchemaDescriptor()
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public SchemaDescriptor getDeclaredGlobalTemporaryTablesSchemaDescriptor()
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isSystemSchemaName(String name) throws StandardException {
		// TODO Auto-generated method stub
		return false;
	}

	public void	dropRoleGrant(String roleName,
							  String grantee,
							  String grantor,
							  TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub
	}

	public void	dropRoleGrantsByGrantee(String grantee,
										TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub
	}

	public void	dropRoleGrantsByName(String roleName,
									 TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub
	}

	public RoleClosureIterator createRoleClosureIterator
		(TransactionController tc,
		 String role,
		 boolean inverse
		) throws StandardException {
		// TODO Auto-generated method stub
		return (RoleClosureIterator)null;
	}

	public void	dropAllPermsByGrantee(String authid,
									  TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub
	}

	public void dropSchemaDescriptor(String schemaName, TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub

	}

	public boolean isSchemaEmpty(SchemaDescriptor sd) throws StandardException {
		// TODO Auto-generated method stub
		return false;
	}

	public RoleGrantDescriptor getRoleDefinitionDescriptor(String roleName)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}


	public RoleGrantDescriptor getRoleGrantDescriptor(UUID uuid)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}


	public RoleGrantDescriptor getRoleGrantDescriptor(String roleName,
													  String grantee,
													  String grantor)
		throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean existsGrantToAuthid(String authId,
									   TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub
		return false;
	}

	public TableDescriptor getTableDescriptor(String tableName,
			SchemaDescriptor schema, TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public TableDescriptor getTableDescriptor(UUID tableID)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public void dropTableDescriptor(TableDescriptor td,
			SchemaDescriptor schema, TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub

	}

	public void updateLockGranularity(TableDescriptor td,
			SchemaDescriptor schema, char lockGranularity,
			TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub

	}

	public ColumnDescriptor getColumnDescriptorByDefaultId(UUID uuid)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public void dropColumnDescriptor(UUID tableID, String columnName,
			TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub

	}

	public void dropAllColumnDescriptors(UUID tableID, TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub

	}

	public void dropAllTableAndColPermDescriptors(UUID tableID,
			TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub

	}

	public void updateSYSCOLPERMSforAddColumnToUserTable(UUID tableID,
			TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub

	}

	public void updateSYSCOLPERMSforDropColumn(UUID tableID,
		TransactionController tc, ColumnDescriptor columnDescriptor)
	    throws StandardException
	{
	}

	public void dropAllRoutinePermDescriptors(UUID routineID,
			TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub

	}

	public ViewDescriptor getViewDescriptor(UUID uuid) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public ViewDescriptor getViewDescriptor(TableDescriptor td)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public void dropViewDescriptor(ViewDescriptor viewDescriptor,
			TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub

	}

	public ConstraintDescriptor getConstraintDescriptor(UUID uuid)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public ConstraintDescriptor getConstraintDescriptor(String constraintName,
			UUID schemaID) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public ConstraintDescriptorList getConstraintDescriptors(TableDescriptor td)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public ConstraintDescriptorList getActiveConstraintDescriptors(
			ConstraintDescriptorList cdl) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean activeConstraint(ConstraintDescriptor constraint)
			throws StandardException {
		// TODO Auto-generated method stub
		return false;
	}

	public ConstraintDescriptor getConstraintDescriptor(TableDescriptor td,
			UUID uuid) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public ConstraintDescriptor getConstraintDescriptorById(TableDescriptor td,
			UUID uuid) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public ConstraintDescriptor getConstraintDescriptorByName(
			TableDescriptor td, SchemaDescriptor sd, String constraintName,
			boolean forUpdate) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public TableDescriptor getConstraintTableDescriptor(UUID constraintId)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public ConstraintDescriptorList getForeignKeys(UUID constraintId)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public void addConstraintDescriptor(ConstraintDescriptor descriptor,
			TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub

	}

	public void dropConstraintDescriptor(
            ConstraintDescriptor descriptor, TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub

	}

	public void dropAllConstraintDescriptors(TableDescriptor table,
			TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub

	}

	public void updateConstraintDescriptor(ConstraintDescriptor cd,
			UUID formerUUID, int[] colsToSet, TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub

	}

	public SubKeyConstraintDescriptor getSubKeyConstraint(UUID constraintId,
			int type) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public SPSDescriptor getSPSDescriptor(UUID uuid) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public SPSDescriptor getSPSDescriptor(String name, SchemaDescriptor sd)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public List getAllSPSDescriptors() throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public DataTypeDescriptor[] getSPSParams(SPSDescriptor spsd, Vector defaults)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public void addSPSDescriptor(SPSDescriptor descriptor,
			TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub

	}

	public void updateSPS(SPSDescriptor spsd, TransactionController tc,
			boolean recompile, boolean updateSYSCOLUMNS,
			boolean firstCompilation) throws StandardException {
		// TODO Auto-generated method stub

	}

	public void dropSPSDescriptor(SPSDescriptor descriptor,
			TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub

	}

	public void dropSPSDescriptor(UUID uuid, TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub

	}

	public void invalidateAllSPSPlans() throws StandardException {
		// TODO Auto-generated method stub

	}

	public TriggerDescriptor getTriggerDescriptor(UUID uuid)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public TriggerDescriptor getTriggerDescriptor(String name,
			SchemaDescriptor sd) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public GenericDescriptorList getTriggerDescriptors(TableDescriptor td)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public void updateTriggerDescriptor(TriggerDescriptor triggerd,
			UUID formerUUID, int[] colsToSet, TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub

	}

	public void dropTriggerDescriptor(TriggerDescriptor descriptor,
			TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub

	}

	public Hashtable hashAllConglomerateDescriptorsByNumber(
			TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public Hashtable hashAllTableDescriptorsByTableId(TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public ConglomerateDescriptor getConglomerateDescriptor(UUID uuid)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public ConglomerateDescriptor[] getConglomerateDescriptors(UUID uuid)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public ConglomerateDescriptor getConglomerateDescriptor(
			long conglomerateNumber) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public ConglomerateDescriptor[] getConglomerateDescriptors(
			long conglomerateNumber) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public ConglomerateDescriptor getConglomerateDescriptor(String indexName,
			SchemaDescriptor sd, boolean forUpdate) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public void dropConglomerateDescriptor(ConglomerateDescriptor conglomerate,
			TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub

	}

	public void dropAllConglomerateDescriptors(TableDescriptor td,
			TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub

	}

	public void updateConglomerateDescriptor(ConglomerateDescriptor[] cds,
			long conglomerateNumber, TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub

	}

	public void updateConglomerateDescriptor(ConglomerateDescriptor cd,
			long conglomerateNumber, TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub

	}

	public List getDependentsDescriptorList(String dependentID)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public List getProvidersDescriptorList(String providerID)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public List getAllDependencyDescriptorsList() throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public void dropStoredDependency(DependencyDescriptor dd,
			TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub

	}

	public void dropDependentsStoredDependencies(UUID dependentsUUID,
			TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub

	}

	public UUIDFactory getUUIDFactory() {
		// TODO Auto-generated method stub
		return null;
	}

    public AliasDescriptor getAliasDescriptorForUDT( TransactionController tc, DataTypeDescriptor dtd )
        throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public AliasDescriptor getAliasDescriptor(UUID uuid)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public AliasDescriptor getAliasDescriptor(String schemaID,
			String aliasName, char nameSpace) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public List getRoutineList(String schemaID, String routineName,
			char nameSpace) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public void dropAliasDescriptor(AliasDescriptor ad, TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub

	}

	public int getEngineType() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public int getCollationTypeOfSystemSchemas(){
		// TODO Auto-generated method stub
		return 0;
	}

	public int getCollationTypeOfUserSchemas(){
		// TODO Auto-generated method stub
		return 0;
	}
	
	public FileInfoDescriptor getFileInfoDescriptor(UUID id)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public FileInfoDescriptor getFileInfoDescriptor(SchemaDescriptor sd,
			String name) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public void dropFileInfoDescriptor(FileInfoDescriptor fid)
			throws StandardException {
		// TODO Auto-generated method stub

	}

	public RowLocation[] computeAutoincRowLocations(TransactionController tc,
			TableDescriptor td) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

    public void getCurrentValueAndAdvance
        ( String sequenceUUIDstring, NumberDataValue returnValue )
        throws StandardException
    {
		// TODO Auto-generated method stub
    }
    
	public RowLocation getRowLocationTemplate(LanguageConnectionContext lcc,
			TableDescriptor td) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public NumberDataValue getSetAutoincrementValue(RowLocation rl,
			TransactionController tc, boolean doUpdate,
			NumberDataValue newValue, boolean wait) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public void setAutoincrementValue(TransactionController tc, UUID tableUUID,
			String columnName, long aiValue, boolean incrementNeeded)
			throws StandardException {
		// TODO Auto-generated method stub

	}

	public List getStatisticsDescriptors(TableDescriptor td)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public void dropStatisticsDescriptors(UUID tableUUID, UUID referenceUUID,
			TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub

	}

	public DependencyManager getDependencyManager() {
		// TODO Auto-generated method stub
		return null;
	}

	public int getCacheMode() {
		// TODO Auto-generated method stub
		return 0;
	}

	public String getSystemSQLName() {
		// TODO Auto-generated method stub
		return null;
	}

	public void addDescriptor(TupleDescriptor tuple, TupleDescriptor parent,
			int catalogNumber, boolean allowsDuplicates,
			TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub

	}

	public void addDescriptorArray(TupleDescriptor[] tuple,
			TupleDescriptor parent, int catalogNumber,
			boolean allowsDuplicates, TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub

	}

	public boolean checkVersion(int majorVersion, String feature)
			throws StandardException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean addRemovePermissionsDescriptor(boolean add,
			PermissionsDescriptor perm, String grantee, TransactionController tc)
			throws StandardException {
		// TODO Auto-generated method stub
		return false;
	}

	public TablePermsDescriptor getTablePermissions(UUID tableUUID,
			String authorizationId) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public TablePermsDescriptor getTablePermissions(UUID tablePermsUUID)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public ColPermsDescriptor getColumnPermissions(UUID tableUUID,
			int privType, boolean forGrant, String authorizationId)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public ColPermsDescriptor getColumnPermissions(UUID tableUUID,
			String privTypeStr, boolean forGrant, String authorizationId)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public ColPermsDescriptor getColumnPermissions(UUID colPermsUUID)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public RoutinePermsDescriptor getRoutinePermissions(UUID routineUUID,
			String authorizationId) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public RoutinePermsDescriptor getRoutinePermissions(UUID routinePermsUUID)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public void dropDependentsStoredDependencies(UUID dependentsUUID,
			TransactionController tc, boolean wait) throws StandardException {
		// TODO Auto-generated method stub

	}

	public boolean canSupport(Properties properties) {
		return Monitor.isDesiredType(properties,
                EngineType.STORELESS_ENGINE);
	}

    public String getVTIClass(TableDescriptor td, boolean asTableFunction) throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public String getBuiltinVTIClass(TableDescriptor td, boolean asTableFunction) throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

	public void updateMetadataSPSes(TransactionController tc) throws StandardException {
		// TODO Auto-generated method stub		
	}

    public void dropSequenceDescriptor(SequenceDescriptor sequenceDescriptor,
                                       TransactionController tc) throws StandardException {
        // TODO Auto-generated method stub
    }

    public SequenceDescriptor getSequenceDescriptor(UUID uuid) throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public SequenceDescriptor getSequenceDescriptor(SchemaDescriptor sd, String sequenceName)
            throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }   

    public PermDescriptor getGenericPermissions(UUID permUUID) throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public PermDescriptor getGenericPermissions(UUID objectID, String objectType, String privilege, String granteeAuthId) 
            throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public void dropAllPermDescriptors(UUID objectID, TransactionController tc)
            throws StandardException {
        // TODO Auto-generated method stub
    }
}

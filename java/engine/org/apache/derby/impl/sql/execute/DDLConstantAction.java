/*

   Derby - Class org.apache.derby.impl.sql.execute.DDLConstantAction

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

import java.util.Iterator;
import java.util.List;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.dictionary.ColPermsDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.PermissionsDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.StatementColumnPermission;
import org.apache.derby.iapi.sql.dictionary.StatementPermission;
import org.apache.derby.iapi.sql.dictionary.StatementRoutinePermission;
import org.apache.derby.iapi.sql.dictionary.StatementSchemaPermission;
import org.apache.derby.iapi.sql.dictionary.StatementTablePermission;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.TransactionController;

/**
 * Abstract class that has actions that are across
 * all DDL actions.
 *
 */
abstract class DDLConstantAction implements ConstantAction
{
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
	 * @param activation activation
	 * @param schemaName name of the schema
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
		TransactionController tc = activation.
			getLanguageConnectionContext().getTransactionExecute();

		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, false);

		if (sd == null || sd.getUUID() == null) {
            CreateSchemaConstantAction csca
                = new CreateSchemaConstantAction(schemaName, (String) null);

			// DERBY-48: This operation creates a schema and we don't
			// want to hold a lock for SYSSCHEMAS for the duration of
			// the user transaction, so we perform the creation in a
			// nested transaction if possible.
			TransactionController useTc    = null;
			TransactionController nestedTc = null;

			try {
				nestedTc = tc.startNestedUserTransaction(false);
				useTc = nestedTc;
			} catch (StandardException e) {
				if (SanityManager.DEBUG) {
					SanityManager.THROWASSERT(
						"Unexpected: not able to start nested transaction " +
						"to auto-create schema", e);
				}
				useTc = tc;
			}

			// Try max twice: if nested transaction times out, try
			// again in the outer transaction because it may be a
			// self-lock, that is, the outer transaction may hold some
			// lock(s) that make the nested transaction attempt to set
			// a write lock time out.  Trying it again in the outer
			// transaction will then succeed. If the reason is some
			// other transaction barring us, trying again in the outer
			// transaction will possibly time out again.
			//
			// Also, if creating a nested transaction failed, only try
			// once in the outer transaction.
			while (true) {
				try {
					csca.executeConstantAction(activation, useTc);
				} catch (StandardException se) {
					if (se.getMessageId().equals(SQLState.LOCK_TIMEOUT)) {
						// We don't test for SQLState.DEADLOCK or
						// .LOCK_TIMEOUT_LOG here because a) if it is a
						// deadlock, it may be better to expose it, and b)
						// LOCK_TIMEOUT_LOG happens when the app has set
						// derby.locks.deadlockTrace=true, in which case we
						// don't want to mask the timeout.  So in both the
						// latter cases we just throw.
						if (useTc == nestedTc) {

							// clean up after use of nested transaction,
							// then try again in outer transaction
							useTc = tc;
							nestedTc.destroy();
							continue;
						}
					} else if (se.getMessageId()
							.equals(SQLState.LANG_OBJECT_ALREADY_EXISTS)) {
						// Ignore "Schema already exists". Another thread has
						// probably created it after we checked for it
						break;
					}

					// We got an non-expected exception, either in
					// the nested transaction or in the outer
					// transaction; we had better pass that on
					if (useTc == nestedTc) {
						nestedTc.destroy();
					}

					throw se;
				}
				break;
			}

			// We either succeeded or got LANG_OBJECT_ALREADY_EXISTS.
			// Clean up if we did this in a nested transaction.
			if (useTc == nestedTc) {
				nestedTc.commit();
				nestedTc.destroy();
			}

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

	protected String constructToString(
						String				statementType,
						String              objectName)
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.

		return statementType + objectName;
	}
	
	
	/**
	 *	This method saves dependencies of constraints on privileges in the  
	 *  dependency system. It gets called by CreateConstraintConstantAction.
	 *  Views and triggers and constraints run with definer's privileges. If 
	 *  one of the required privileges is revoked from the definer, the 
	 *  dependent view/trigger/constraint on that privilege will be dropped 
	 *  automatically. In order to implement this behavior, we need to save 
	 *  view/trigger/constraint dependencies on required privileges in the 
	 *  dependency system. Following method accomplishes that part of the 
	 *  equation for constraints only. The dependency collection for 
	 *  constraints is not same as for views and triggers and hence 
	 *  constraints are handled by this special method.
	 * 	Views and triggers can depend on many different kind of privileges
	 *  where as constraints only depend on REFERENCES privilege on a table.
	 *  Another difference is only one view or trigger can be defined by a
	 *  sql statement and hence all the dependencies collected for the sql
	 *  statement apply to the view or trigger in question. As for constraints,
	 *  one sql statement can defined multiple constraints and hence the 
	 *  all the privileges required by the statement are not necessarily
	 *  required by all the constraints defined by that sql statement. We need
	 *  to identify right privileges for right constraints for a given sql
	 *  statement. Because of these differences between constraints and views
	 *  (and triggers), there are 2 different methods in this class to save
	 *  their privileges in the dependency system.
	 *   
	 *  @param activation The execution environment for this constant action.
	 *  @param dependent Make this object depend on required privileges
	 *  @param refTableUUID Make sure we are looking for REFERENCES privilege 
	 * 		for right table
	 *
	 * @exception StandardException		Thrown on failure
	 */
	protected void storeConstraintDependenciesOnPrivileges(
			Activation activation, Dependent dependent, UUID refTableUUID)
	throws StandardException
	{
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		
		//If the Database Owner is creating this constraint, then no need to 
		//collect any privilege dependencies because the Database Owner can   
		//access any objects without any restrictions
		if (!(lcc.getAuthorizationId().equals(dd.getAuthorizationDatabaseOwner())))
		{
			PermissionsDescriptor permDesc;
			//Now, it is time to add into dependency system, constraint's 
			//dependency on REFERENCES privilege. If the REFERENCES privilege is 
			//revoked from the constraint owner, the constraint will get 
			//dropped automatically.
			List requiredPermissionsList = activation.getPreparedStatement().getRequiredPermissionsList();
			if (requiredPermissionsList != null && ! requiredPermissionsList.isEmpty())
			{
				for(Iterator iter = requiredPermissionsList.iterator();iter.hasNext();)
				{
					StatementPermission statPerm = (StatementPermission) iter.next();
					//First check if we are dealing with a Table or 
					//Column level privilege. All the other privileges
					//are not required for a foreign key constraint.
					if (statPerm instanceof StatementTablePermission)
					{//It is a table/column level privilege
						StatementTablePermission statementTablePermission = 
							(StatementTablePermission) statPerm;
						//Check if we are dealing with REFERENCES privilege.
						//If not, move on to the next privilege in the
						//required privileges list
						if (statementTablePermission.getPrivType() != Authorizer.REFERENCES_PRIV)
							continue;
						//Next check is this REFERENCES privilege is 
						//on the same table as referenced by the foreign
						//key constraint? If not, move on to the next
						//privilege in the required privileges list
						if (!statementTablePermission.getTableUUID().equals(refTableUUID))
							continue;
					} else if (statPerm instanceof StatementSchemaPermission) { 
						continue;
					}
					//We know that we are working with a REFERENCES 
					//privilege. Find all the PermissionDescriptors for
					//this privilege and make constraint depend on it
					//through dependency manager.
					//The REFERENCES privilege could be defined at the
					//table level or it could be defined at individual
					//column levels. In addition, individual column
					//REFERENCES privilege could be available at the
					//user level or PUBLIC level.
					permDesc = statPerm.getPermissionDescriptor(lcc.getAuthorizationId(), dd);				
					if (permDesc == null) 
					{
						//No REFERENCES privilege exists for given 
						//authorizer at table or column level.
						//REFERENCES privilege has to exist at at PUBLIC level
						permDesc = statPerm.getPermissionDescriptor(Authorizer.PUBLIC_AUTHORIZATION_ID, dd);
						if (permDesc != null &&
							!(permDesc.checkOwner(lcc.getAuthorizationId())))
							dm.addDependency(dependent, permDesc, lcc.getContextManager());
					} else 
						//if the object on which permission is required is owned by the
						//same user as the current user, then no need to keep that
						//object's privilege dependency in the dependency system
					if (!(permDesc.checkOwner(lcc.getAuthorizationId())))
					{
						dm.addDependency(dependent, permDesc, lcc.getContextManager());
						if (permDesc instanceof ColPermsDescriptor)
						{
							//The if statement above means we found a
							//REFERENCES privilege at column level for
							//the given authorizer. If this privilege
							//doesn't cover all the column , then there 
							//has to exisit REFERENCES for the remaining
							//columns at PUBLIC level. Get that permission
							//descriptor and save it in dependency system
							StatementColumnPermission statementColumnPermission = (StatementColumnPermission) statPerm;
							permDesc = statementColumnPermission.getPUBLIClevelColPermsDescriptor(lcc.getAuthorizationId(), dd);
							//Following if checks if some column level privileges
							//exist only at public level. If so, then the public
							//level column privilege dependency is added
							//into the dependency system
							if (permDesc != null)
								dm.addDependency(dependent, permDesc, lcc.getContextManager());	           																
						}
					}
					//We have found the REFERENCES privilege for all the
					//columns in foreign key constraint and we don't 
					//need to go through the rest of the privileges
					//for this sql statement.
					break;																										
				}
			}
		}
		
	}	
	
	/**
	 *	This method saves dependencies of views and triggers on privileges in  
	 *  the dependency system. It gets called by CreateViewConstantAction
	 *  and CreateTriggerConstantAction. Views and triggers and constraints
	 *  run with definer's privileges. If one of the required privileges is
	 *  revoked from the definer, the dependent view/trigger/constraint on
	 *  that privilege will be dropped automatically. In order to implement 
	 *  this behavior, we need to save view/trigger/constraint dependencies 
	 *  on required privileges in the dependency system. Following method 
	 *  accomplishes that part of the equation for views and triggers. The
	 *  dependency collection for constraints is not same as for views and
	 *  triggers and hence constraints are not covered by this method.
	 *  Views and triggers can depend on many different kind of privileges
	 *  where as constraints only depend on REFERENCES privilege on a table.
	 *  Another difference is only one view or trigger can be defined by a
	 *  sql statement and hence all the dependencies collected for the sql
	 *  statement apply to the view or trigger in question. As for constraints,
	 *  one sql statement can defined multiple constraints and hence the 
	 *  all the privileges required by the statement are not necessarily
	 *  required by all the constraints defined by that sql statement. We need
	 *  to identify right privileges for right constraints for a given sql
	 *  statement. Because of these differences between constraints and views
	 *  (and triggers), there are 2 different methods in this class to save
	 *  their privileges in the dependency system.  
	 *
	 *  @param activation The execution environment for this constant action.
	 *  @param dependent Make this object depend on required privileges
	 *
	 * @exception StandardException		Thrown on failure
	 */
	protected void storeViewTriggerDependenciesOnPrivileges(
			Activation activation, Dependent dependent)
	throws StandardException
	{
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		
		//If the Database Owner is creating this view/triiger, then no need to  
		//collect any privilege dependencies because the Database Owner can  
		//access any objects without any restrictions
		if (!(lcc.getAuthorizationId().equals(dd.getAuthorizationDatabaseOwner())))
		{
			PermissionsDescriptor permDesc;
			List requiredPermissionsList = activation.getPreparedStatement().getRequiredPermissionsList();
			if (requiredPermissionsList != null && ! requiredPermissionsList.isEmpty())
			{
				for(Iterator iter = requiredPermissionsList.iterator();iter.hasNext();)
				{
					StatementPermission statPerm = (StatementPermission) iter.next();
					//The schema ownership permission just needs to be checked 
					//at object creation time, to see if the object creator has 
					//permissions to create the object in the specified schema. 
					//But we don't need to add schema permission to list of 
					//permissions that the object is dependent on once it is 
					//created.
					if (statPerm instanceof StatementSchemaPermission)
						continue;
					//See if we can find the required privilege for given authorizer?
					permDesc = statPerm.getPermissionDescriptor(lcc.getAuthorizationId(), dd);				
					if (permDesc == null)//privilege not found for given authorizer 
					{
						//The if condition above means that required privilege does 
						//not exist at the user level. The privilege has to exist at 
						//PUBLIC level.
						permDesc = statPerm.getPermissionDescriptor(Authorizer.PUBLIC_AUTHORIZATION_ID, dd);
						//If the user accessing the object is the owner of that 
						//object, then no privilege tracking is needed for the
						//owner.
						if (permDesc != null &&
							!(permDesc.checkOwner(lcc.getAuthorizationId())))
							dm.addDependency(dependent, permDesc, lcc.getContextManager());
						continue;
					}
					//if the object on which permission is required is owned by the
					//same user as the current user, then no need to keep that
					//object's privilege dependency in the dependency system
					if (!(permDesc.checkOwner(lcc.getAuthorizationId())))
					{
						dm.addDependency(dependent, permDesc, lcc.getContextManager());	           							
						if (permDesc instanceof ColPermsDescriptor)
						{
							//For a given table, the table owner can give privileges
							//on some columns at individual user level and privileges
							//on some columns at PUBLIC level. Hence, when looking for
							//column level privileges, we need to look both at user
							//level as well as PUBLIC level(only if user level column
							//privileges do not cover all the columns accessed by this
							//object). We have finished adding dependency for user level 
							//columns, now we are checking if some required column 
							//level privileges are at PUBLIC level.
							//A specific eg of a view
							//user1
							//create table t11(c11 int, c12 int);
							//grant select(c11) on t1 to user2;
							//grant select(c12) on t1 to PUBLIC;
							//user2
							//create view v1 as select c11 from user1.t11 where c12=2;
							//For the view above, there are 2 column level privilege 
							//depencies, one for column c11 which exists directly
							//for user2 and one for column c12 which exists at PUBLIC level.
							StatementColumnPermission statementColumnPermission = (StatementColumnPermission) statPerm;
							permDesc = statementColumnPermission.getPUBLIClevelColPermsDescriptor(lcc.getAuthorizationId(), dd);
							//Following if checks if some column level privileges
							//exist only at public level. If so, then the public
							//level column privilege dependency of view is added
							//into dependency system.
							if (permDesc != null)
								dm.addDependency(dependent, permDesc, lcc.getContextManager());	           							
						}
					}
				}
			}
			
		}
	}
}


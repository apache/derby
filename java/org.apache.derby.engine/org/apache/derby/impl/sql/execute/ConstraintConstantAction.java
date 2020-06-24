/*

   Derby - Class org.apache.derby.impl.sql.execute.ConstraintConstantAction

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

import java.util.ArrayList;
import java.util.List;
import org.apache.derby.catalog.UUID;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.PreparedStatement;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.GroupFetchScanController;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.impl.sql.execute.DeferredConstraintsMemory.CheckInfo;
import org.apache.derby.impl.store.access.heap.HeapRowLocation;
import org.apache.derby.shared.common.sanity.SanityManager;
/**
 *	This class  describes actions that are ALWAYS performed for a
 *	constraint creation at Execution time.
 *
 *	@version 0.1
 */

public abstract class ConstraintConstantAction extends DDLSingleTableConstantAction 
{

	protected	String			constraintName;
	protected	int				constraintType;
	protected	String			tableName;
	protected	String			schemaName;
	protected	UUID			schemaId;
	protected  IndexConstantAction indexAction;
    protected   UUID            constraintId;

	// CONSTRUCTORS
	/**
	 *	Make one of these puppies.
	 *
	 *  @param constraintName	Constraint name.
	 *  @param constraintType	Constraint type.
	 *  @param tableName		Table name.
	 *  @param tableId			UUID of table.
	 *  @param schemaName		schema that table and constraint lives in.
	 *  @param indexAction		IndexConstantAction for constraint (if necessary)
	 *  RESOLVE - the next parameter should go away once we use UUIDs
	 *			  (Generated constraint names will be based off of uuids)
	 */
	ConstraintConstantAction(
		               String	constraintName,
					   int		constraintType,
		               String	tableName,
					   UUID		tableId,
					   String	schemaName,
					   IndexConstantAction indexAction)
	{
		super(tableId);
		this.constraintName = constraintName;
		this.constraintType = constraintType;
		this.tableName = tableName;
		this.indexAction = indexAction;
		this.schemaName = schemaName;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(schemaName != null, "Constraint schema name is null");
		}
	}

	// Class implementation

	/**
	 * Get the constraint type.
	 *
	 * @return The constraint type
	 */
	public	int getConstraintType()
	{
		return constraintType;
	}

	/**
	  *	Get the constraint name
	  *
	  *	@return	the constraint name
	  */
    public	String	getConstraintName() { return constraintName; }

    /**
     * Get the constraint id of the constraint
     * @return constraint id
     */
    public UUID getConstraintId() {
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        return constraintId;
    }


	/**
	  *	Get the associated index constant action.
	  *
	  *	@return	the constant action for the backing index
	  */
    public	IndexConstantAction	getIndexAction() { return indexAction; }

	/**
	 * Make sure that the foreign key constraint is valid
	 * with the existing data in the target table.  Open
	 * the table, if there aren't any rows, ok.  If there
	 * are rows, open a scan on the referenced key with
	 * table locking at level 2.  Pass in the scans to
	 * the BulkRIChecker.  If any rows fail, barf.
	 *
	 * @param	tc		transaction controller
	 * @param	dd		data dictionary
	 * @param	fk		foreign key constraint
	 * @param	refcd	referenced key
	 * @param 	indexTemplateRow	index template row
	 *
	 * @exception StandardException on error
	 */
	static void validateFKConstraint
	(
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        Activation                          activation,
		TransactionController				tc,
		DataDictionary						dd,
		ForeignKeyConstraintDescriptor		fk,
		ReferencedKeyConstraintDescriptor	refcd,
		ExecRow 							indexTemplateRow 
	)
		throws StandardException
	{

		GroupFetchScanController refScan = null;

		GroupFetchScanController fkScan = 
            tc.openGroupFetchScan(
                fk.getIndexConglomerateDescriptor(dd).getConglomerateNumber(),
                false,                       			// hold 
                0, 										// read only
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                TransactionController.MODE_TABLE,       // already locked
                TransactionController.ISOLATION_READ_COMMITTED,
                (FormatableBitSet)null,                 // retrieve all fields
                (DataValueDescriptor[])null,    	    // startKeyValue
                ScanController.GE,            			// startSearchOp
                null,                         			// qualifier
                (DataValueDescriptor[])null,  			// stopKeyValue
                ScanController.GT             			// stopSearchOp 
                );

		try
		{
			/*
			** If we have no rows, then we are ok.  This will 
			** catch the CREATE TABLE T (x int references P) case
			** (as well as an ALTER TABLE ADD CONSTRAINT where there
			** are no rows in the target table).
			*/	
			if (!fkScan.next())
			{
				fkScan.close();
				return;
			}

			fkScan.reopenScan(
					(DataValueDescriptor[])null,    		// startKeyValue
					ScanController.GE,            			// startSearchOp
					null,                         			// qualifier
					(DataValueDescriptor[])null,  			// stopKeyValue
					ScanController.GT             			// stopSearchOp 
					);

			/*
			** Make sure each row in the new fk has a matching
			** referenced key.  No need to get any special locking
			** on the referenced table because it cannot delete
			** any keys we match because it will block on the table
			** lock on the fk table (we have an ex tab lock on
			** the target table of this ALTER TABLE command).
			** Note that we are doing row locking on the referenced
			** table.  We could speed things up and get table locking
			** because we are likely to be hitting a lot of rows
			** in the referenced table, but we are going to err
			** on the side of concurrency here.
			*/
			refScan = 
                tc.openGroupFetchScan(
					refcd.getIndexConglomerateDescriptor(dd).getConglomerateNumber(),
                        false,                       	// hold 
                        0, 								// read only
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                        TransactionController.MODE_RECORD,
                        TransactionController.ISOLATION_READ_COMMITTED,
                                              // read committed is good enough
                        (FormatableBitSet)null,         // retrieve all fields
                        (DataValueDescriptor[])null,    // startKeyValue
                        ScanController.GE,            	// startSearchOp
                        null,                         	// qualifier
                        (DataValueDescriptor[])null,  	// stopKeyValue
                        ScanController.GT             	// stopSearchOp 
                        );

//IC see: https://issues.apache.org/jira/browse/DERBY-532
            RIBulkChecker riChecker = new RIBulkChecker(
                    activation,
                    refScan,
                    fkScan,
                    indexTemplateRow,
                    true,               // fail on 1st failure
                    (ConglomerateController)null,
                    (ExecRow)null,
                    fk.getTableDescriptor().getSchemaName(),
                    fk.getTableDescriptor().getName(),
                    fk.getUUID(),
                    fk.deferrable(),
                    fk.getIndexConglomerateDescriptor(dd).
                        getConglomerateNumber(),
                    refcd.getIndexConglomerateDescriptor(dd).
                        getConglomerateNumber());

			int numFailures = riChecker.doCheck();
			if (numFailures > 0)
			{
				StandardException se = StandardException.newException(SQLState.LANG_ADD_FK_CONSTRAINT_VIOLATION, 
									fk.getConstraintName(), 
									fk.getTableDescriptor().getName());
				throw se;
			}
		}
		finally
		{
			if (fkScan != null)
			{
				fkScan.close();
				fkScan = null;
			}
			if (refScan != null)
			{
				refScan.close();
				refScan = null;
			}
		}
	}

	/**
	 * Evaluate a check constraint or not null column constraint.  
	 * Generate a query of the
	 * form SELECT COUNT(*) FROM t where NOT(<check constraint>)
	 * and run it by compiling and executing it.   Will
	 * work ok if the table is empty and query returns null.
	 *
	 * @param constraintName	constraint name
	 * @param constraintText	constraint text
     * @param constraintId      constraint id
	 * @param td				referenced table
	 * @param lcc				the language connection context
	 * @param isCheckConstraint	the constraint is a check constraint
     * @param isInitiallyDeferred {@code true} if the constraint is
     *                          initially deferred
     *
	 * @return true if null constraint passes, false otherwise
	 *
	 * @exception StandardException if check constraint fails
	 */
	 static boolean validateConstraint
	(
		String							constraintName,
		String							constraintText,
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        UUID                            constraintId,
		TableDescriptor					td,
		LanguageConnectionContext		lcc,
        boolean                         isCheckConstraint,
        boolean                         isInitiallyDeferred
	)
		throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        StringBuilder checkStmt = new StringBuilder();
		/* should not use select sum(not(<check-predicate>) ? 1: 0) because
		 * that would generate much more complicated code and may exceed Java
		 * limits if we have a large number of check constraints, beetle 4347
		 */
		checkStmt.append("SELECT COUNT(*) FROM ");
		checkStmt.append(td.getQualifiedName());
		checkStmt.append(" WHERE NOT(");
		checkStmt.append(constraintText);
		checkStmt.append(")");
	
		ResultSet rs = null;
		try
		{
			PreparedStatement ps = lcc.prepareInternalStatement(checkStmt.toString());
//IC see: https://issues.apache.org/jira/browse/DERBY-231

            // This is a substatement; for now, we do not set any timeout
            // for it. We might change this behaviour later, by linking
            // timeout to its parent statement's timeout settings.
//IC see: https://issues.apache.org/jira/browse/DERBY-3897
			rs = ps.executeSubStatement(lcc, false, 0L);
			ExecRow row = rs.getNextRow();
			if (SanityManager.DEBUG)
			{
				if (row == null)
				{
					SanityManager.THROWASSERT("did not get any rows back from query: "+checkStmt.toString());
				}
			}

			Number value = ((Number)((NumberDataValue)row.getRowArray()[0]).getObject());
			/*
			** Value may be null if there are no rows in the
			** table.
			*/
			if ((value != null) && (value.longValue() != 0))
			{	
                // The query yielded a valid value > 0, so we must conclude the
                // check constraint is violated.
                if (isCheckConstraint) {
                    if (isInitiallyDeferred) {
                        // Remember the violation
                        List<UUID> violatingConstraints = new ArrayList<UUID>();
                        violatingConstraints.add(constraintId);

                        // FIXME: We don't know the row locations of the
                        // violating rows, so for now, just pretend we know one,
                        // then invalidate the row location information forcing
                        // full table check at validation time
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                        CheckInfo newCi[] = new CheckInfo[1];
                        DeferredConstraintsMemory.rememberCheckViolations(
                                lcc,
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
                                td.getObjectID(),
                                td.getSchemaName(),
                                td.getName(),
                                null,
                                violatingConstraints,
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                                new HeapRowLocation() /* dummy */,
                                newCi);
                        newCi[0].setInvalidatedRowLocations();

                    } else {
                        throw StandardException.newException(
                                SQLState.LANG_ADD_CHECK_CONSTRAINT_FAILED,
                                constraintName,
                                td.getQualifiedName(),
                                value.toString());
                    }
                }
				/*
				 * for not null constraint violations exception will be thrown in caller
				 * check constraint will not get here since exception is thrown
				 * above
				 */
				return false;
			}
		}
		finally
		{
			if (rs != null)
			{
				rs.close();
			}
		}
		return true;
	}
}

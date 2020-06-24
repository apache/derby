/*

   Derby - Class org.apache.derby.impl.sql.execute.ForeignKeyRIChecker

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

import org.apache.derby.catalog.UUID;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.StatementUtil;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * A Referential Integrity checker for a foreign
 * key constraint.  It makes sure the foreign key is
 * intact.  This is used for a change to a foreign
 * key column.  see ReferencedKeyRIChecker for the code
 * that validates changes to referenced keys.
 */
public class ForeignKeyRIChecker extends GenericRIChecker
{
	/**
     * @param lcc       the language connection context
	 * @param tc		the xact controller
	 * @param fkinfo	the foreign key information 
	 *
	 * @exception StandardException		Thrown on failure
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-532
    ForeignKeyRIChecker(
        LanguageConnectionContext lcc,
        TransactionController tc,
        FKInfo fkinfo) throws StandardException
	{
        super(lcc, tc, fkinfo);

		if (SanityManager.DEBUG)
		{
			if (fkInfo.type != FKInfo.FOREIGN_KEY)
			{
				SanityManager.THROWASSERT("invalid type "+fkInfo.type+" for a ForeignKeyRIChecker");
			}
		} 
	}

	/**
	 * Check that the row either has a null column(s), or
	 * corresponds to a row in the referenced key.
	 * <p> 
	 * If the referenced key is found, then it is locked
	 * when this method returns.  The lock is held until
	 * the next call to doCheck() or close().
	 *
     * @param a     the activation
	 * @param row	the row to check
     * @param restrictCheckOnly
     *              {@code true} if the check is relevant only for RESTRICTED
     *              referential action.
     * @param deferredRowReq
     *              dummy (interface obligation only)
	 *
     * @exception StandardException on unexpected error, or
	 *		on a foreign key violation
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-532
    void doCheck(Activation a,
                 ExecRow row,
                 boolean restrictCheckOnly,
                 int deferredRowReq) throws StandardException
	{

		if(restrictCheckOnly) //RESTRICT rule checks are not valid here.
			return; 

		/*
		** If any of the columns are null, then the
		** check always succeeds.
		*/
		if (isAnyFieldNull(row))
		{
			return;
		}

		/*
		** Otherwise, we had better find this row in the
		** referenced key
		*/
		ScanController scan = getScanController(fkInfo.refConglomNumber, refScoci, refDcoci, row);
		if (!scan.next())
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-532
            final UUID fkId = fkInfo.fkIds[0];
            close();

            if (fkInfo.deferrable[0] &&
                lcc.isEffectivelyDeferred(lcc.getCurrentSQLSessionContext(a),
                        fkId)) {
                deferredRowsHashTable =
                        DeferredConstraintsMemory.rememberFKViolation(
                                lcc,
                                deferredRowsHashTable,
                                fkInfo.fkIds[0],
                                indexQualifierRow.getRowArray(),
                                fkInfo.schemaName,
                                fkInfo.tableName);
            } else {
                StandardException se = StandardException.newException(
                        SQLState.LANG_FK_VIOLATION,
                        fkInfo.fkConstraintNames[0],
                        fkInfo.tableName,
                        StatementUtil.typeName(fkInfo.stmtType),
                        RowUtil.toString(row, fkInfo.colArray));
                throw se;
            }

		}
		
		/*
		** If we found the row, we are currently positioned on
		** the row when we leave this method.  So we hold the
		** lock on the referenced key, which is very important.
		*/	
	}

	/**
	 * Get the isolation level for the scan for
	 * the RI check.
	 *
	 * NOTE: The level will eventually be instantaneous
     * locking once the implementation changes.
	 *
	 * @return The isolation level for the scan for
	 * the RI check.
	 */
	int getRICheckIsolationLevel()
	{
		return TransactionController.ISOLATION_READ_COMMITTED;
	}
}

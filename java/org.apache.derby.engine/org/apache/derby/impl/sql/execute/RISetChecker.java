/*

   Derby - Class org.apache.derby.impl.sql.execute.RISetChecker

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

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * Checks a set or referential integrity constraints.  Used
 * to shield the caller from ReferencedKeyRIChecker and
 * ForeignKeyRICheckers.
 */
public class RISetChecker
{
	private GenericRIChecker[] 	checkers;
    LanguageConnectionContext lcc;
//IC see: https://issues.apache.org/jira/browse/DERBY-532

	/**
     * @param lcc       the language connection context
	 * @param tc		the xact controller
	 * @param fkInfo	the foreign key information 
	 *
	 * @exception StandardException		Thrown on failure
	 */
    public RISetChecker(LanguageConnectionContext lcc,
                        TransactionController tc,
                        FKInfo fkInfo[]) throws StandardException
	{
		if (fkInfo == null)
		{
			return;
		}

		checkers = new GenericRIChecker[fkInfo.length];
        this.lcc = lcc;
//IC see: https://issues.apache.org/jira/browse/DERBY-532

		for (int i = 0; i < fkInfo.length; i++)
		{
			checkers[i] = (fkInfo[i].type == FKInfo.FOREIGN_KEY) ?
                (GenericRIChecker)new ForeignKeyRIChecker(
                    lcc, tc, fkInfo[i]) :
                (GenericRIChecker)new ReferencedKeyRIChecker(
                    lcc, tc, fkInfo[i]);
		}
	}

	/**
	 * Do any work needed to reopen our ri checkers
	 * for another round of checks.  Must do a close()
	 * first.
	 *
	 * @exception StandardException on error
	 */
	void reopen() throws StandardException
	{
		// currently a noop
	}

	/**
	 * Check that there are no referenced primary keys in
	 * the passed in row.  So for each foreign key that
	 * references a primary key constraint, make sure
	 * that there is no row that matches the values in
	 * the passed in row.
	 *
     * @param a     The activation
     * @param row   The row to check
     * @param restrictCheckOnly
     *              {@code true} if the check is relevant only for RESTRICTED
     *              referential action.
     * @param deferredRowReq
     *              For referenced keys: The required number of duplicates that
     *              need to be present. Only used if {@code postCheck==false}.
	 *
	 * @exception StandardException on unexpected error, or
	 *		on a primary/unique key violation
	 */
    public void doPKCheck(Activation a,
                          ExecRow row,
                          boolean restrictCheckOnly,
                          int deferredRowReq) throws StandardException
	{
		if (checkers == null)
			return;

//IC see: https://issues.apache.org/jira/browse/DERBY-6576
        for (GenericRIChecker checker : checkers) {
            if (checker instanceof ReferencedKeyRIChecker) {
                checker.doCheck(a,
                                row,
                                restrictCheckOnly,
                                deferredRowReq);
            }
        }
	}

    public void postCheck() throws StandardException
    {
        if (checkers == null) {
            return;
        }

        for (int i = 0; i < checkers.length; i++) {
            postCheck(i);
        }
    }

    public void postCheck(int index) throws StandardException
    {
        if (checkers == null) {
            return;
        }

        if (checkers[index] instanceof ReferencedKeyRIChecker) {
            ((ReferencedKeyRIChecker)checkers[index]).postCheck();
        }
    }

    /**
	 * Check that everything in the row is ok, i.e.
	 * that there are no foreign keys in the passed
	 * in row that have invalid values.
	 *
     * @param a     the activation
	 * @param row	the row to check
	 *
	 * @exception StandardException on unexpected error, or
	 *		on a primary/unique key violation
	 */
    public void doFKCheck(Activation a, ExecRow row) throws StandardException
	{
		if (checkers == null)
			return;

		for (int i = 0; i < checkers.length; i++)
		{
			if (checkers[i] instanceof ForeignKeyRIChecker)
			{
//IC see: https://issues.apache.org/jira/browse/DERBY-6576
                checkers[i].doCheck(a, row, false, 0);
			}
		}
	}

	/**
	 * Execute the specific RI check on the passed in row.
	 *
     * @param a     the activation
	 * @param index	index into fkInfo
     * @param row   the row to check
     * @param restrictCheckOnly
     *              {@code true} if the check is relevant only for RESTRICTED
     *              referential action.
     * @param deferredRowReq
     *              For referenced keys: the required number of duplicates that
     *              need to be present. Only used if {@code postCheck==false}.
	 *
	 * @exception StandardException on unexpected error, or
	 *		on a primary/unique key violation
	 */
    public void doRICheck(Activation a,
                          int index,
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                          ExecRow row,
                          boolean restrictCheckOnly,
                          int deferredRowReq) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			if (checkers == null)
			{
				SanityManager.THROWASSERT("no checkers, how can i execute checker "+index);
			}

			if (index >= checkers.length)
			{
				SanityManager.THROWASSERT("there are only "+
					checkers.length+" checkers, "+index+" is invalid");
			}
		}

        checkers[index].doCheck(
//IC see: https://issues.apache.org/jira/browse/DERBY-6576
            a, row, restrictCheckOnly, deferredRowReq);
	}

	/**
	 * clean up
	 *
	 * @exception StandardException on error
	 */
	public void close() throws StandardException
	{
		if (checkers == null)
			return;

		for (int i = 0; i < checkers.length; i++)
		{
			checkers[i].close();
		}
	}	
}


/*

   Derby - Class org.apache.derby.impl.sql.execute.ReferencedKeyRIChecker

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

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.StatementUtil;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.sql.StatementType;

/**
 * A Referential Integrity checker for a change
 * to a referenced key (primary or unique).   Makes
 * sure that all the referenced key row is not
 * referenced by any of its foreign keys.  see 
 * ForeignKeyRIChecker for the code that validates
 * changes to foreign keys.
 */
public class ReferencedKeyRIChecker extends GenericRIChecker
{
	/**
	 * @param tc		the xact controller
	 * @param fkInfo	the foreign key information 
	 *
	 * @exception StandardException		Thrown on failure
	 */
	ReferencedKeyRIChecker(TransactionController tc, FKInfo fkinfo)
		throws StandardException
	{
		super(tc, fkinfo);

		if (SanityManager.DEBUG)
		{
			if (fkInfo.type != FKInfo.REFERENCED_KEY)
			{
				SanityManager.THROWASSERT("invalid type "+fkInfo.type+
					" for a ReferencedKeyRIChecker");
			}
		} 
	}

	/**
	 * Check that the row either has a null column(s), or
	 * has no corresponding foreign keys.
	 * <p> 
	 * If a foreign key is found, an exception is thrown.
	 * If not, the scan is closed.
	 *
	 * @param row	the row to check
	 *
	 * @exception StandardException on unexpected error, or
	 *		on a primary/unique key violation
	 */
	void doCheck(ExecRow row, boolean restrictCheckOnly) throws StandardException
	{
		/*
		** If any of the columns are null, then the
		** check always succeeds.
		*/
		if (isAnyFieldNull(row))
		{
			return;
		}

		/*
		** Otherwise, should be no rows found.
	 	** Check each conglomerate.
		*/
		ScanController scan;

		for (int i = 0; i < fkInfo.fkConglomNumbers.length; i++)
		{
			
			if(restrictCheckOnly)
			{
				if(fkInfo.raRules[i] != StatementType.RA_RESTRICT)
					continue;
			}

			scan = getScanController(fkInfo.fkConglomNumbers[i], fkScocis[i], fkDcocis[i], row);
			if (scan.next())
			{
				close();
				StandardException se = StandardException.newException(SQLState.LANG_FK_VIOLATION, fkInfo.fkConstraintNames[i],
										fkInfo.tableName,
										StatementUtil.typeName(fkInfo.stmtType),
										RowUtil.toString(row, fkInfo.colArray));

				throw se;
			}
			/*
			** Move off of the current row to release any locks.
			*/
			scan.next();
		}
	}
}






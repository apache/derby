/*

   Derby - Class org.apache.derby.impl.sql.execute.RIBulkChecker

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
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.LanguageProperties;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.GroupFetchScanController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;

/**
 * Do a merge run comparing all the foreign keys from the
 * foreign key conglomerate against the referenced keys
 * from the primary key conglomerate.  The scanControllers
 * are passed in by the caller (caller controls locking on
 * said conglomerates).
 * <p>
 * The comparision is done via a merge.  Consequently,
 * it is imperative that the scans are on keyed conglomerates
 * (indexes) and that the referencedKeyScan is a unique scan.
 * <p>
 * Performance is no worse than N + M where N is foreign key 
 * rows and M is primary key rows.  
 * <p>
 * Bulk fetch is used to further speed performance.  The
 * fetch size is LanguageProperties.BULK_FETCH_DEFAULT
 *
 * @see LanguageProperties
 */
public class RIBulkChecker 
{
	private static final int EQUAL = 0;
	private static final int GREATER_THAN = 1;
	private static final int LESS_THAN = -1;

    private final long            fkCID;
    private final long            pkCID;
    private final String          schemaName;
    private final String          tableName;
    private final UUID            constraintId;
    private BackingStoreHashtable deferredRowsHashTable; // cached value
    private final LanguageConnectionContext lcc;
    private final boolean deferred; // constraint is deferred

    private GroupFetchScanController    referencedKeyScan;
	private DataValueDescriptor[][]		referencedKeyRowArray;
	private GroupFetchScanController	foreignKeyScan;
	private DataValueDescriptor[][]		foreignKeyRowArray;
	private ConglomerateController	unreferencedCC;
	private int 			failedCounter;
	private boolean			quitOnFirstFailure;
	private	int				numColumns;
	private	int				currRefRowIndex;
	private	int				currFKRowIndex;
	private int				lastRefRowIndex;
	private int				lastFKRowIndex;
	private ExecRow			firstRowToFail;
    /**
     * Create a RIBulkChecker
	 * 
     * @param a                     the activation
	 * @param referencedKeyScan		scan of the referenced key's
	 *								backing index.  must be unique
	 * @param foreignKeyScan		scan of the foreign key's
	 *								backing index
	 * @param templateRow			a template row for the indexes.
	 *								Will be cloned when it is used.
	 *								Must be a full index row.
	 * @param quitOnFirstFailure	quit on first unreferenced key
	 * @param unreferencedCC	put unreferenced keys here
	 * @param firstRowToFail		the first row that fails the constraint
	 *								is copied to this, if non-null
     * @param schemaName            schema name of the table we insert into
     * @param tableName             table name of the table we insert into
     * @param constraintId          constraint id of the foreign constraint
     * @param deferrable            {@code true} if the constraint is deferrable
     * @param fkCID                 conglomerate id of the foreign key
     *                              supporting index
     * @param pkCID                 conglomerate id of the referenced primary
     *                              key or unique index.
     * @throws org.apache.derby.shared.common.error.StandardException
     *
     */
    public RIBulkChecker
	(
//IC see: https://issues.apache.org/jira/browse/DERBY-532
            Activation                  a,
			GroupFetchScanController    referencedKeyScan,
			GroupFetchScanController	foreignKeyScan,
			ExecRow					    templateRow,
			boolean					    quitOnFirstFailure,
			ConglomerateController	    unreferencedCC,
            ExecRow                     firstRowToFail,
            String                      schemaName,
            String                      tableName,
            UUID                        constraintId,
            boolean                     deferrable,
            long                        fkCID,
            long                        pkCID
    ) throws StandardException
	{
        this.referencedKeyScan = referencedKeyScan;
		this.foreignKeyScan = foreignKeyScan;
		this.quitOnFirstFailure = quitOnFirstFailure;
		this.unreferencedCC = unreferencedCC;
		this.firstRowToFail = firstRowToFail;
        this.constraintId = constraintId;
        this.fkCID = fkCID;
        this.pkCID = pkCID;
        this.schemaName = schemaName;
        this.tableName = tableName;
		foreignKeyRowArray		= new DataValueDescriptor[LanguageProperties.BULK_FETCH_DEFAULT_INT][];
		foreignKeyRowArray[0]	= templateRow.getRowArrayClone();
		referencedKeyRowArray	= new DataValueDescriptor[LanguageProperties.BULK_FETCH_DEFAULT_INT][];
		referencedKeyRowArray[0]= templateRow.getRowArrayClone();
		failedCounter = 0;
		numColumns = templateRow.getRowArray().length - 1;
		currFKRowIndex = -1; 
		currRefRowIndex = -1; 

//IC see: https://issues.apache.org/jira/browse/DERBY-532
        this.lcc = a.getLanguageConnectionContext();
        this.deferred = deferrable && lcc.isEffectivelyDeferred(
                lcc.getCurrentSQLSessionContext(a), constraintId);
	}

	/**
     * Perform the check. If deferred constraint mode, the numbers of failed
     * rows returned will be always be 0 (but any violating keys will have been
     * saved for later checking).
	 *
	 * @return the number of failed rows
	 *
	 * @exception StandardException on error
	 */
	public int doCheck()
		throws StandardException
	{
		DataValueDescriptor[] foreignKey;
		DataValueDescriptor[] referencedKey;

		int compareResult;

		referencedKey = getNextRef();

		/*
		** 	For each foreign key
	 	**
		**		while (fk > pk)
		**			next pk
		**			if no next pk
		**				failed
		**
		**		if fk != pk
		**			failed
		*/	
		while ((foreignKey = getNextFK()) != null)
		{
			/*
			** If all of the foreign key is not null and there are no
			** referenced keys, then everything fails
			** ANSI standard says the referential constraint is
			** satisfied if either at least one of the values of the
			** referencing columns(i.e., foreign key) is null or the
			** value of each referencing column is equal to the 
			** corresponding referenced column in the referenced table
			*/
			if (!anyNull(foreignKey) && referencedKey == null)
			{
				do
				{
					failure(foreignKey);
					if (quitOnFirstFailure)
					{
                            return failedCounter;
					}
				} while ((foreignKey = getNextFK()) != null);
				return failedCounter;
			}

			while ((compareResult = greaterThan(foreignKey, referencedKey)) == GREATER_THAN)
			{
				if ((referencedKey = getNextRef()) == null)
				{
					do
					{
						failure(foreignKey);
						if (quitOnFirstFailure)
						{
//IC see: https://issues.apache.org/jira/browse/DERBY-532
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                            return failedCounter;
						}
					} while ((foreignKey = getNextFK()) != null);
					return failedCounter;
				}
			}

			if (compareResult != EQUAL)
			{
				failure(foreignKey);
				if (quitOnFirstFailure)
				{
                    return failedCounter;
				}
			}	
		}
//IC see: https://issues.apache.org/jira/browse/DERBY-532
		return failedCounter;
	}


	/*
	 * Use bulk fetch to get the next set of rows,
	 * or read the next out of our internal array.
	 */
	private DataValueDescriptor[] getNextFK()
		throws StandardException
	{
		if ((currFKRowIndex > lastFKRowIndex) ||
			(currFKRowIndex == -1))
		{
			int rowCount = 
            	foreignKeyScan.fetchNextGroup(foreignKeyRowArray, (RowLocation[]) null);

			if (rowCount == 0)
			{
				currFKRowIndex = -1;
				return null;
			}

			lastFKRowIndex = rowCount - 1;
			currFKRowIndex = 0;
		}

		return foreignKeyRowArray[currFKRowIndex++];
	}

	/*
	 * Use bulk fetch to get the next set of rows,
	 * or read the next out of our internal array.
	 */
	private DataValueDescriptor[] getNextRef()
		throws StandardException
	{
		if ((currRefRowIndex > lastRefRowIndex) ||
			(currRefRowIndex == -1))
		{
			int rowCount = 
            	referencedKeyScan.fetchNextGroup(referencedKeyRowArray, (RowLocation[]) null);

			if (rowCount == 0)
			{
				currRefRowIndex = -1;
				return null;
			}

			lastRefRowIndex = rowCount - 1;
			currRefRowIndex = 0;
		}

		return referencedKeyRowArray[currRefRowIndex++];
	}

	private void failure(DataValueDescriptor[] foreignKeyRow)
		throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        if (deferred) {
            deferredRowsHashTable =
                    DeferredConstraintsMemory.rememberFKViolation(
                            lcc,
                            deferredRowsHashTable,
                            constraintId,
                            foreignKeyRow,
                            schemaName,
                            tableName);

        } else {
            if (failedCounter == 0)
            {
                if (firstRowToFail != null)
                {
                    firstRowToFail.setRowArray(foreignKeyRow);
                    // clone it
                    firstRowToFail.setRowArray(
                        firstRowToFail.getRowArrayClone());
                }
            }

            failedCounter++;
            if (unreferencedCC != null)
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-532
//IC see: https://issues.apache.org/jira/browse/DERBY-3330
//IC see: https://issues.apache.org/jira/browse/DERBY-6419
                unreferencedCC.insert(foreignKeyRow);
            }
        }
	}	
	/*
	** Returns true if any of the foreign keys are null
	** otherwise, false.
	*/
	private boolean anyNull(DataValueDescriptor[] fkRowArray)
		throws StandardException
	{
		DataValueDescriptor	fkCol;
	
		/*
		** Check all columns excepting the row location.
		*/	
		for (int i = 0; i < numColumns; i++)
		{
			fkCol = (DataValueDescriptor)fkRowArray[i];

			/*
			** If ANY column in the fk is null, 
			** return true
			*/
			if (fkCol.isNull())
			{
				return true;
			}
		}
		return false;

	}

	private int greaterThan(DataValueDescriptor[] fkRowArray, DataValueDescriptor[] refRowArray)
		throws StandardException
	{
		DataValueDescriptor	fkCol;
		DataValueDescriptor	refCol;
		int 				result;
	
		/*
		** If ANY column in the fk is null,
 		** it is assumed to be equal
		*/	
 		if (anyNull(fkRowArray))
                    return EQUAL;

		for (int i = 0; i < numColumns; i++)
		{
			fkCol = (DataValueDescriptor)fkRowArray[i];
			refCol = (DataValueDescriptor)refRowArray[i];

			result = fkCol.compare(refCol);

//IC see: https://issues.apache.org/jira/browse/DERBY-6587
			if (result > 0)
			{
				return GREATER_THAN;
			}
			else if (result < 0)
			{
				return LESS_THAN;
			}

			/*
			** If they are equal, go on to the next 
			** column.
			*/
		}
		
		/*
		** If we got here they must be equal
		*/
		return EQUAL;
	}
}

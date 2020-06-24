/*

   Derby - Class org.apache.derby.impl.sql.execute.DeleteCascadeResultSet

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

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Vector;
import org.apache.derby.catalog.UUID;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.TemporaryRowHolder;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;

/**
 * Delete the rows from the specified  base table and executes delete/update
 * on dependent tables depending on the referential actions specified.
 * Note:(beetle:5197) Dependent Resultsets of DeleteCascade Resultset can  in
 * any one of the multiple resultsets generated for the same table because of
 * multiple foreign key relationship to  the same table. At the bind time ,
 * dependents are binded only once per table.
 * We can not depend on mainNodeTable Flag to fire actions on dependents,
 * it should be done based on whether the resultset has dependent resultsets or not.
 *
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-3049
class DeleteCascadeResultSet extends DeleteResultSet
{

    ResultSet[] dependentResultSets;
	private int noDependents =0;
    private final String resultSetId;
	private boolean mainNodeForTable = true;
	private boolean affectedRows = false;
	private int tempRowHolderId; //this result sets temporary row holder id 

    /*
     * class interface
	 * @exception StandardException		Thrown on error
     */
    public DeleteCascadeResultSet
	(
		NoPutResultSet		source,
		Activation			activation,
		int 				constantActionItem,
		ResultSet[]			dependentResultSets,
		String  	        resultSetId
	)
		throws StandardException
    {

		super(source,
			  ((constantActionItem == -1) ?activation.getConstantAction() :
			  (ConstantAction)activation.getPreparedStatement().getSavedObject(constantActionItem)),
			  activation);

//IC see: https://issues.apache.org/jira/browse/DERBY-4610
//IC see: https://issues.apache.org/jira/browse/DERBY-3049
		ConstantAction passedInConstantAction;
		if(constantActionItem == -1)
			passedInConstantAction = activation.getConstantAction(); //root table
		else
		{
			passedInConstantAction = 
				(ConstantAction) activation.getPreparedStatement().getSavedObject(constantActionItem);
			resultDescription = constants.resultDescription;
		}
		cascadeDelete = true;
		this.resultSetId = resultSetId;
		
		if(dependentResultSets != null)
		{
			noDependents = dependentResultSets.length;
			this.dependentResultSets = dependentResultSets;
		}

	}



	/**
		@exception StandardException Standard Derby error policy
	*/
    @Override
    @SuppressWarnings({"UseOfObsoleteCollectionType", "empty-statement"})
	public void open() throws StandardException
	{


		try{
			setup();
			if(isMultipleDeletePathsExist())
			{
				setRowHoldersTypeToUniqueStream();
				//collect until there are no more rows to found
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                while(collectAffectedRows(false)) {};
			}else
			{
				collectAffectedRows(false);
			}
			if (! affectedRows)
			{
				activation.addWarning(
							StandardException.newWarning(
								SQLState.LANG_NO_ROW_FOUND));
			}

//IC see: https://issues.apache.org/jira/browse/DERBY-6576
            runFkChecker(true); // check for only RESTRICT referential
                                      // action rule violations
            HashMap<String,String> mntHashTable =
                new HashMap<String,String>(); // Hash table to identify multiple
                                              // node for same table cases.
			mergeRowHolders(mntHashTable);
			fireBeforeTriggers(mntHashTable);
            deleteDeferredRows();
            runFkChecker(false); // check for all constraint violations
			rowChangerFinish();
			fireAfterTriggers();
		}finally
		{
			cleanUp();

			//clear the parent result sets hash table
			activation.clearParentResultSets();
		}

		endTime = getCurrentTimeMillis();

    }
	

	/**
	 *Gathers the rows that needs to be deleted/updated 
	 *and creates a temporary resulsets that will be passed
	 *as source to its  dependent result sets.
	 */
    @Override @SuppressWarnings("UseOfObsoleteCollectionType")
	void  setup() throws StandardException
	{

		/* Cache query plan text for source, before it gets blown away */
		if (lcc.getRunTimeStatisticsMode())
		{
			/* savedSource nulled after run time statistics generation */
			savedSource = source;
		}

		super.setup();
		activation.setParentResultSet(rowHolder, resultSetId);
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        Vector<TemporaryRowHolder> sVector =
                activation.getParentResultSet(resultSetId);
		tempRowHolderId = sVector.size() -1;
		for(int i =0 ; i < noDependents; i++)
		{
			if(dependentResultSets[i] instanceof UpdateResultSet)
			{
				((UpdateResultSet) dependentResultSets[i]).setup();
			}else
			{
				((DeleteCascadeResultSet) dependentResultSets[i]).setup();
			}
		}

	}


	boolean  collectAffectedRows(boolean rowsFound) throws StandardException
	{
		if(super.collectAffectedRows())
		{
			affectedRows = true;
			rowsFound = true;
		}

		for(int i =0 ; i < noDependents; i++)
		{
			if(dependentResultSets[i] instanceof UpdateResultSet)
			{
				if(((UpdateResultSet)dependentResultSets[i]).collectAffectedRows())
					rowsFound = true;
			}else
			{
				if(((DeleteCascadeResultSet)
					dependentResultSets[i]).collectAffectedRows(rowsFound))
					rowsFound = true;
			}
		}

		return rowsFound;
	}

//IC see: https://issues.apache.org/jira/browse/DERBY-6576
    void fireBeforeTriggers(HashMap<String, String> msht)
            throws StandardException
	{
		if(!mainNodeForTable) 
		{
			/*to handle case where no table node had qualified rows, in which case no node for
			 * the table get marked as mainNodeFor table , one way to identify
			 * such case is to look at the mutinode hash table and see if the result id exist ,
			 *if it does not means none of the table nodes resulsets got marked
			 * as main node for table. If that is the case we mark this
			 * resultset as mainNodeTable and put entry in the hash table.
			 */
			if(!msht.containsKey(resultSetId))
			{
				mainNodeForTable = true;
				msht.put(resultSetId, resultSetId);
			}
		}
		
		//execute the before triggers on the dependents
		//Defect 5743: Before enabling BEFORE triggers, check DB2 behavior.
		for(int i =0 ; i < noDependents; i++)
		{
			if(dependentResultSets[i] instanceof UpdateResultSet)
			{
				((UpdateResultSet) dependentResultSets[i]).fireBeforeTriggers();
			}
			else{
				((DeleteCascadeResultSet)dependentResultSets[i]).fireBeforeTriggers(msht);
			}
		}

		//If there is more than one node for the same table
		//only one node fires the triggers
		if(mainNodeForTable && constants.deferred)
			super.fireBeforeTriggers();
	}

    @Override
    void fireAfterTriggers() throws StandardException
	{
		//fire the After Triggers on the dependent tables, if any rows changed
		for(int i=0 ; i<noDependents && affectedRows; i++){
			if(dependentResultSets[i] instanceof UpdateResultSet)
			{
				((UpdateResultSet) dependentResultSets[i]).fireAfterTriggers();
			}
			else{

				((DeleteCascadeResultSet)dependentResultSets[i]).fireAfterTriggers();
			}
		}

		//If there is more than one node for the same table
		//, we let only one node fire the triggers.
		if(mainNodeForTable && constants.deferred)
			super.fireAfterTriggers();
	}

    @Override
	void deleteDeferredRows() throws StandardException
	{
		
		//delete the rows in the  dependents tables
		for(int i =0 ; i < noDependents; i++)
		{
			if(dependentResultSets[i] instanceof UpdateResultSet)
			{
				((UpdateResultSet) dependentResultSets[i]).updateDeferredRows();
			}
			else{
				((DeleteCascadeResultSet)dependentResultSets[i]).deleteDeferredRows();
			}
		}

			
		//If there is more than one node for the same table
		//only one node deletes all the rows.
		if(mainNodeForTable)
			super.deleteDeferredRows();
	}

	
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-6576
    void runFkChecker(boolean restrictCheckOnly)
            throws StandardException
	{

		//run the Foreign key or primary key Checker on the dependent tables
		for(int i =0 ; i < noDependents; i++)
		{		
			if(dependentResultSets[i] instanceof UpdateResultSet)
			{
                ((UpdateResultSet) dependentResultSets[i]).runChecker(
//IC see: https://issues.apache.org/jira/browse/DERBY-6576
                    restrictCheckOnly);
			}
			else{
                ((DeleteCascadeResultSet)dependentResultSets[i]).runFkChecker(
                    restrictCheckOnly);
			}
		}

		//If there  is more than one node for the same table
		//only one node does all foreign key checks.
		if(mainNodeForTable)
            super.runFkChecker(restrictCheckOnly);
	}


    @Override
	public void cleanUp() throws StandardException
	{

		super.cleanUp();
		for(int i =0 ; i < noDependents; i++)
		{
			if(dependentResultSets[i] instanceof UpdateResultSet)
			{
				((UpdateResultSet) dependentResultSets[i]).cleanUp();
			}else
			{
				((DeleteCascadeResultSet) dependentResultSets[i]).cleanUp();
			}
		}
		
		endTime = getCurrentTimeMillis();
	}


	private void rowChangerFinish() throws StandardException
	{

		rc.finish();
		for(int i =0 ; i < noDependents; i++)
		{
			if(dependentResultSets[i] instanceof UpdateResultSet)
			{
				((UpdateResultSet) dependentResultSets[i]).rowChangerFinish();
			}else
			{
				((DeleteCascadeResultSet) dependentResultSets[i]).rowChangerFinish();
			}
		}
	}



	//if there is more than one node for the same table, copy the rows
	// into one node , so that we don't fire trigger more than once.
    private void mergeRowHolders(HashMap<String, String> msht)
//IC see: https://issues.apache.org/jira/browse/DERBY-6576
            throws StandardException
	{
		if(msht.containsKey(resultSetId) || rowCount ==0)
		{
			//there is already another resultset node that is marked as main
			//node for this table or this resultset has no rows qualified.
			//when none of the  resultset nodes for the table has any rows then
			//we mark them as one them as main node in fireBeforeTriggers().
			mainNodeForTable = false;
		}else
		{
			mergeResultSets();
			mainNodeForTable = true;
			msht.put(resultSetId, resultSetId);
		}
		
		for(int i =0 ; i < noDependents; i++)
		{		
			if(dependentResultSets[i] instanceof UpdateResultSet)
			{
				return; 
			}
			else{
				((DeleteCascadeResultSet)dependentResultSets[i]).mergeRowHolders(msht);
			}
		}
	}



    @SuppressWarnings("UseOfObsoleteCollectionType")
	private void mergeResultSets() throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        Vector<TemporaryRowHolder>
                sVector = activation.getParentResultSet(resultSetId);
        int size = sVector.size();
		// if there is more than one source, we need to merge them into onc
		// temporary result set.
		if(size > 1)
		{
            ExecRow row;
			int rowHolderId = 0 ;
			//copy all the vallues in the result set to the current resultset row holder
			while(rowHolderId <  size)
			{
				if(rowHolderId == tempRowHolderId )
				{
					//skipping the row holder that  we are copying the rows into.
					rowHolderId++;
					continue;
				}
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                TemporaryRowHolder
                        currentRowHolder = sVector.elementAt(rowHolderId);
				CursorResultSet rs = currentRowHolder.getResultSet();
				rs.open();
				while ((row = rs.getNextRow()) != null)
				{
					rowHolder.insert(row);
				}
				rs.close();
				rowHolderId++;
			}
			
		}
	}


    @Override
	public void finish() throws StandardException {
		super.finish();
		
		//clear the parent result sets hash table
		//This is necessary in case if we hit any error conditions
		activation.clearParentResultSets();
	}


	/* check whether we have mutiple path delete scenario, if
	** find any retun true.	Multiple delete paths exist if we find more than
	** one parent source resultset for a table involved in the delete cascade
	**/
    @SuppressWarnings("UseOfObsoleteCollectionType")
	private boolean isMultipleDeletePathsExist()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        for (Enumeration<String> e = activation.getParentResultSetKeys() ;
             e.hasMoreElements() ;)
		{
            String rsId  = e.nextElement();
            Vector<TemporaryRowHolder>
                    sVector = activation.getParentResultSet(rsId);

            if(sVector.size() > 1)
			{
				return true;
			}
		}
		return false;
	}

	/*
	**Incases where we have multiple paths we could get the same
	**rows to be deleted  mutiple time and also in case of cycles
	**there might be new rows getting added to the row holders through
	**multiple iterations. To handle these case we set the temporary row holders
	** to be  'uniqStream' type.
	**/
    @SuppressWarnings("UseOfObsoleteCollectionType")
	private void setRowHoldersTypeToUniqueStream()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        for (Enumeration<String> e = activation.getParentResultSetKeys() ;
             e.hasMoreElements() ;)
		{
            String rsId  = e.nextElement();
            Vector<TemporaryRowHolder>
                    sVector = activation.getParentResultSet(rsId);
			int size = sVector.size();
			int rowHolderId = 0 ;
			while(rowHolderId <  size)
			{
                TemporaryRowHolder
                        currentRowHolder = sVector.elementAt(rowHolderId);
				currentRowHolder.setRowHolderTypeToUniqueStream();
				rowHolderId++;
			}
		}
	}

}













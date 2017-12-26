/*

   Derby - Class org.apache.derby.impl.sql.execute.NoRowsResultSetImpl

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

import java.sql.SQLWarning;
import java.sql.Timestamp;
import org.w3c.dom.Element;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.Row;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.ResultSetStatisticsFactory;
import org.apache.derby.iapi.sql.execute.RunTimeStatistics;
import org.apache.derby.iapi.sql.execute.xplain.XPLAINVisitor;
import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * Abstract ResultSet for implementations that do not return rows.
 * Examples are DDL statements, CALL statements and DML.
 * <P>
 * An implementation must provide a ResultSet.open() method
 * that performs the required action. 
 * <P>
 * ResultSet.returnsRows() returns false and any method
 * that fetches a row will throw an exception.
 *
 */
abstract class NoRowsResultSetImpl implements ResultSet
{
	final Activation    activation;
    private NoPutResultSet[] subqueryTrackingArray;

	private final boolean statisticsTimingOn;
	/** True if the result set has been opened, and not yet closed. */
	private boolean isOpen;

	/* Run time statistics variables */
	final LanguageConnectionContext lcc;
	protected long beginTime;
	protected long endTime;
	protected long beginExecutionTime;
	protected long endExecutionTime;

    private int                             firstColumn = -1;    // First column being stuffed. For UPDATES, this lies in the second half of the row.
    private int[]                           generatedColumnPositions; // 1-based positions of generated columns in the target row

    // One cell for  each slot in generatedColumnPositions. These are temporary
    // values which hold the result of running the generation clause before we
    // stuff the result into the target row.
    private DataValueDescriptor[]  normalizedGeneratedValues;

	NoRowsResultSetImpl(Activation activation)
	{
		this.activation = activation;

		if (SanityManager.DEBUG) {
			if (activation == null)
				SanityManager.THROWASSERT("activation is null in result set " + getClass());
		}

		lcc = activation.getLanguageConnectionContext();
		statisticsTimingOn = lcc.getStatisticsTiming();

		/* NOTE - We can't get the current time until after setting up the
		 * activation, as we end up using the activation to get the 
		 * LanguageConnectionContext.
		 */
		beginTime = getCurrentTimeMillis();
		beginExecutionTime = beginTime;
	}

	/**
	 * Set up the result set for use. Should always be called from
	 * <code>open()</code>.
	 *
	 * @exception StandardException thrown on error
	 */
	void setup() throws StandardException {
		isOpen = true;

        StatementContext sc = lcc.getStatementContext();
        sc.setTopResultSet(this, subqueryTrackingArray);

        // Pick up any materialized subqueries
        if (subqueryTrackingArray == null) {
            subqueryTrackingArray = sc.getSubqueryTrackingArray();
        }
	}

    /**
	 * Returns FALSE
	 */
	 public final boolean	returnsRows() { return false; }

	/**
	 * Returns zero.
	 */
	public long	modifiedRowCount() { return 0L; }

	/**
	 * Returns null.
	 */
	public ResultDescription	getResultDescription()
	{
	    return (ResultDescription)null;
	}
	
	public final Activation getActivation()
	{
		return activation;
	}

	/**
	 * Returns the row at the absolute position from the query, 
	 * and returns NULL when there is no such position.
	 * (Negative position means from the end of the result set.)
	 * Moving the cursor to an invalid position leaves the cursor
	 * positioned either before the first row (negative position)
	 * or after the last row (positive position).
	 * NOTE: An exception will be thrown on 0.
	 *
	 * @param row	The position.
	 * @return	The row at the absolute position, or NULL if no such position.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public final ExecRow	getAbsoluteRow(int row) throws StandardException
	{
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "absolute");
	}

	/**
	 * Returns the row at the relative position from the current
	 * cursor position, and returns NULL when there is no such position.
	 * (Negative position means toward the beginning of the result set.)
	 * Moving the cursor to an invalid position leaves the cursor
	 * positioned either before the first row (negative position)
	 * or after the last row (positive position).
	 * NOTE: 0 is valid.
	 * NOTE: An exception is thrown if the cursor is not currently
	 * positioned on a row.
	 *
	 * @param row	The position.
	 * @return	The row at the relative position, or NULL if no such position.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public final ExecRow	getRelativeRow(int row) throws StandardException
	{
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "relative");
	}

	/**
	 * Sets the current position to before the first row and returns NULL
	 * because there is no current row.
	 *
	 * @return	NULL.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public final ExecRow	setBeforeFirstRow() 
		throws StandardException
	{
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "beforeFirst");
	}

	/**
	 * Returns the first row from the query, and returns NULL when there
	 * are no rows.
	 *
	 * @return	The first row, or NULL if no rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public final ExecRow	getFirstRow() 
		throws StandardException
	{
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "first");
	}

	/**
     * No rows to return, so throw an exception.
	 *
	 * @exception StandardException		Always throws a
	 *									StandardException to indicate
	 *									that this method is not intended to
	 *									be used.
	 */
	public final ExecRow	getNextRow() throws StandardException
	{
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "next");
	}

	/**
	 * Returns the previous row from the query, and returns NULL when there
	 * are no more previous rows.
	 *
	 * @return	The previous row, or NULL if no more previous rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public final ExecRow	getPreviousRow() 
		throws StandardException
	{
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "previous");
	}

	/**
	 * Returns the last row from the query, and returns NULL when there
	 * are no rows.
	 *
	 * @return	The last row, or NULL if no rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public final ExecRow	getLastRow()
		throws StandardException
	{
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "last");
	}

	/**
	 * Sets the current position to after the last row and returns NULL
	 * because there is no current row.
	 *
	 * @return	NULL.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public final ExecRow	setAfterLastRow() 
		throws StandardException
	{
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "afterLast");
	}

	/**
	 * Clear the current row. This is done after a commit on holdable
	 * result sets.
	 * This is a no-op on result set which do not provide rows.
	 */
	public final void clearCurrentRow() 
	{
		
	}

    /**
     * Determine if the cursor is before the first row in the result 
     * set.   
     *
     * @return true if before the first row, false otherwise. Returns
     * false when the result set contains no rows.
     */
    public final boolean checkRowPosition(int isType)
	{
		return false;
	}

	/**
	 * Returns the row number of the current row.  Row
	 * numbers start from 1 and go to 'n'.  Corresponds
	 * to row numbering used to position current row
	 * in the result set (as per JDBC).
	 *
	 * @return	the row number, or 0 if not on a row
	 *
	 */
	public final int getRowNumber()
	{
		return 0;
	}

	/**
     * Dump the stat if not already done so. Close all of the open subqueries.
	 *
	 * @param underMerge    True if this is part of an action of a MERGE statement.
	 *
	 * @exception StandardException thrown on error
	 */
	public void	close( boolean underMerge ) throws StandardException
	{ 
		if (!isOpen)
			return;

		{
			/*
			** If run time statistics tracing is turned on, then now is the
			** time to dump out the information.
			** NOTE - We make a special exception for commit.  If autocommit
			** is on, then the run time statistics from the autocommit is the
			** only one that the user would ever see.  So, we don't overwrite
			** the run time statistics object for a commit.
            ** DERBY-2353: Also make an exception when the activation is
            ** closed. If the activation is closed, the run time statistics
            ** object is null and there's nothing to print. This may happen
            ** if a top-level result set closes the activation and close() is
            ** subsequently called on the child result sets. The information
            ** about the children is also printed by the parent, so it's safe
            ** to skip printing it.
			*/
			if (lcc.getRunTimeStatisticsMode() &&
                !doesCommit() && !activation.isClosed() &&
                !lcc.getStatementContext().getStatementWasInvalidated())
			{
				endExecutionTime = getCurrentTimeMillis();

				ResultSetStatisticsFactory rssf =
                    lcc.getLanguageConnectionFactory().
                         getExecutionFactory().getResultSetStatisticsFactory();

                // get the RuntimeStatisticsImpl object which is the wrapper for all 
                // statistics
                RunTimeStatistics rsImpl = rssf.getRunTimeStatistics(activation, this, subqueryTrackingArray); 

                // save RTS object in lcc
                lcc.setRunTimeStatisticsObject(rsImpl);
                
                // explain gathered statistics
                XPLAINVisitor visitor =  lcc.getLanguageConnectionFactory().getExecutionFactory().getXPLAINFactory().getXPLAINVisitor();
                visitor.doXPLAIN(rsImpl,activation);

			}
		}

		/* This is the top ResultSet, 
		 * close all of the open subqueries.
		 */
		int staLength = (subqueryTrackingArray == null) ? 0 :
							subqueryTrackingArray.length;

		for (int index = 0; index < staLength; index++)
		{
			if (subqueryTrackingArray[index] == null)
			{
				continue;
			}
			if (subqueryTrackingArray[index].isClosed())
			{
				continue;
			}
			subqueryTrackingArray[index].close();
		}

		isOpen = false;

		if (activation.isSingleExecution() && !underMerge)
			activation.close();
	}

	/**
	 * Find out if the <code>ResultSet</code> is closed.
	 *
	 * @return <code>true</code> if closed, <code>false</code> otherwise
	 */
	public boolean isClosed() {
		return !isOpen;
	}

	public void	finish() throws StandardException
	{
	}

	/**
	 * Get the execution time in milliseconds.
	 *
	 * @return long		The execution time in milliseconds.
	 */
	public long getExecuteTime()
	{
		return endTime - beginTime;
	}

	/**
	 * Get the Timestamp for the beginning of execution.
	 *
	 * @return Timestamp		The Timestamp for the beginning of execution.
	 */
	public Timestamp getBeginExecutionTimestamp()
	{
		if (beginExecutionTime == 0)
		{
			return null;
		}
		else
		{
			return new Timestamp(beginExecutionTime);
		}
	}

	/**
	 * Get the Timestamp for the end of execution.
	 *
	 * @return Timestamp		The Timestamp for the end of execution.
	 */
	public Timestamp getEndExecutionTimestamp()
	{
		if (endExecutionTime == 0)
		{
			return null;
		}
		else
		{
			return new Timestamp(endExecutionTime);
		}
	}

	/**
	 * RESOLVE - This method will go away once it is overloaded in all subclasses.
	 * Return the query plan as a String.
	 *
	 * @param depth	Indentation level.
	 *
	 * @return String	The query plan as a String.
	 */
	public String getQueryPlanText(int depth)
	{
		return MessageService.getTextMessage(
				SQLState.LANG_GQPT_NOT_SUPPORTED,
				getClass().getName());
	}

	/**
	 * Return the total amount of time spent in this ResultSet
	 *
	 * @param type	CURRENT_RESULTSET_ONLY - time spent only in this ResultSet
	 *				ENTIRE_RESULTSET_TREE  - time spent in this ResultSet and below.
	 *
	 * @return long		The total amount of time spent (in milliseconds).
	 */
	public long getTimeSpent(int type)
	{
		/* RESOLVE - this should be overloaded in all subclasses */
		return 0;
	}

	/**
	 * @see ResultSet#getSubqueryTrackingArray
	 */
	public final NoPutResultSet[] getSubqueryTrackingArray(int numSubqueries)
	{
		if (subqueryTrackingArray == null)
		{
			subqueryTrackingArray = new NoPutResultSet[numSubqueries];
		}

		return subqueryTrackingArray;
	}

	/**
	 * @see ResultSet#getAutoGeneratedKeysResultset
	 */
	public ResultSet getAutoGeneratedKeysResultset()
	{
		//A non-null resultset would be returned only for an insert statement 
		return (ResultSet)null;
	}

	/**
		Return the cursor name, null in this case.

		@see ResultSet#getCursorName
	*/
	public String getCursorName() {
		return null;
	}

	// class implementation

	/**
	 * Return the current time in milliseconds, if DEBUG and RunTimeStats is
	 * on, else return 0.  (Only pay price of system call if need to.)
	 *
	 * @return long		Current time in milliseconds.
	 */
	protected final long getCurrentTimeMillis()
	{
		if (statisticsTimingOn)
		{
			return System.currentTimeMillis();
		}
		else
		{
			return 0;
		}
	}


	/**
	  * Compute the generation clauses on the current row in order to fill in
	  * computed columns.
      *
      * @param generationClauses    the generated method which evaluates generation clauses
      * @param activation               the thread-specific instance of the generated class
      * @param source                   the tuple stream driving this INSERT/UPDATE
      * @param newRow                   the base row being stuffed
      * @param isUpdate                 true if this is an UPDATE. false otherwise.
	  */
	public	void	evaluateGenerationClauses
	(
	  GeneratedMethod generationClauses,
	  Activation activation,
      NoPutResultSet    source,
      ExecRow           newRow,
      boolean           isUpdate
	)
		throws StandardException
	{
		if (generationClauses != null)
		{
            ExecRow oldRow = (ExecRow) activation.getCurrentRow( source.resultSetNumber() );

            //
            // We may need to poke the current row into the Activation so that
            // it is visible to the method which evaluates the generation
            // clause. This is because the generation clause may refer to other
            // columns in that row.
            //
            try {
                source.setCurrentRow( newRow );
                activation.setCurrentRow( newRow, source.resultSetNumber() );
                generationClauses.invoke(activation);

                //
                // Now apply NOT NULL checks and other coercions. For non-generated columns, these
                // are performed in the driving ResultSet.
                //
                if ( firstColumn < 0 ) { firstColumn = NormalizeResultSet.computeStartColumn( isUpdate, activation.getResultDescription() ); }
                if ( generatedColumnPositions == null ) { setupGeneratedColumns( activation, (ValueRow) newRow ); }
                
                ResultDescription   resultDescription = activation.getResultDescription();
                int                         count = generatedColumnPositions.length;

                for ( int i = 0; i < count; i++ )
                {
                    int         position = generatedColumnPositions[ i ];

                    DataValueDescriptor normalizedColumn = NormalizeResultSet.normalizeColumn
                        (
                         resultDescription.getColumnDescriptor( position ).getType(),
                         newRow,
                         position,
                         normalizedGeneratedValues[ i ],
                         resultDescription
                         );

                    newRow.setColumn( position, normalizedColumn );
                }
            }
            finally
            {
                //
                // We restore the Activation to its state before we ran the generation
                // clause. This may not be necessary but I don't understand all of
                // the paths through the Insert and Update result sets. This
                // defensive coding seems prudent to me.
                //
                if ( oldRow == null ) { source.clearCurrentRow(); }
                else { source.setCurrentRow( oldRow ); }
            }
		}
	}

	/**
	  * Construct support for normalizing generated columns. This figures out
	  * which columns in the target row have generation clauses which need to be run.
	  */
    private void    setupGeneratedColumns( Activation activation, ValueRow newRow )
        throws StandardException
    {
        ResultDescription   resultDescription = activation.getResultDescription();
        int                         columnCount = resultDescription.getColumnCount();
        ExecRow                 emptyRow = newRow.getNewNullRow();
        int                         generatedColumnCount = 0;

        // first count the number of generated columns
        for ( int i = 1; i <= columnCount; i++ )
        {
            if ( i < firstColumn ) { continue; }
            
            ResultColumnDescriptor  rcd = resultDescription.getColumnDescriptor( i );

            if ( rcd.hasGenerationClause() ) { generatedColumnCount++; }
        }

        // now allocate and populate support structures
        generatedColumnPositions = new int[ generatedColumnCount ];
        normalizedGeneratedValues = new DataValueDescriptor[ generatedColumnCount ];

        int     idx = 0;
        for ( int i = 1; i <= columnCount; i++ )
        {
            if ( i < firstColumn ) { continue; }
            
            ResultColumnDescriptor  rcd = resultDescription.getColumnDescriptor( i );

            if ( rcd.hasGenerationClause() )
            {
                generatedColumnPositions[ idx ] = i;
                normalizedGeneratedValues[ idx ] = emptyRow.getColumn( i );

                idx++;
            }
        }
    }
    
	/**
	 * Does this ResultSet cause a commit or rollback.
	 *
	 * @return Whether or not this ResultSet cause a commit or rollback.
	 */
	public boolean doesCommit()
	{
		return false;
	}

    public void addWarning(SQLWarning w) {
        // We're not returning a (JDBC) ResultSet, so add the warning to
        // the Activation so that it's included in the warning chain of the
        // executing Statement.
        getActivation().addWarning(w);
    }

	public SQLWarning getWarnings() {
		return null;
	}

    public Element toXML( Element parentNode, String tag ) throws Exception
    {
        return BasicNoPutResultSetImpl.childrenToXML( BasicNoPutResultSetImpl.toXML( parentNode, tag, this ), this );
    }

}

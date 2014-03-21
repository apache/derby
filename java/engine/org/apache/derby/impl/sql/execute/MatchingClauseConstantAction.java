/*

   Derby - Class org.apache.derby.impl.sql.execute.MatchingClauseConstantAction

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

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * Describes the execution machinery needed to evaluate a WHEN [ NOT ] MATCHING clause
 * of a MERGE statement.
 */
public class MatchingClauseConstantAction implements ConstantAction, Formatable
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Serial version produced by the serialver utility. Needed in order to
     * make serialization work reliably across different compilers.
     */
    private static  final   long    serialVersionUID = -6725483265211088817L;

    // for versioning during (de)serialization
    private static final int FIRST_VERSION = 0;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    // constructor args
    private int _clauseType;
    private String  _matchRefinementName;
    private ResultDescription   _thenColumnSignature;
    private String  _rowMakingMethodName;
    private String  _resultSetFieldName;
    private String  _actionMethodName;
    private ConstantAction  _thenAction;

    // faulted in or built at run-time
    private transient   GeneratedMethod _matchRefinementMethod;
    private transient   GeneratedMethod _rowMakingMethod;
    private transient   ResultSet           _actionRS;


    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** 0-arg constructor needed by Formatable machinery */
    public  MatchingClauseConstantAction() {}

    /**
     * Construct from thin air.
     *
     * @param   clauseType  WHEN_NOT_MATCHED_THEN_INSERT, WHEN_MATCHED_THEN_UPDATE, WHEN_MATCHED_THEN_DELETE
     * @param   matchRefinementName Name of the method which evaluates the boolean expression in the WHEN clause.
     * @param   thenColumnSignature The shape of the row which goes into the temporary table.
     * @param   rowMakingMethodName Name of the method which populates the "then" row with expressions from the driving left join.
     * @param   resultSetFieldName  Name of the field which will be stuffed at runtime with the temporary table of relevant rows.
     * @param   actionMethodName    Name of the method which invokes the INSERT/UPDATE/DELETE action.
     * @param   thenAction  The ConstantAction describing the associated INSERT/UPDATE/DELETE action.
     */
    public  MatchingClauseConstantAction
        (
         int    clauseType,
         String matchRefinementName,
         ResultDescription  thenColumnSignature,
         String rowMakingMethodName,
         String resultSetFieldName,
         String actionMethodName,
         ConstantAction thenAction
         )
    {
        _clauseType = clauseType;
        _matchRefinementName = matchRefinementName;
        _thenColumnSignature = thenColumnSignature;
        _rowMakingMethodName = rowMakingMethodName;
        _resultSetFieldName = resultSetFieldName;
        _actionMethodName = actionMethodName;
        _thenAction = thenAction;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ACCESSORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Get the clause type: WHEN_NOT_MATCHED_THEN_INSERT, WHEN_MATCHED_THEN_UPDATE, WHEN_MATCHED_THEN_DELETE */
    public  int clauseType() { return _clauseType; }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ConstantAction BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

	public void	executeConstantAction( Activation activation )
        throws StandardException
    {}
    
	public void	executeConstantAction( Activation activation, TemporaryRowHolderImpl thenRows )
        throws StandardException
    {
        // nothing to do if no rows qualified
        if ( thenRows == null ) { return; }

        CursorResultSet sourceRS = thenRows.getResultSet();
        GeneratedMethod actionGM = ((BaseActivation) activation).getMethod( _actionMethodName );

        //
        // Push the action-specific ConstantAction rather than the Merge statement's
        // ConstantAction. The INSERT/UPDATE/DELETE expects the default ConstantAction
        // to be appropriate to it.
        //
        try {
            activation.pushConstantAction( _thenAction );

            try {
                //
                // Poke the temporary table into the variable which will be pushed as
                // an argument to the INSERT/UPDATE/DELETE action.
                //
                Field   resultSetField = activation.getClass().getField( _resultSetFieldName );
                resultSetField.set( activation, sourceRS );

                Activation  cursorActivation = sourceRS.getActivation();

                //
                // Now execute the generated method which creates an InsertResultSet,
                // UpdateResultSet, or DeleteResultSet.
                //
                Method  actionMethod = activation.getClass().getMethod( _actionMethodName );
                _actionRS = (ResultSet) actionMethod.invoke( activation, null );
            }
            catch (Exception e) { throw StandardException.plainWrapException( e ); }

            // this is where the INSERT/UPDATE/DELETE is processed
            _actionRS.open();
        }
        finally
        {
            activation.popConstantAction();
        }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OTHER PACKAGE VISIBLE BEHAVIOR, CALLED BY MergeResultSet
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Initialize this constant action, nulling out any transient state left over from
     * a previous use.
     * </p>
     */
    void    init()  throws StandardException
    {
        _actionRS = null;
    }
    
    /**
     * <p>
     * Run the matching refinement clause associated with this WHEN [ NOT ] MATCHED clause.
     * The refinement is a boolean expression. Return the boolean value it resolves to.
     * A boolean NULL is treated as false. If there is no refinement clause, then this method
     * evaluates to true.
     * </p>
     */
    boolean evaluateRefinementClause( Activation activation )
        throws StandardException
    {
        if ( _matchRefinementName == null ) { return true; }
        if ( _matchRefinementMethod == null )
        {
            _matchRefinementMethod = ((BaseActivation) activation).getMethod( _matchRefinementName );
        }

        SQLBoolean  result = (SQLBoolean) _matchRefinementMethod.invoke( activation );

        if ( result.isNull() ) { return false; }
        else { return result.getBoolean(); }
    }

    /**
     * <p>
     * Construct and buffer a row for the INSERT/UPDATE/DELETE
     * action corresponding to this [ NOT ] MATCHED clause. The buffered row
     * is built from columns in the passed-in row. The passed-in row is the SELECT list
     * of the MERGE statement's driving left join.
     * </p>
     */
    TemporaryRowHolderImpl  bufferThenRow
        (
         Activation activation,
         TemporaryRowHolderImpl thenRows,
         ExecRow selectRow
         ) throws StandardException
    {
        if ( thenRows == null ) { thenRows = createThenRows( activation ); }

        ExecRow thenRow = bufferThenRow( activation );

        thenRows.insert( thenRow );

        return thenRows;
    }
    
    /**
     * <p>
     * Release resources at the end.
     * </p>
     */
    void    cleanUp()   throws StandardException
    {
        if ( _actionRS != null )
        {
            _actionRS.close();
            _actionRS = null;
        }

        _matchRefinementMethod = null;
        _rowMakingMethod = null;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCT ROWS TO PUT INTO THE TEMPORARY TABLE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Construct and buffer a row for the INSERT/UPDATE/DELETE
     * action corresponding to this [ NOT ] MATCHED clause.
     * </p>
     */
    private ExecRow    bufferThenRow
        (
         Activation activation
         )
        throws StandardException
    {
        if ( _rowMakingMethod == null )
        {
            _rowMakingMethod = ((BaseActivation) activation).getMethod( _rowMakingMethodName );
        }

        return (ExecRow) _rowMakingMethod.invoke( activation );
    }
    
    /**
     * <p>
     * Create the temporary table for holding the rows which are buffered up
     * for bulk-processing after the driving left join completes.
     * </p>
     */
    private TemporaryRowHolderImpl  createThenRows( Activation activation )
        throws StandardException
    {
        return new TemporaryRowHolderImpl( activation, new Properties(), _thenColumnSignature );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Formatable BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal( ObjectInput in )
		 throws IOException, ClassNotFoundException
	{
        // as the persistent form evolves, switch on this value
        int oldVersion = in.readInt();

        _clauseType = in.readInt();
        _matchRefinementName = (String) in.readObject();
        _thenColumnSignature = (ResultDescription) in.readObject();
        _rowMakingMethodName = (String) in.readObject();
        _resultSetFieldName = (String) in.readObject(); 
        _actionMethodName = (String) in.readObject();
       _thenAction = (ConstantAction) in.readObject();
	}

	/**
	 * Write this object to a stream of stored objects.
	 *
	 * @param out write bytes here.
	 *
	 * @exception IOException		thrown on error
	 */
	public void writeExternal( ObjectOutput out )
		 throws IOException
	{
		out.writeInt( FIRST_VERSION );

        out.writeInt( _clauseType );
        out.writeObject( _matchRefinementName );
        out.writeObject( _thenColumnSignature );
        out.writeObject( _rowMakingMethodName );
        out.writeObject( _resultSetFieldName );
        out.writeObject( _actionMethodName );
        out.writeObject( _thenAction );
	}
 
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.MATCHING_CLAUSE_CONSTANT_ACTION_V01_ID; }

}

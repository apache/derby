/*

   Derby - Class org.apache.derby.impl.sql.execute.MergeConstantAction

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
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.util.ArrayUtil;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * Describes the execution machinery needed to evaluate a MERGE statement.
 */
public class MergeConstantAction implements ConstantAction, Formatable
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
    //private static  final   long    serialVersionUID = -6725483265211088817L;

    // for versioning during (de)serialization
    private static final int FIRST_VERSION = 0;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    // constructor args
    private MatchingClauseConstantAction[]  _matchingClauses;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** 0-arg constructor needed by Formatable machinery */
    public  MergeConstantAction() {}

    /**
     * Construct from thin air.
     *
     * @param   matchingClauses Constant actions for WHEN [ NOT ] MATCHED clauses.
     */
    public  MergeConstantAction
        (
         ConstantAction[] matchingClauses
         )
    {
        int     clauseCount = matchingClauses.length;
        _matchingClauses = new MatchingClauseConstantAction[ clauseCount ];
        for ( int i = 0; i < clauseCount; i++ )
        { _matchingClauses[ i ] = (MatchingClauseConstantAction) matchingClauses[ i ]; }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ACCESSORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Get the number of matching clauses */
    public  int matchingClauseCount() { return _matchingClauses.length; }

    /** Get the ith (0-based) matching clause */
    public  MatchingClauseConstantAction  getMatchingClause( int idx )  { return _matchingClauses[ idx ]; }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ConstantAction BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

	public void	executeConstantAction( Activation activation )
        throws StandardException
    {}
    
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

        _matchingClauses = new MatchingClauseConstantAction[ in.readInt() ];
        for ( int i = 0; i < _matchingClauses.length; i++ )
        { _matchingClauses[ i ] = (MatchingClauseConstantAction) in.readObject(); }
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

        out.writeInt( _matchingClauses.length );
        for ( int i = 0; i < _matchingClauses.length; i++ ) { out.writeObject( _matchingClauses[ i ] ); }
	}
 
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.MERGE_CONSTANT_ACTION_V01_ID; }

}

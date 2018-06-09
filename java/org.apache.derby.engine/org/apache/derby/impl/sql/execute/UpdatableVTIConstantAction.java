/*

   Derby - Class org.apache.derby.impl.sql.execute.UpdatableVTIConstantAction

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

import org.apache.derby.iapi.services.io.StoredFormatIds;

/**
 *	This class  describes compiled constants that are passed into
 *	Updatable VTIResultSets.
 *
 */

public class UpdatableVTIConstantAction extends WriteCursorConstantAction
{

	/********************************************************
	**
	**	This class implements Formatable. But it is NOT used
 	**	across either major or minor releases.  It is only
	** 	written persistently in stored prepared statements, 
	**	not in the replication stage.  SO, IT IS OK TO CHANGE
	**	ITS read/writeExternal.
	**
	********************************************************/

	public int[]	changedColumnIds;

    public int statementType;
    
	// CONSTRUCTORS

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
    public	UpdatableVTIConstantAction() { super(); }

	/**
	 *	Make the ConstantAction for an updatable VTI statement.
	 *
     * @param statementType             Statement type, cf.
     * {@link org.apache.derby.vti.DeferModification#INSERT_STATEMENT} etc.
	 * @param deferred					Whether or not to do operation in deferred mode
     * @param changedColumnIds Array of ids of changed columns
	 *
	 */
    UpdatableVTIConstantAction(int statementType,
                               boolean deferred,
                               int[] changedColumnIds)
	{
		super(0, 
			  null,
			  null, 
			  null, 
			  null,
			  null, 
			  deferred	, 
			  null,
			  null,
			  0,
			  null,	
			  null,
			  null,
			  null,
			  null,
			  // singleRowSource, irrelevant
			  false,
			  false // not under a MERGE statement
			  );
        this.statementType = statementType;
        this.changedColumnIds = changedColumnIds;
	}

	// INTERFACE METHODS

	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.UPDATABLE_VTI_CONSTANT_ACTION_V01_ID; }

	// CLASS METHODS

}

/*

   Derby - Class org.apache.derby.impl.sql.execute.UpdateConstantAction

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

import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;

import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;

import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.util.Properties;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

/**
 *	This class  describes compiled constants that are passed into
 *	UpdateResultSets.
 *
 */

public class UpdateConstantAction extends WriteCursorConstantAction
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
	
	/* 
	** Integer array of columns that are being updated.
	*/
	int[]	changedColumnIds;

	private boolean positionedUpdate;

	int numColumns;

    private String schemaName;
    private String tableName;

	// CONSTRUCTORS

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
    public	UpdateConstantAction() { super(); }

	/**
	 *	Make the ConstantAction for an UPDATE statement.
	 *
     *  @param targetTableDesc descriptor for the table to be updated
	 *	@param heapSCOCI	StaticCompiledOpenConglomInfo for heap.
	 *  @param irgs			Index descriptors
	 *  @param indexCIDS	Conglomerate IDs of indices
	 *	@param indexSCOCIs	StaticCompiledOpenConglomInfos for indexes.
	 *  @param indexNames	Names of indices on this table for error reporting.
	 *  @param deferred		True means process as a deferred update.
	 *	@param targetUUID	UUID of target table
	 *	@param lockMode		The lock mode to use
	 *							(row or table, see TransactionController)
	 *  @param changedColumnIds	Array of ids of changed columns	
	 *	@param fkInfo		Array of structures containing foreign key info, 
	 *						if any (may be null)
	 *	@param triggerInfo	Array of structures containing trigger info, 
	 *						if any (may be null)
	 *  @param baseRowReadList Map of columns read in.  1 based.
	 *  @param baseRowReadMap BaseRowReadMap[heapColId]->ReadRowColumnId. (0 based)
     *  @param streamStorableHeapColIds Null for non rep. (0 based)
	 *  @param numColumns	Number of columns being read.
	 *  @param positionedUpdate	is this a positioned update
	 *  @param singleRowSource		Whether or not source is a single row source
	 *  @param underMerge   True if this is an action of a MERGE statement.
	 */
    UpdateConstantAction(
                                TableDescriptor     targetTableDesc,
								StaticCompiledOpenConglomInfo heapSCOCI,
								IndexRowGenerator[]	irgs,
								long[]				indexCIDS,
								StaticCompiledOpenConglomInfo[] indexSCOCIs,
								String[]			indexNames,
								boolean				deferred,
								UUID				targetUUID,
								int					lockMode,
								int[]				changedColumnIds,
								FKInfo[]			fkInfo,
								TriggerInfo			triggerInfo,
								FormatableBitSet				baseRowReadList,
								int[]				baseRowReadMap,
								int[]               streamStorableHeapColIds,
								int					numColumns,
								boolean				positionedUpdate,
								boolean				singleRowSource,
                                boolean             underMerge)
            throws StandardException
	{
		super(
            targetTableDesc.getHeapConglomerateId(),
			heapSCOCI,
			irgs,
			indexCIDS,
			indexSCOCIs,
			indexNames,
			deferred, 
			(Properties) null,
			targetUUID,
			lockMode,
			fkInfo,
			triggerInfo,
			baseRowReadList,
			baseRowReadMap,
			streamStorableHeapColIds,
			singleRowSource,
			underMerge
			);

		this.changedColumnIds = changedColumnIds;
		this.positionedUpdate = positionedUpdate;
		this.numColumns = numColumns;
        this.schemaName = targetTableDesc.getSchemaName();
        this.tableName = targetTableDesc.getName();
	}

	// INTERFACE METHODS


	// Formatable methods

	/**
	  @see java.io.Externalizable#readExternal
	  @exception IOException thrown on error
	  @exception ClassNotFoundException	thrown on error
	  */
	public void readExternal( ObjectInput in )
		 throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		changedColumnIds = ArrayUtil.readIntArray(in);
		positionedUpdate = in.readBoolean();
		numColumns = in.readInt();
	}

	/**

	  @see java.io.Externalizable#writeExternal
	  @exception IOException thrown on error
	  */
	public void writeExternal( ObjectOutput out )
		 throws IOException
	{
		super.writeExternal(out);
		ArrayUtil.writeIntArray(out,changedColumnIds);
		out.writeBoolean(positionedUpdate);
		out.writeInt(numColumns);
	}

	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int getTypeFormatId()	{ return StoredFormatIds.UPDATE_CONSTANT_ACTION_V01_ID; }

	// CLASS METHODS
    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }
}

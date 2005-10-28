/*

   Derby - Class org.apache.derby.impl.sql.execute.WriteCursorConstantAction

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.sql.execute.ExecutionContext;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;

import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.catalog.UUID;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.util.Properties;


/**
 *	This abstract class describes compiled constants that are passed into
 *	Delete, Insert, and Update ResultSets.
 *
 *	@author Rick Hillegas
 */

abstract	class WriteCursorConstantAction implements ConstantAction, Formatable
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

	public	long						conglomId;
	public  StaticCompiledOpenConglomInfo heapSCOCI;
	public	IndexRowGenerator[] 		irgs;
	public	long[]						indexCIDS;
	public  StaticCompiledOpenConglomInfo[] indexSCOCIs;
	public  String[]					indexNames;
	public	boolean						deferred;
	public  Properties					targetProperties;
	public	UUID						targetUUID;
	public	int							lockMode;
	private	FKInfo[]					fkInfo;
	private TriggerInfo					triggerInfo;

	private ExecRow						emptyHeapRow;
	private FormatableBitSet baseRowReadList;
	private int[] baseRowReadMap;
	private int[] streamStorableHeapColIds;
	public boolean singleRowSource;


	// CONSTRUCTORS

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
    public	WriteCursorConstantAction() {}

	/**
	 *	Make the ConstantAction for a DELETE, INSERT, or UPDATE statement.
	 *
	 *  @param conglomId	Conglomerate ID of heap.
	 *	@param heapSCOCI	StaticCompiledOpenConglomInfo for heap.
	 *  @param irgs			Index descriptors
	 *  @param indexCIDS	Conglomerate IDs of indices
	 *	@param indexSCOCIs	StaticCompiledOpenConglomInfos for indexes.
	 *  @param indexNames   Names of indices on this table for error reporting.
	 *  @param deferred		True means process as a deferred update
	 *  @param targetProperties	Properties on the target table
	 *	@param targetUUID	UUID of target table
	 *	@param lockMode		The lock mode to use on the target table
	 *	@param fkInfo	Structure containing foreign key info, if any (may be null)
	 *	@param triggerInfo	Structure containing trigger info, if any (may be null)
	 *  @param emptyHeapRow	an empty heap row
	 *  @param baseRowReadMap	BaseRowReadMap[heapColId]->ReadRowColumnId. (0 based)
     *  @param streamStorableHeapColIds Null for non rep. (0 based)
	 *  @param numColumns	Number of columns to read.
	 *  @param singleRowSource		Whether or not source is a single row source
	 */
	public	WriteCursorConstantAction(
								long				conglomId,
								StaticCompiledOpenConglomInfo heapSCOCI,
								IndexRowGenerator[]	irgs,
								long[]				indexCIDS,
								StaticCompiledOpenConglomInfo[] indexSCOCIs,
								String[]			indexNames,
								boolean				deferred,
								Properties			targetProperties,
								UUID				targetUUID,
								int					lockMode,
								FKInfo[]			fkInfo,
								TriggerInfo			triggerInfo,
								ExecRow				emptyHeapRow,
								FormatableBitSet				baseRowReadList,
								int[]               baseRowReadMap,
								int[]               streamStorableHeapColIds,
								boolean				singleRowSource
								)
	{
		this.conglomId = conglomId;
		this.heapSCOCI = heapSCOCI;
		this.irgs = irgs;
		this.indexSCOCIs = indexSCOCIs;
		this.indexCIDS = indexCIDS;
		this.indexSCOCIs = indexSCOCIs;
		this.deferred = deferred;
		this.targetProperties = targetProperties;
		this.targetUUID = targetUUID;
		this.lockMode = lockMode;
		this.emptyHeapRow = emptyHeapRow;
		this.fkInfo = fkInfo;
		this.triggerInfo = triggerInfo;
		this.baseRowReadList = baseRowReadList;
		this.baseRowReadMap = baseRowReadMap;
		this.streamStorableHeapColIds = streamStorableHeapColIds;
		this.singleRowSource = singleRowSource;
		this.indexNames = indexNames;
		if (SanityManager.DEBUG)
		{
			if (fkInfo != null)
			{
				SanityManager.ASSERT(fkInfo.length != 0, "fkinfo array has no elements, if there are no foreign keys, then pass in null");
			}
		}
	}

	///////////////////////////////////////////////////////////////////
	//
	//	ACCESSORS
	//
	///////////////////////////////////////////////////////////////////

	/**
	  *	Gets the foreign key information for this constant action.
	  *	A full list of foreign keys was compiled into this constant
	  *	action. However, we prune this list at run time so that we
	  *	enforce only the foreign keys that we absolutely must.
	  *
	  *	What's going on here? Well, in certain contexts (REFRESH and
	  *	when intially booting a Target), we don't have to enforce some
	  *	foreign keys. We allow the ExecutionContext to rule on which
	  *	foreign keys are relevant and which aren't.
	  *
	  *	@param	ec	the ExecutionContext
	  *
	  *	@return	the list of foreign keys to enforce for this action
	  *
	  * @exception StandardException		Thrown on failure
	  */
	public	FKInfo[]	getFKInfo
	(
		ExecutionContext	ec
    )
		throws StandardException
	{
		FKInfo[]	siftedArray = (FKInfo[]) ec.siftForeignKeys( fkInfo );

		return siftedArray;
	}

	/**
	 * Basically, the same as getFKInfo but for triggers.
	 *
	 * @return	the triggers that should be fired
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public TriggerInfo getTriggerInfo(ExecutionContext ec) 
		throws StandardException
	{
		return (TriggerInfo)ec.siftTriggers(triggerInfo);
	}


	///////////////////////////////////////////////////////////////////
	//
	// INTERFACE METHODS
	//
	///////////////////////////////////////////////////////////////////


    /**
	  *	Reports whether these constants are up-to-date. This returns true
	  *	for homogenous Cloudscape/Cloudsync. For the Plugin, this may
	  *	return false;
	  *
	  *	@return	true if these constants are up-to-date
	  *			false otherwise
	  */
	public	boolean	upToDate()
	{ return true; }

	/**
	 *	NOP routine. The work is done in InsertResultSet.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
		throws StandardException { }

	// Formatable methods
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
		conglomId = in.readLong();
		heapSCOCI = (StaticCompiledOpenConglomInfo) in.readObject();
		irgs = new IndexRowGenerator[ArrayUtil.readArrayLength(in)];
		ArrayUtil.readArrayItems(in, irgs);

		indexCIDS = ArrayUtil.readLongArray(in);
		indexSCOCIs = new StaticCompiledOpenConglomInfo[ArrayUtil.readArrayLength(in)];
		ArrayUtil.readArrayItems(in, indexSCOCIs);

		deferred = in.readBoolean();
		targetProperties = (Properties) in.readObject();
		targetUUID = (UUID) in.readObject();
		lockMode = in.readInt();

		fkInfo = new FKInfo[ArrayUtil.readArrayLength(in)];
		ArrayUtil.readArrayItems(in, fkInfo);

		triggerInfo = (TriggerInfo)in.readObject();

		baseRowReadList = (FormatableBitSet)in.readObject();
		baseRowReadMap = ArrayUtil.readIntArray(in);
		streamStorableHeapColIds = ArrayUtil.readIntArray(in); 
		singleRowSource = in.readBoolean();
		indexNames = ArrayUtil.readStringArray(in);
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
		out.writeLong(conglomId);
		out.writeObject(heapSCOCI);
		ArrayUtil.writeArray(out, irgs);
		ArrayUtil.writeLongArray(out, indexCIDS);
		ArrayUtil.writeArray(out, indexSCOCIs);
		out.writeBoolean(deferred);
		out.writeObject(targetProperties);
		out.writeObject(targetUUID);
		out.writeInt(lockMode);
		ArrayUtil.writeArray(out, fkInfo);

		//
		//Added for Xena.
		out.writeObject(triggerInfo);

		//
		//Moved from super class for Xena.
		out.writeObject(baseRowReadList);

		//
		//Added for Xena
		ArrayUtil.writeIntArray(out,baseRowReadMap);
		ArrayUtil.writeIntArray(out,streamStorableHeapColIds);

		//Added for Buffy
		out.writeBoolean(singleRowSource);
		
		// Added for Mulan (Track Bug# 3322)
		ArrayUtil.writeArray(out, indexNames);
		
	}

	// ACCESSORS

	/**
	 * Get the conglomerate id for the changed heap.
	 * @return the conglomerate id.
	 */
	public long getConglomerateId() { return conglomId; }
		
	/**
	 *	Get emptyHeapRow
	 *
	 * @param lcc	The LanguageConnectionContext to use.
	 *
	 * @return	an empty base table row for the table being updated.
	 *
	 * @exception StandardException on error
	 */
	public ExecRow getEmptyHeapRow(LanguageConnectionContext lcc) throws StandardException
	{
		DataDictionary dd;
		TableDescriptor td;

		if (emptyHeapRow == null)
		{

			dd = lcc.getDataDictionary();
	
			td = dd.getTableDescriptor(targetUUID);
	
			emptyHeapRow = td.getEmptyExecRow(lcc.getContextManager());
		}

		return emptyHeapRow.getClone();
	}

	/**
	 * Get the targetProperties from the constant action.
	 *
	 * @return The targetProperties
	 */
	public Properties getTargetProperties()
	{
		return targetProperties;
	}

	/**
	 * The the value of the specified key, if it exists, from
	 * the targetProperties.
	 *
	 * @param key		The key to search for
	 *
	 * @return	The value for the specified key if it exists, otherwise null.
	 *			(Return null if targetProperties is null.)
	 */
	public String getProperty(String key)
	{
		return (targetProperties == null) ? null : targetProperties.getProperty(key);
	}

	public FormatableBitSet getBaseRowReadList() { return baseRowReadList; }
	public int[] getBaseRowReadMap() { return baseRowReadMap; }
	public int[] getStreamStorableHeapColIds() { return streamStorableHeapColIds; }

	/**
	 * get the index name given the conglomerate id of the index.
	 * 
	 * @param indexCID		conglomerate ID of the index.
	 * 
	 * @return index name of given index.
	 */
	public String getIndexNameFromCID(long indexCID)
	{
		int size = indexCIDS.length;

		if (indexNames == null) 
		{
			return null;
		} 
		
		for (int i = 0; i < size; i++)
		{
			if (indexCIDS[i] == indexCID)
				return indexNames[i];
		}
		return null;
	}
			
	public String[] getIndexNames()
	{
		return indexNames;
	}

	/**
	 * Does this constant action modify the passed in table
	 * uuid?  By modify we mean add or drop things tied to
	 * this table (e.g. index, trigger, constraint).  Things
	 * like views or spses that reference this table don't
	 * count.
	 *
	 * @param tableId on error
	 *
	 */
	public boolean modifiesTableId(UUID tableId)
	{
		return ((tableId != null) && (targetUUID != null)) ?
			tableId.equals(targetUUID) :
			false;
	}
}
 

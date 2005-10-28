/*

   Derby - Class org.apache.derby.impl.sql.execute.IndexChanger

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

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.i18n.MessageService;
import java.util.Properties;

/**
  Perform Index maintenace associated with DML operations for a single index.
  */
public class IndexChanger
{
	private IndexRowGenerator irg;
	//Index Conglomerate ID
	private long indexCID;
	private DynamicCompiledOpenConglomInfo indexDCOCI;
	private StaticCompiledOpenConglomInfo indexSCOCI;
	private String indexName;
	private ConglomerateController baseCC;
	private TransactionController tc;
	private int lockMode;
	private FormatableBitSet baseRowReadMap;

	private ConglomerateController indexCC = null;
	private ScanController indexSC = null;

	private LanguageConnectionContext lcc;

	//
	//Index rows used by this module to perform DML.
	private ExecIndexRow ourIndexRow = null;
	private ExecIndexRow ourUpdatedIndexRow = null;

	private TemporaryRowHolderImpl	rowHolder = null;
	private boolean					rowHolderPassedIn;
	private int						isolationLevel;
	private Activation				activation;
	private boolean					ownIndexSC = true;

	/**
	  Create an IndexChanger

	  @param irg the IndexRowGenerator for the index.
	  @param indexCID the conglomerate id for the index.
	  @param indexSCOCI the SCOCI for the idexes. 
	  @param indexDCOCI the DCOCI for the idexes. 
	  @param baseCC the ConglomerateController for the base table.
	  @param tc			The TransactionController
	  @param lockMode	The lock mode (granularity) to use
	  @param baseRowReadMap Map of columns read in.  1 based.
	  @param isolationLevel	Isolation level to use.
	  @param activation	Current activation

	  @exception StandardException		Thrown on error
	  */
	public IndexChanger
	(
		IndexRowGenerator 		irg,
		long 					indexCID,
	    StaticCompiledOpenConglomInfo indexSCOCI,
		DynamicCompiledOpenConglomInfo indexDCOCI,
		String					indexName,
		ConglomerateController	baseCC,
		TransactionController 	tc,
		int 					lockMode,
		FormatableBitSet					baseRowReadMap,
		int						isolationLevel,
		Activation				activation
	)
		 throws StandardException
	{
		this.irg = irg;
		this.indexCID = indexCID;
		this.indexSCOCI = indexSCOCI;
		this.indexDCOCI = indexDCOCI;
		this.baseCC = baseCC;
		this.tc = tc;
		this.lockMode = lockMode;
		this.baseRowReadMap = baseRowReadMap;
		this.rowHolderPassedIn = false;
		this.isolationLevel = isolationLevel;
		this.activation = activation;
		this.indexName = indexName;

		// activation will be null when called from DataDictionary
		if (activation != null && activation.getIndexConglomerateNumber() == indexCID)
		{
			ownIndexSC = false;
		}
	
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(tc != null, 
				"TransactionController argument to constructor is null");
			SanityManager.ASSERT(irg != null, 
				"IndexRowGenerator argument to constructor is null");
		}
	}

	/**
	 * Set the row holder for this changer to use.
	 * If the row holder is set, it wont bother 
	 * saving copies of rows needed for deferred
	 * processing.  Also, it will never close the
	 * passed in rowHolder.
	 *
	 * @param rowHolder	the row holder
	 */
	public void setRowHolder(TemporaryRowHolderImpl rowHolder)
	{
		this.rowHolder = rowHolder;
		rowHolderPassedIn = (rowHolder != null);
	}

	/**
	 * Propagate the heap's ConglomerateController to
	 * this index changer.
	 *
	 * @param baseCC	The heap's ConglomerateController.
	 *
	 * @return Nothing.
	 */
	public void setBaseCC(ConglomerateController baseCC)
	{
		this.baseCC = baseCC;
	}

	/**
	  Set the column values for 'ourIndexRow' to refer to 
	  a base table row and location provided by the caller.
	  The idea here is to 
	  @param baseRow a base table row.
	  @param baseRowLoc baseRowLoc baseRow's location
	  @exception StandardException		Thrown on error
	  */
	private void setOurIndexRow(ExecRow baseRow,
								RowLocation baseRowLoc)
		 throws StandardException
	{
			if (ourIndexRow == null)
				ourIndexRow = irg.getIndexRowTemplate();
		
			irg.getIndexRow(baseRow, baseRowLoc, ourIndexRow, baseRowReadMap);
	}

	/**
	  Set the column values for 'ourUpdatedIndexRow' to refer to 
	  a base table row and location provided by the caller.
	  The idea here is to 
	  @param baseRow a base table row.
	  @param baseRowLoc baseRowLoc baseRow's location
	  @exception StandardException		Thrown on error
	  */
	private void setOurUpdatedIndexRow(ExecRow baseRow,
								RowLocation baseRowLoc)
		 throws StandardException
	{
			if (ourUpdatedIndexRow == null)
				ourUpdatedIndexRow = irg.getIndexRowTemplate();
		
			irg.getIndexRow(baseRow, baseRowLoc, ourUpdatedIndexRow, baseRowReadMap);
	}

	/**
	 * Determine whether or not any columns in the current index
	 * row are being changed by the update.  No need to update the
	 * index if no columns changed.
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	private boolean indexRowChanged()
		throws StandardException
	{
		int numColumns = ourIndexRow.nColumns();
		for (int index = 1; index <= numColumns; index++)
		{
			DataValueDescriptor oldOrderable = ourIndexRow.getColumn(index);
			DataValueDescriptor newOrderable = ourUpdatedIndexRow.getColumn(index);
			if (! (oldOrderable.compare(DataValueDescriptor.ORDER_OP_EQUALS, newOrderable, true, true)))
			{
				return true;
			}
		}
		return false;
	}

	private ExecIndexRow getDeferredIndexRowTemplate(ExecRow baseRow,
													RowLocation baseRowLoc)
		 throws StandardException
	{
		ExecIndexRow	template;

		template = irg.getIndexRowTemplate();

		irg.getIndexRow(baseRow, baseRowLoc, template, baseRowReadMap);

		return template;
	}

	/**
	  Position our index scan to 'ourIndexRow'.

	  <P>This creates the scan the first time it is called.

	  @exception StandardException		Thrown on error
	  */
	private void setScan()
		 throws StandardException
	{
		/* Get the SC from the activation if re-using */
		if (! ownIndexSC)
		{
			indexSC = activation.getIndexScanController();
		}
		else if (indexSC == null)
		{
			RowLocation templateBaseRowLocation = baseCC.newRowLocationTemplate();
			/* DataDictionary doesn't have compiled info */
			if (indexSCOCI == null)
			{
				indexSC = 
		            tc.openScan(
			              indexCID,
				          false,                       /* hold */
					      TransactionController.OPENMODE_FORUPDATE, /* forUpdate */
						  lockMode,
	                      isolationLevel,
		                  (FormatableBitSet)null,					/* all fields */
			              ourIndexRow.getRowArray(),    /* startKeyValue */
				          ScanController.GE,            /* startSearchOp */
					      null,                         /* qualifier */
						  ourIndexRow.getRowArray(),    /* stopKeyValue */
						ScanController.GT             /* stopSearchOp */
	                      );
			}
			else
			{
				indexSC = 
		            tc.openCompiledScan(
				          false,                       /* hold */
					      TransactionController.OPENMODE_FORUPDATE, /* forUpdate */
						  lockMode,
	                      isolationLevel,
		                  (FormatableBitSet)null,					/* all fields */
			              ourIndexRow.getRowArray(),    /* startKeyValue */
				          ScanController.GE,            /* startSearchOp */
					      null,                         /* qualifier */
						  ourIndexRow.getRowArray(),    /* stopKeyValue */
						  ScanController.GT,             /* stopSearchOp */
						  indexSCOCI,
						  indexDCOCI
	                      );
            }
		}
		else
		{
			indexSC.reopenScan(
							   ourIndexRow.getRowArray(),			/* startKeyValue */
							   ScanController.GE, 	/* startSearchOperator */
							   null,	            /* qualifier */
							   ourIndexRow.getRowArray(),			/* stopKeyValue */
							   ScanController.GT	/* stopSearchOperator */
							   );
		}
	}

	/**
	  Close our index Conglomerate Controller
	  */
	private void closeIndexCC()
        throws StandardException
	{
		if (indexCC != null)
			indexCC.close();
		indexCC = null;
	}

	/**
	  Close our index ScanController.
	  */
	private void closeIndexSC()
        throws StandardException
	{
		/* Only consider closing index SC if we own it. */
		if (ownIndexSC && indexSC != null)
		{
			indexSC.close();
			indexSC = null;
		}
	}

	/**
	  Delete a row from our index. This assumes our index ScanController
	  is positioned before the row by setScan if we own the SC, otherwise
	  it is positioned on the row by the underlying index scan.
	  
	  <P>This verifies the row exists and is unique.
	  
	  @exception StandardException		Thrown on error
	  */
	private void doDelete()
		 throws StandardException
	{
		if (ownIndexSC)
		{
			if (! indexSC.next())
			{
                // This means that the entry for the index does not exist, this
                // is a serious problem with the index.  Past fixed problems
                // like track 3703 can leave db's in the field with this problem
                // even though the bug in the code which caused it has long 
                // since been fixed.  Then the problem can surface months later
                // when the customer attempts to upgrade.  By "ignoring" the
                // missing row here the problem is automatically "fixed" and
                // since the code is trying to delete the row anyway it doesn't
                // seem like such a bad idea.  It also then gives a tool to 
                // support to be able to fix some system catalog problems where
                // they can delete the base rows by dropping the system objects
                // like stored statements.

				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT(
                        "Index row "+RowUtil.toString(ourIndexRow)+
                        " not found in conglomerateid " + indexCID +
                        "Current scan = " + indexSC);

                Object[] args = new Object[2];
                args[0] = ourIndexRow.getRowArray()[ourIndexRow.getRowArray().length - 1];
                args[1] = new Long(indexCID);

                Monitor.getStream().println(MessageService.getCompleteMessage(
                    SQLState.LANG_IGNORE_MISSING_INDEX_ROW_DURING_DELETE, 
                    args));

                // just return indicating the row has been deleted.
                return;
			}
		}

        indexSC.delete();
	}

	/**
	  Insert a row into our indes.
	  
	  <P>This opens our index ConglomeratController the first time it
	  is called. 
	  
	  @exception StandardException		Thrown on error
	  */
	private void doInsert()
		 throws StandardException
	{
		insertAndCheckDups(ourIndexRow);
	}

	/**
	  Insert a row into the temporary conglomerate
	  
	  <P>This opens our deferred ConglomeratController the first time it
	  is called.
	  
	  @exception StandardException		Thrown on error
	  */
	private void doDeferredInsert()
		 throws StandardException
	{
		if (rowHolder == null)
		{
			Properties properties = new Properties();

			// Get the properties on the index
			openIndexCC().getInternalTablePropertySet(properties);

			/*
			** Create our row holder.  it is ok to skip passing
			** in the result description because if we don't already
			** have a row holder, then we are the only user of the
			** row holder (the description is needed when the row
			** holder is going to be handed to users for triggers).
			*/
			rowHolder = new TemporaryRowHolderImpl(tc, properties, (ResultDescription)null);
		}

		/*
		** If the user of the IndexChanger already
		** had a row holder, then we don't need to
		** bother saving deferred inserts -- they
		** have already done so.	
		*/
		if (!rowHolderPassedIn)
		{
			rowHolder.insert(ourIndexRow);
		}
	}

	/**
	 * Insert the given row into the given conglomerate and check for duplicate
	 * key error.
	 *
	 * @param row	The row to insert
	 *
	 * @exception StandardException		Thrown on duplicate key error
	 */
	private void insertAndCheckDups(ExecIndexRow row)
				throws StandardException
	{
		openIndexCC();

		int insertStatus = indexCC.insert(row.getRowArray());

		if (insertStatus == ConglomerateController.ROWISDUPLICATE)
		{
			/*
			** We have a duplicate key error. 
			*/
			String indexOrConstraintName = indexName;
			// now get table name, and constraint name if needed
			LanguageConnectionContext lcc =
                			activation.getLanguageConnectionContext();
			DataDictionary dd = lcc.getDataDictionary();
			//get the descriptors
			ConglomerateDescriptor cd = dd.getConglomerateDescriptor(indexCID);

			UUID tableID = cd.getTableID();
			TableDescriptor td = dd.getTableDescriptor(tableID);
			String tableName = td.getName();
			
			if (indexOrConstraintName == null) // no index name passed in
			{
				ConstraintDescriptor conDesc = dd.getConstraintDescriptor(td,
                                                                      cd.getUUID());
				indexOrConstraintName = conDesc.getConstraintName();
			}		

			StandardException se = 
				StandardException.newException(
				SQLState.LANG_DUPLICATE_KEY_CONSTRAINT, indexOrConstraintName, tableName);
			throw se;
		}
		if (SanityManager.DEBUG)
		{
			if (insertStatus != 0)
			{
				SanityManager.THROWASSERT("Unknown insert status " + insertStatus);
			}
		}
	}


	/**
	 * Open the ConglomerateController for this index if it isn't open yet.
	 *
	 * @return The ConglomerateController for this index.
	 *
	 * @exception StandardException		Thrown on duplicate key error
	 */
	private ConglomerateController openIndexCC()
		throws StandardException
	{
		if (indexCC == null)
		{
			/* DataDictionary doesn't have compiled info */
			if (indexSCOCI == null)
			{
				indexCC = 
		            tc.openConglomerate(
						indexCID,
                        false,
			            (TransactionController.OPENMODE_FORUPDATE |
				         TransactionController.OPENMODE_BASEROW_INSERT_LOCKED),
					    lockMode,
                        isolationLevel);
			}
			else
			{
				indexCC = 
		            tc.openCompiledConglomerate(
                        false,
			            (TransactionController.OPENMODE_FORUPDATE |
				         TransactionController.OPENMODE_BASEROW_INSERT_LOCKED),
					    lockMode,
                        isolationLevel,
						indexSCOCI,
						indexDCOCI);
			}
		}

		return indexCC;
	}

	/**
	  Open this IndexChanger.

	  @exception StandardException		Thrown on error
	  */
	public void open()
		 throws StandardException
	{
	}

	/**
	  Perform index maintenance to support a delete of a base table row.

	  @param baseRow the base table row.
	  @param baseRowLocation the base table row's location.
	  @exception StandardException		Thrown on error
	  */
	public void delete(ExecRow baseRow,
					   RowLocation baseRowLocation)
		 throws StandardException
	{
		setOurIndexRow(baseRow, baseRowLocation);
		setScan();
		doDelete();
	}

	/**
	  Perform index maintenance to support an update of a base table row.

	  @param ef	                ExecutionFactory to use in case of cloning
	  @param oldBaseRow         the old image of the base table row.
	  @param newBaseRow         the new image of the base table row.
	  @param baseRowLocation    the base table row's location.

	  @exception StandardException		Thrown on error
	  */
	public void update(ExecRow oldBaseRow,
					   ExecRow newBaseRow,
					   RowLocation baseRowLocation
					   )
		 throws StandardException
	{
		setOurIndexRow(oldBaseRow, baseRowLocation);
		setOurUpdatedIndexRow(newBaseRow, baseRowLocation);

		/* We skip the update in the degenerate case
		 * where none of the key columns changed.
		 * (From an actual customer case.)
		 */
		if (indexRowChanged())
		{
			setScan();
			doDelete();
			insertForUpdate(newBaseRow, baseRowLocation);
		}
	}

	/**
	  Perform index maintenance to support an insert of a base table row.

	  @param baseRow            the base table row.
	  @param baseRowLocation    the base table row's location.

	  @exception StandardException		Thrown on error
	  */
	public void insert(ExecRow newRow, RowLocation baseRowLocation)
		 throws StandardException
	{
		setOurIndexRow(newRow, baseRowLocation);
		doInsert();
	}

	/**
	  If we're updating a unique index, the inserts have to be
	  deferred.  This is to avoid uniqueness violations that are only
	  temporary.  If we do all the deletes first, only "true" uniqueness
	  violations can happen.  We do this here, rather than in open(),
	  because this is the only operation that requires deferred inserts,
	  and we only want to create the conglomerate if necessary.

	  @param ef		            ExecutionFactory to use in case of cloning
	  @param baseRow            the base table row.
	  @param baseRowLocation    the base table row's location.

	  @exception StandardException		Thrown on error
	*/
	void insertForUpdate(ExecRow newRow, RowLocation baseRowLocation)
		 throws StandardException
	{
		setOurIndexRow(newRow, baseRowLocation);

		if (irg.isUnique())
		{
			doDeferredInsert();
		}
		else
		{
			doInsert();
		}
	}

	/**
	  Finish doing the changes for this index.  This is intended for deferred
	  inserts for unique indexes.  It has no effect unless we are doing an
	  update of a unique index.

	  @exception StandardException		Thrown on error
	 */
	public void finish()
		throws StandardException
	{
		ExecRow			deferredRow;
		ExecIndexRow	deferredIndexRow = new IndexRow();

		/* Deferred processing only necessary for unique indexes */
		if (rowHolder != null)
		{
			CursorResultSet rs = rowHolder.getResultSet();
			try
			{
				rs.open();
				while ((deferredRow = rs.getNextRow()) != null)
				{
					if (SanityManager.DEBUG)
					{
						if (!(deferredRow instanceof ExecIndexRow))
						{
							SanityManager.THROWASSERT("deferredRow isn't an instance "+
								"of ExecIndexRow as expected. "+
								"It is an "+deferredRow.getClass().getName());
						}
					}
					insertAndCheckDups((ExecIndexRow)deferredRow);
				}
			}
			finally
			{
				rs.close();

				/*
				** If row holder was passed in, let the
				** client of this method clean it up.
				*/
				if (!rowHolderPassedIn)
				{
					rowHolder.close();
				}
			}
		}
	}

	/**
	  Close this IndexChanger.

	  @exception StandardException		Thrown on error
	  */
	public void close()
		throws StandardException
	{
		closeIndexCC();
		closeIndexSC();
		if (rowHolder != null && !rowHolderPassedIn)
		{
			rowHolder.close();
		}
		baseCC = null;
	}
}

/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.TableDescriptor

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

package org.apache.derby.iapi.sql.dictionary;

import java.security.PrivilegedAction;
import java.security.AccessController;
import java.util.List;

import org.apache.derby.catalog.Dependable;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.util.IdUtil;

/**
 * This class represents a table descriptor. The external interface to this
 * class is:
 <p>
		<ol>
		<li>external interface </li>
		<li>public String	getSchemaName();</li>
		<li>public String	getQualifiedName();</li>
		<li>public int	getTableType();</li>
		<li>public long getHeapConglomerateId() throws StandardException;</li>
		<li>public int getNumberOfColumns();		</li>
		<li>public FormatableBitSet getReferencedColumnMap();</li>
		<li>public void setReferencedColumnMap(FormatableBitSet referencedColumnMap);</li>
		<li>public int getMaxColumnID() throws StandardException;</li>
		<li>public void	setUUID(UUID uuid);</li>
		<li>public char	getLockGranularity();</li>
		<li>public void	setTableName(String newTableName);</li>
		<li>public void	setLockGranularity(char lockGranularity);</li>
		<li>public ExecRow getEmptyExecRow( ContextManager cm) throws StandardException;</li>
		<li>public boolean tableNameEquals(String otherSchemaName, String otherTableName);</li>
		<li>public ReferencedKeyConstraintDescriptor getPrimaryKey() throws StandardException;</li>
		<li>public void removeConglomerateDescriptor(ConglomerateDescriptor cd)	throws StandardException;</li>
		<li>public void removeConstraintDescriptor(ConstraintDescriptor cd)	throws StandardException;</li>
		<li>public void getAffectedIndexes(...) throws StandardException;</li>
		<li>public void	getAllRelevantTriggers(...) throws StandardException;</li>
		<li>public void getAllRelevantConstraints(...) throws StandardException</li>
		<li>public ColumnDescriptorList getColumnDescriptorList();</li>
		<li> public String[] getColumnNamesArray();</li>
		<li>public long[]   getAutoincIncrementArray();</li>
		<li>public ColumnDescriptor	getColumnDescriptor(String columnName);</li>
		<li>public ColumnDescriptor	getColumnDescriptor(int columnNumber);</li>
		<li>public ConglomerateDescriptor[]	getConglomerateDescriptors() throws StandardException;</li>
		<li>public ConglomerateDescriptor	getConglomerateDescriptor(long conglomerateNumber)	throws StandardException;</li>
		<li>public ConglomerateDescriptor	getConglomerateDescriptor(UUID conglomerateUUID) throws StandardException;</li>
		<li>public	IndexLister	getIndexLister() throws StandardException;</li>
		<li>public ViewDescriptor getViewDescriptor();</li>
		<li>public boolean tableHasAutoincrement();</li>
		<li>public boolean statisticsExist(ConglomerateDescriptor cd) throws StandardException;</li>
		<li>public double selectivityForConglomerate(...)throws StandardException;</li>
		</ol>
	<p>
	*
	*/

public class TableDescriptor extends UniqueSQLObjectDescriptor
	implements Provider, Dependent
{
	public static final int BASE_TABLE_TYPE = 0;
	public static final int SYSTEM_TABLE_TYPE = 1;
	public static final int VIEW_TYPE = 2;
	public static final int GLOBAL_TEMPORARY_TABLE_TYPE = 3;
	public static final int SYNONYM_TYPE = 4;
	public static final int VTI_TYPE = 5;

	public static final char	ROW_LOCK_GRANULARITY = 'R';
	public static final char	TABLE_LOCK_GRANULARITY = 'T';
	public static final char	DEFAULT_LOCK_GRANULARITY = ROW_LOCK_GRANULARITY;

    // Constants for the automatic index statistics update feature.
    public static final int ISTATS_CREATE_THRESHOLD;
    public static final int ISTATS_ABSDIFF_THRESHOLD;
    public static final double ISTATS_LNDIFF_THRESHOLD;
    static {
        ISTATS_CREATE_THRESHOLD = PropertyUtil.getSystemInt(
                Property.STORAGE_AUTO_INDEX_STATS_DEBUG_CREATE_THRESHOLD,
                Property.STORAGE_AUTO_INDEX_STATS_DEBUG_CREATE_THRESHOLD_DEFAULT
                );
        ISTATS_ABSDIFF_THRESHOLD = PropertyUtil.getSystemInt(
            Property.STORAGE_AUTO_INDEX_STATS_DEBUG_ABSDIFF_THRESHOLD,
            Property.STORAGE_AUTO_INDEX_STATS_DEBUG_ABSDIFF_THRESHOLD_DEFAULT);
        double tmpLog2Diff =
            Property.STORAGE_AUTO_INDEX_STATS_DEBUG_LNDIFF_THRESHOLD_DEFAULT;
        try {
            String tmpStr = PropertyUtil.getSystemProperty(
                    Property.STORAGE_AUTO_INDEX_STATS_DEBUG_LNDIFF_THRESHOLD);
            if (tmpStr != null) {
                tmpLog2Diff = Double.parseDouble(tmpStr);
            }
        } catch (NumberFormatException nfe) {
            // Ignore, use the default.
        }
        ISTATS_LNDIFF_THRESHOLD = tmpLog2Diff;
    }

	/**
	*/

	// implementation
	private char					lockGranularity;
	private boolean					onCommitDeleteRows; //true means on commit delete rows, false means on commit preserve rows of temporary table.
	private boolean					onRollbackDeleteRows; //true means on rollback delete rows. This is the only value supported.
    private boolean                 indexStatsUpToDate = true;
    private String                  indexStatsUpdateReason;
	SchemaDescriptor				schema;
	String							tableName;
	UUID							oid;
	int								tableType;

    /**
     * <p>
     * The id of the heap conglomerate for the table described by this
     * instance. The value -1 means it's uninitialized, in which case it
     * will be initialized lazily when {@link #getHeapConglomerateId()} is
     * called.
     * </p>
     *
     * <p>
     * It is declared volatile to ensure that concurrent callers of
     * {@code getHeapConglomerateId()} while {@code heapConglomNumber} is
     * uninitialized, will either see the value -1 or the fully initialized
     * conglomerate number, and never see a partially initialized value
     * (as was the case in DERBY-5358 because reads/writes of a long field are
     * not guaranteed to be atomic unless the field is declared volatile).
     * </p>
     */
    private volatile long           heapConglomNumber = -1;

    ColumnDescriptorList            columnDescriptorList;
	ConglomerateDescriptorList		conglomerateDescriptorList;
	ConstraintDescriptorList		constraintDescriptorList;
    private TriggerDescriptorList   triggerDescriptorList;
	ViewDescriptor					viewDescriptor;

	private FormatableBitSet referencedColumnMapGet() {

        LanguageConnectionContext lcc =
            (LanguageConnectionContext)getContextOrNull(
                LanguageConnectionContext.CONTEXT_ID);

        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(lcc != null);
        }

        return lcc.getReferencedColumnMap(this);

	}

	private void referencedColumnMapPut
		(FormatableBitSet newReferencedColumnMap) {

        LanguageConnectionContext lcc =
            (LanguageConnectionContext)getContextOrNull(
                LanguageConnectionContext.CONTEXT_ID);

        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(lcc != null || newReferencedColumnMap == null);
        }

        // This method is called with a null argument at database
        // creation time when there is no lcc, cf stack trace in the
        // JIRA for DERBY-4895, we can safely ignore that, as there
        // exists no referencedColumnMap yet.
        if (lcc != null) {
            lcc.setReferencedColumnMap(this, newReferencedColumnMap);
        }
	}

	/** A list of statistics pertaining to this table-- 
	 */
	private List<StatisticsDescriptor>					statisticsDescriptorList;

	/**
	 * Constructor for a TableDescriptor (this is for a temporary table).
	 *
	 * @param dataDictionary		The data dictionary that this descriptor lives in
	 * @param tableName	The name of the temporary table
	 * @param schema	The schema descriptor for this table.
	 * @param tableType	An integer identifier for the type of the table : declared global temporary table
	 * @param onCommitDeleteRows	If true, on commit delete rows else on commit preserve rows of temporary table.
	 * @param onRollbackDeleteRows	If true, on rollback, delete rows from temp tables which were logically modified. true is the only supported value
	 */

	public TableDescriptor
	(
		DataDictionary		dataDictionary,
		String				tableName,
		SchemaDescriptor	schema,
		int					tableType,
		boolean				onCommitDeleteRows,
		boolean				onRollbackDeleteRows
    )
	{
		this(dataDictionary, tableName, schema, tableType, '\0');

		this.onCommitDeleteRows = onCommitDeleteRows;
		this.onRollbackDeleteRows = onRollbackDeleteRows;
	}

	/**
	 * Constructor for a TableDescriptor.
	 *
	 * @param dataDictionary		The data dictionary that this descriptor lives in
	 * @param tableName	The name of the table
	 * @param schema	The schema descriptor for this table.
	 * @param tableType	An integer identifier for the type of the table
	 *			(base table, view, etc.)
	 * @param lockGranularity	The lock granularity.
	 */

	public TableDescriptor
	(
		DataDictionary		dataDictionary,
		String				tableName,
		SchemaDescriptor	schema,
		int					tableType,
		char				lockGranularity
    )
	{
		super( dataDictionary );

		this.schema = schema;
		this.tableName = tableName;
		this.tableType = tableType;
		this.lockGranularity = lockGranularity;

		this.conglomerateDescriptorList = new ConglomerateDescriptorList();
		this.columnDescriptorList = new ColumnDescriptorList();
		this.constraintDescriptorList = new ConstraintDescriptorList();
        this.triggerDescriptorList = new TriggerDescriptorList();
	}

	//
	// TableDescriptor interface
	//

	/**
	 * Gets the name of the schema the table lives in.
	 *
	 * @return	A String containing the name of the schema the table
	 *		lives in.
	 */
	public String	getSchemaName()
	{
		return schema.getSchemaName();
	}

	/**
	 * Gets the SchemaDescriptor for this TableDescriptor.
	 *
	 * @return SchemaDescriptor	The SchemaDescriptor.
	 */
	public SchemaDescriptor getSchemaDescriptor()
	{
		return schema;
	}

	/**
	 * Gets the name of the table.
	 *
	 * @return	A String containing the name of the table.
	 */
	public String	getName()
	{
		return tableName;
	}

	/**
	 * Sets the the table name in case of rename table.
	 *
	 * This is used only by rename table
	 * @param newTableName	The new table name.
	 */
	public void	setTableName(String newTableName)
	{
		this.tableName = newTableName;
	}

	/**
	 * Gets the full, qualified name of the table.
	 *
	 * @return	A String containing the name of the table.
	 */
	public String	getQualifiedName()
	{
        return IdUtil.mkQualifiedName(getSchemaName(), getName());
	}

	/**
	 * Gets the UUID of the table.
	 *
	 * @return	The UUID of the table.
	 */
	public UUID	getUUID()
	{
		return oid;
	}

	/**
	 * Gets an identifier telling what type of table this is
	 * (base table, declared global temporary table, view, etc.)
	 *
	 * @return	An identifier telling what type of table this is.
	 */
	public int	getTableType()
	{
		return tableType;
	}

	/**
	 * Gets the id for the heap conglomerate of the table.
	 * There may also be keyed conglomerates, these are
	 * stored separately in the conglomerates table.
	 *
	 * @return the id of the heap conglomerate for the table.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public long getHeapConglomerateId()
			throws StandardException
	{
		ConglomerateDescriptor cd = null;

		/* If we've already cached the heap conglomerate number, then
		 * simply return it.
		 */
		if (heapConglomNumber != -1)
		{
			return heapConglomNumber;
		}

		ConglomerateDescriptor[] cds = getConglomerateDescriptors();

		for (int index = 0; index < cds.length; index++)
		{
			cd = cds[index];
			if ( ! cd.isIndex())
				break;
		}

		if (SanityManager.DEBUG)
		{
			if (cd == null)
			{
				SanityManager.THROWASSERT(
					"cd is expected to be non-null for " + tableName);
			}

			if (cd.isIndex())
			{
				SanityManager.THROWASSERT(
					"Did not find heap conglomerate for " + tableName);
			}
		}

		heapConglomNumber = cd.getConglomerateNumber();

		return heapConglomNumber;
	}

	/**
	 * Gets the number of columns in the table.
	 *
	 * @return the number of columns in the table.
	 *
	 */
	public int getNumberOfColumns()
	{
		return getColumnDescriptorList().size();
	}

	/**
	 * Get the referenced column map of the table.
	 *
	 * @return the referencedColumnMap of the table.
	 *
	 */
	public FormatableBitSet getReferencedColumnMap()
	{
		return referencedColumnMapGet();
	}

	/**
	 * Set the referenced column map of the table.
	 *
	 * @param	referencedColumnMap	FormatableBitSet of referenced columns.
	 *
	 */
	public void setReferencedColumnMap(FormatableBitSet referencedColumnMap)
	{
		referencedColumnMapPut(referencedColumnMap);
	}

	/**
	 * Given a list of columns in the table, construct a bit  map of those
	 * columns' ids.
     *
     * @param cdl list of columns whose positions we want to record in the bit map
	 */
	public FormatableBitSet makeColumnMap( ColumnDescriptorList cdl )
	{
		FormatableBitSet    result = new FormatableBitSet( columnDescriptorList.size() + 1 );
        int                         count = cdl.size();

        for ( int i = 0; i < count; i++ )
        {
            ColumnDescriptor    cd = cdl.elementAt( i );

            result.set( cd.getPosition() );
        }

        return result;
	}

	/**
	 * Gets the highest column id in the table. For now this is the same as
	 * the number of columns. However, in the future, after we implement
	 * ALTER TABLE DROP COLUMN, this correspondence won't hold any longer.
	 *
	 * @return the highest column ID in the table
 	 *
 	 * @exception StandardException		Thrown on error
	 */
	public int getMaxColumnID()
		throws StandardException
	{
		int					maxColumnID = 1;

        for (ColumnDescriptor cd : columnDescriptorList)
		{
			maxColumnID = Math.max( maxColumnID, cd.getPosition() );
		}

		return maxColumnID;
	}

	/**
	 * Sets the UUID of the table
	 *
	 * @param oid	The UUID of the table to be set in the descriptor
	 */
	public void setUUID(UUID oid)
	{
		this.oid = oid;
	}

	/**
	 * Gets the lock granularity for the table.
	 *
	 * @return	A char representing the lock granularity for the table.
	 */
	public char	getLockGranularity()
	{
		return lockGranularity;
	}

	/**
	 * Sets the lock granularity for the table to the specified value.
	 *
	 * @param lockGranularity	The new lockGranularity.
	 */
	public void	setLockGranularity(char lockGranularity)
	{
		this.lockGranularity = lockGranularity;
	}

	/**
	 * Gets the on rollback behavior for the declared global temporary table.
	 *
	 * @return	A boolean representing the on rollback behavior for the declared global temporary table.
	 */
	public boolean	isOnRollbackDeleteRows()
	{
		return onRollbackDeleteRows;
	}

	/**
	 * Gets the on commit behavior for the declared global temporary table.
	 *
	 * @return	A boolean representing the on commit behavior for the declared global temporary table.
	 */
	public boolean	isOnCommitDeleteRows()
	{
		return onCommitDeleteRows;
	}

	/**
	 * Sets the heapConglomNumber to -1 for temporary table since the table was dropped and recreated at the commit time
	 * and hence its conglomerate id has changed. This is used for temporary table descriptors only
	 */
	public void	resetHeapConglomNumber()
	{
		if (SanityManager.DEBUG)
		{
			if (tableType != TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
			{
				SanityManager.THROWASSERT("tableType expected to be TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE, not " +
				tableType);
			}
		}
		heapConglomNumber = -1;
	}

	/**
	 * Gets an ExecRow for rows stored in the table this describes.
	 *
	 *
	 *	@return	the row.
	 *  @exception StandardException		Thrown on failure
	 */
	public ExecRow getEmptyExecRow()
		 throws StandardException
	{
		int							columnCount = getNumberOfColumns();
		ExecRow result =
            getDataDictionary().getExecutionFactory().getValueRow(columnCount);

		for (int index = 0; index < columnCount; index++)
		{
            ColumnDescriptor cd = columnDescriptorList.elementAt(index);
			//String name = column.getColumnName();
			DataValueDescriptor dataValue = cd.getType().getNull();
			result.setColumn(index + 1, dataValue);
		}
		return result;
	}

    /**
     * Return an array of collation ids for this table.
     * <p>
     * Return an array of collation ids, one for each column in the
     * columnDescriptorList.  This is useful for passing collation id info
     * down to store, for instance in createConglomerate().
     *
     * This is only expected to get called during ddl, so object allocation
     * is ok. 
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public int[] getColumnCollationIds()
		throws StandardException
    {
        int[] collation_ids = new int[getNumberOfColumns()]; 

		for (int index = 0; index < collation_ids.length; index++)
		{
            ColumnDescriptor cd = columnDescriptorList.elementAt(index);

            collation_ids[index] = cd.getType().getCollationType();

		}
		return(collation_ids);

    }

	/**
	 * Gets the conglomerate descriptor list
	 *
	 * @return	The conglomerate descriptor list for this table descriptor
	 */
	public ConglomerateDescriptorList getConglomerateDescriptorList()
	{
		return conglomerateDescriptorList;
	}

	/**
	 * Gets the view descriptor for this TableDescriptor.
	 * 
	 * @return ViewDescriptor	The ViewDescriptor, if any.
	 */
	public ViewDescriptor getViewDescriptor()
	{
		return viewDescriptor;
	}

	/**
	 * Set (cache) the view descriptor for this TableDescriptor
	 *
	 * @param viewDescriptor	The view descriptor to cache.
	 */
	public void setViewDescriptor(ViewDescriptor viewDescriptor)
	{
		if (SanityManager.DEBUG)
		{
			if (tableType != TableDescriptor.VIEW_TYPE)
			{
				SanityManager.THROWASSERT("tableType expected to be TableDescriptor.VIEW_TYPE, not " +
				tableType);
			}
		}
		this.viewDescriptor = viewDescriptor;
	}

	/**
	 * Is this provider persistent?  A stored dependency will be required
	 * if both the dependent and provider are persistent.
	 *
	 * @return boolean              Whether or not this provider is persistent.
	 */
    @Override
	public boolean isPersistent()
	{
		if (tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
			return false;
		else
			return(super.isPersistent());
	}

	/**
	 * Is this descriptor represents a synonym?
	 *
	 * @return boolean              Whether or not this represents a synonym
	 */
	public boolean isSynonymDescriptor()
	{
		if (tableType == TableDescriptor.SYNONYM_TYPE)
			return true;
		return false;
	}

	/**
	 * Gets the number of indexes on the table, including the backing indexes.
	 *
	 * @return the number of columns in the table.
     * @see #getQualifiedNumberOfIndexes
	 */
	public int getTotalNumberOfIndexes()
		throws StandardException
	{
		return getQualifiedNumberOfIndexes(0, false);
	}

    /**
     * Returns the number of indexes matching the criteria.
     *
     * @param minColCount the minimum number of ordered columns in the indexes
     *      we want to count
     * @param nonUniqeTrumpsColCount if {@code true} a non-unique index will be
     *      included in the count even if it has less than {@code minColCount}
     *      ordered columns
     * @return Number of matching indexes.
     * @see #getTotalNumberOfIndexes()
     */
    public int getQualifiedNumberOfIndexes(int minColCount,
                                           boolean nonUniqeTrumpsColCount) {
        int matches = 0;
        for (ConglomerateDescriptor cd : conglomerateDescriptorList) {
            if (cd.isIndex()) {
                IndexRowGenerator irg = cd.getIndexDescriptor();
                if (irg.numberOfOrderedColumns() >= minColCount ||
                        (nonUniqeTrumpsColCount && !irg.isUnique())) {
                    matches++;
                }
            }
        }
        return matches;
    }

    /**
	  *	Builds a list of all triggers which are relevant to a
	  *	given statement type, given a list of updated columns.
	  *
	  *	@param	statementType		defined in StatementType
	  *	@param	changedColumnIds	array of changed columns
	  *	@param	relevantTriggers	IN/OUT. Passed in as an empty list. Filled in as we go.
	  *
	  * @exception StandardException		Thrown on error
	  */
	public	void	getAllRelevantTriggers
	(
		int						statementType,
		int[]					changedColumnIds,
        TriggerDescriptorList   relevantTriggers
    )
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT((statementType == StatementType.INSERT) ||
								 (statementType == StatementType.BULK_INSERT_REPLACE) ||
								 (statementType == StatementType.UPDATE) ||
								 (statementType == StatementType.DELETE),
							"invalid statement type "+statementType);
		}

		DataDictionary				dd = getDataDictionary();

        for (TriggerDescriptor tgr : dd.getTriggerDescriptors(this)) {
            if (tgr.needsToFire(statementType, changedColumnIds)) {
                relevantTriggers.add(tgr);
            }
        }
	}

    /**
	  *	Gets all of the relevant constraints for a statement, given its
	  *	statement type and its list of updated columns.
	  *
	  *	@param	statementType			As defined in StatementType.
	  * @param	changedColumnIds		If null, all columns being changed, otherwise array
	  *									of 1-based column ids for columns being changed
	  *	@param	needsDeferredProcessing	IN/OUT. true if the statement already needs
	  *											deferred processing. set while evaluating this
	  *											routine if a trigger or constraint requires
	  *											deferred processing
	  *	@param	relevantConstraints		IN/OUT. Empty list is passed in. We hang constraints on it as we go.
	  *
	  * @exception StandardException		Thrown on error
	  */
	public	void	getAllRelevantConstraints
	(
		int							statementType,
		int[]						changedColumnIds,
		boolean[]					needsDeferredProcessing,
		ConstraintDescriptorList	relevantConstraints
    )
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT((statementType == StatementType.INSERT) ||
								 (statementType == StatementType.BULK_INSERT_REPLACE) ||
								 (statementType == StatementType.UPDATE) ||
								 (statementType == StatementType.DELETE),
							"invalid statement type "+statementType);
		}

		DataDictionary					dd = getDataDictionary();
		ConstraintDescriptorList		cdl = dd.getConstraintDescriptors(this);
		int cdlSize = cdl.size();

		for (int index = 0; index < cdlSize; index++)
		{
			ConstraintDescriptor cd = cdl.elementAt(index);

			/*
			** For each constraint, figure out if it requires deferred processing.
			** Note that we need to do this on constraints that don't
			** necessarily need to fire -- e.g. for an insert into a
			** a table with a self-referencing constraint, we don't
			** need to check the primary key constraint (assuming it
			** is only referenced by the self-referencing fk on the same
			** table), but we have to run in deferred mode nonetheless
			** (even though we aren't going to check the pk constraint).
			*/
			if (!needsDeferredProcessing[0] &&
				(cd instanceof ReferencedKeyConstraintDescriptor) &&
				(statementType != StatementType.UPDATE &&
				 statementType != StatementType.BULK_INSERT_REPLACE))
			{
				/* For insert (bulk or regular) on a non-published table,
				 * we only need deferred mode if there is a 
				 * self-referencing foreign key constraint.
				 */
				needsDeferredProcessing[0] = ((ReferencedKeyConstraintDescriptor)cd).
									hasSelfReferencingFK(cdl, ConstraintDescriptor.ENABLED);
			}

			if (cd.needsToFire(statementType, changedColumnIds))
			{
				/*
				** For update, if we are updating a referenced key, then
				** we have to do it in deferred mode (in case we update
				** multiple rows).
				*/
				if ((cd instanceof ReferencedKeyConstraintDescriptor) &&
					(statementType == StatementType.UPDATE ||
					 statementType == StatementType.BULK_INSERT_REPLACE))
				{
					needsDeferredProcessing[0] = true;
				}

				relevantConstraints.add(cd);
			}
		}
	}


	//
	// Provider interface
	//

	/**		
		@return the stored form of this provider

			@see Dependable#getDependableFinder
	 */
	public DependableFinder getDependableFinder()
	{
		if (referencedColumnMapGet() == null)
			return	getDependableFinder(StoredFormatIds.TABLE_DESCRIPTOR_FINDER_V01_ID);
		else
			return getColumnDependableFinder
				(StoredFormatIds.COLUMN_DESCRIPTOR_FINDER_V01_ID,
				 referencedColumnMapGet().getByteArray());
	}

	/**
	 * Return the name of this Provider.  (Useful for errors.)
	 *
	 * @return String	The name of this provider.
	 */
	public String getObjectName()
	{
		if (referencedColumnMapGet() == null)
			return tableName;
		else
		{
            StringBuilder name = new StringBuilder();
            name.append(tableName);
			boolean first = true;

            for (ColumnDescriptor cd: columnDescriptorList)
			{
				if (referencedColumnMapGet().isSet(cd.getPosition()))
				{
					if (first)
					{
						name.append("(").append(cd.getColumnName());
						first = false;
					}
					else
						name.append(", ").append(cd.getColumnName());
				}
			}
			if (! first)
				name.append(")");
			return name.toString();
		}
	}

	/**
	 * Get the provider's UUID 
	 *
	 * @return String	The provider's UUID
	 */
	public UUID getObjectID()
	{
		return oid;
	}

	/**
	 * Get the provider's type.
	 *
	 * @return String		The provider's type.
	 */
	public String getClassType()
	{
		return Dependable.TABLE;
	}

	//
	// class interface
	//

	/**
	 * Prints the contents of the TableDescriptor
	 *
	 * @return The contents as a String
	 */
    @Override
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			String tempString =
				"\n" + "schema: " + schema + "\n" +
				"tableName: " + tableName + "\n" +
				"oid: " + oid + " tableType: " + tableType + "\n" +
				"conglomerateDescriptorList: " + conglomerateDescriptorList + "\n" +
				"columnDescriptorList: " + columnDescriptorList + "\n" +
				"constraintDescriptorList: " + constraintDescriptorList + "\n" +
				"heapConglomNumber: " + heapConglomNumber + "\n";
			if (tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
			{
				tempString = tempString + "onCommitDeleteRows: " + "\n" +
					onCommitDeleteRows + "\n";
				tempString = tempString + "onRollbackDeleteRows: " + "\n" +
					onRollbackDeleteRows;
			} else
				tempString = tempString + "lockGranularity: " + lockGranularity;
			return tempString;
		}
		else
		{
			return "";
		}
	}

	/**
	 * Gets the column descriptor list
	 *
	 * @return	The column descriptor list for this table descriptor
	 *
	 */
	public ColumnDescriptorList getColumnDescriptorList()
	{
		return columnDescriptorList;
	}

	/**
	 * Gets the list of columns defined by generation clauses.
	 */
	public ColumnDescriptorList getGeneratedColumns()
	{
        ColumnDescriptorList    fullList = getColumnDescriptorList();
        ColumnDescriptorList    result = new ColumnDescriptorList();
        int                                 count = fullList.size();

        for ( int i = 0; i < count; i++ )
        {
            ColumnDescriptor    cd = fullList.elementAt( i );
            if ( cd.hasGenerationClause() ) { result.add( oid, cd ); }
        }
        
		return result;
	}

	/**
	 * Turn an array of column names into the corresponding 1-based column positions.
	 */
	public int[]    getColumnIDs( String[] names )
	{
        int     count = names.length;
        int[]    result = new int[ count];
        
        for ( int i = 0; i < count; i++ )
        {
            result[ i ] = getColumnDescriptor( names[ i ] ).getPosition();
        }
        
		return result;
	}

	/**
	 * Gets the constraint descriptor list
	 *
	 * @return	The constraint descriptor list for this table descriptor
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstraintDescriptorList getConstraintDescriptorList()
		throws StandardException
	{
		return constraintDescriptorList;
	}

	/**
	 * Sets the constraint descriptor list
	 *
	 * @param newCDL	The new constraint descriptor list for this table descriptor
	 */
	public void setConstraintDescriptorList(ConstraintDescriptorList newCDL)
	{
		constraintDescriptorList = newCDL;
	}

	/**
	 * Empty the constraint descriptor list
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void emptyConstraintDescriptorList()
		throws StandardException
	{
		// Easier just to get a new CDL then to clean out the current one
		this.constraintDescriptorList = new ConstraintDescriptorList();
	}

	/**
	 * Gets the primary key, may return null if no primary key
	 *
	 * @return	The priamry key or null
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ReferencedKeyConstraintDescriptor getPrimaryKey() 
		throws StandardException
	{
		ConstraintDescriptorList cdl = getDataDictionary().getConstraintDescriptors(this);
		
		return cdl.getPrimaryKey();
	}

	/**
	 * Gets the trigger descriptor list
	 *
	 * @return	The trigger descriptor list for this table descriptor
	 *
	 * @exception StandardException		Thrown on failure
	 */
    public TriggerDescriptorList getTriggerDescriptorList()
		throws StandardException
	{
		return triggerDescriptorList;
	}

	/**
	 * Sets the trigger descriptor list
	 *
	 * @param newCDL	The new trigger descriptor list for this table descriptor
	 */
    public void setTriggerDescriptorList(TriggerDescriptorList newCDL)
	{
		triggerDescriptorList = newCDL;
	}

	/**
	 * Empty the trigger descriptor list
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void emptyTriggerDescriptorList()
		throws StandardException
	{
		// Easier just to get a new CDL then to clean out the current one
        this.triggerDescriptorList = new TriggerDescriptorList();
	}

	
	/**
	 * Compare the tables descriptors based on the names.
	 * Null schema names match.
	 *
	 * @param otherTableName	the other table name
	 * @param otherSchemaName	the other schema name
	 *
	 * @return boolean		Whether or not the 2 TableNames are equal.
	 */
	public boolean tableNameEquals(String otherTableName, String otherSchemaName)
	{
		String schemaName = getSchemaName();

		if ((schemaName == null) || 
				 (otherSchemaName == null))
		{
			return tableName.equals(otherTableName);
		}
		else
		{
			return	schemaName.equals(otherSchemaName) &&
					tableName.equals(otherTableName);
		}
	}

	/**
	 * Remove this descriptor
	 *
	 * @param	cd	The conglomerate descriptor 
	 *
	 * @exception StandardException on error
	 */
	public void removeConglomerateDescriptor(ConglomerateDescriptor cd)
		throws StandardException
	{
		conglomerateDescriptorList.dropConglomerateDescriptor(getUUID(), cd);
	}

	/**
	 * Remove this descriptor.  Warning, removes by using object
	 * reference, not uuid.
	 *
	 * @param	cd constraint descriptor 
	 *
	 * @exception StandardException on error
	 */
	public void removeConstraintDescriptor(ConstraintDescriptor cd)
		throws StandardException
	{
		constraintDescriptorList.remove(cd);
	}

	/**
	 * Get the descriptor for a column in the table,
	 * either by the column name or by its ordinal position (column number).
	 * Returns NULL for columns that do not exist.
	 *
	 * @param columnName	A String containing the name of the column
	 *
	 * @return	A ColumnDescriptor describing the column
	 */
	public ColumnDescriptor	getColumnDescriptor(String columnName)
	{
		return columnDescriptorList.getColumnDescriptor(oid, columnName);
	}

	/**
	 * @param columnNumber	The ordinal (1-based) position of the column in the table
	 *
	 * @return	A ColumnDescriptor describing the column
	 */
	public ColumnDescriptor	getColumnDescriptor(int columnNumber)
	{
		return columnDescriptorList.getColumnDescriptor(oid, columnNumber);
	}

	/**
	 * Gets a ConglomerateDescriptor[] to loop through all the conglomerate descriptors
	 * for the table.
	 *
	 * @return	A ConglomerateDescriptor[] for looping through the table's conglomerates
	 */
	public ConglomerateDescriptor[]	getConglomerateDescriptors()
	{

		int size = conglomerateDescriptorList.size();
		ConglomerateDescriptor[] cdls = new ConglomerateDescriptor[size];
		conglomerateDescriptorList.toArray(cdls);
		return cdls;
	}

	/**
	 * Gets a conglomerate descriptor for the given table and conglomerate number.
	 *
	 * @param conglomerateNumber	The conglomerate number
	 *				we're interested in
	 *
	 * @return	A ConglomerateDescriptor describing the requested
	 *		conglomerate. Returns NULL if no such conglomerate.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConglomerateDescriptor	getConglomerateDescriptor(
						long conglomerateNumber)
						throws StandardException
	{
		return conglomerateDescriptorList.getConglomerateDescriptor(conglomerateNumber);
	}

	/**
	 * Gets array of conglomerate descriptors for the given table and
	 * conglomerate number.  More than one descriptors if duplicate indexes
	 * share one conglomerate.
	 *
	 * @param conglomerateNumber	The conglomerate number
	 *				we're interested in
	 *
	 * @return	Array of ConglomerateDescriptors with the requested
	 *		conglomerate number. Returns size 0 array if no such conglomerate.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConglomerateDescriptor[]	getConglomerateDescriptors(
						long conglomerateNumber)
						throws StandardException
	{
		return conglomerateDescriptorList.getConglomerateDescriptors(conglomerateNumber);
	}


	/**
	 * Gets a conglomerate descriptor for the given table and conglomerate UUID String.
	 *
	 * @param conglomerateUUID	The UUID  for the conglomerate
	 *				we're interested in
	 *
	 * @return	A ConglomerateDescriptor describing the requested
	 *		conglomerate. Returns NULL if no such conglomerate.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConglomerateDescriptor	getConglomerateDescriptor(
						UUID conglomerateUUID)
						throws StandardException
	{
		return conglomerateDescriptorList.getConglomerateDescriptor(conglomerateUUID);
	}

	/**
	 * Gets array of conglomerate descriptors for the given table and
	 * conglomerate UUID.  More than one descriptors if duplicate indexes
	 * share one conglomerate.
	 *
	 * @param conglomerateUUID	The conglomerate UUID
	 *				we're interested in
	 *
	 * @return	Array of ConglomerateDescriptors with the requested
	 *		conglomerate UUID. Returns size 0 array if no such conglomerate.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConglomerateDescriptor[]	getConglomerateDescriptors(
						UUID conglomerateUUID)
						throws StandardException
	{
		return conglomerateDescriptorList.getConglomerateDescriptors(conglomerateUUID);
	}

	/**
	 * Gets an object which lists out all the index row generators on a table together
	 * with their conglomerate ids.
	 *
	 * @return	An object to list out the index row generators.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public	IndexLister	getIndexLister()
						throws StandardException
	{
		return new IndexLister( this );
	}

	/**
     * Does the table have an auto-increment column or not?
	 * 
     * @return TRUE if the table has at least one auto-increment column, false
	 * otherwise 
	 */
	public boolean tableHasAutoincrement()
	{
        for (ColumnDescriptor cd : columnDescriptorList)
		{
			if (cd.isAutoincrement())
				return true;
		}
		return false;
	}
	
	/**
	 * Gets an array of column names.
	 *
	 * @return An array, filled with the column names in the table.
	 *
	 */
	public String[] getColumnNamesArray()
	{
		int size = getNumberOfColumns();
		String[] s = new String[size];

		for (int i = 0; i < size; i++)
			s[i] = getColumnDescriptor(i+1).getColumnName();
		
		return s;
	}

	/**
	 * gets an array of increment values for autoincrement columns in the target
	 * table. If column is not an autoincrement column, then increment value is
	 * 0. If table has no autoincrement columns, returns NULL.
	 *
	 * @return		array containing the increment values of autoincrement
	 * columns.
	 *
	 */
	public long[]   getAutoincIncrementArray()
	{
		if (!tableHasAutoincrement())
			return null;

		int size = getNumberOfColumns();
		long[] inc = new long[size];

		for (int i = 0; i < size; i++)
		{
			ColumnDescriptor cd = getColumnDescriptor(i + 1);
			if (cd.isAutoincrement())
				inc[i] = cd.getAutoincInc();
		}

		return inc;
	}

	
	/** Returns a list of statistics for this table.
	 */
	public synchronized List<StatisticsDescriptor> getStatistics() throws StandardException
	{
		// if table already has the statistics descriptors initialized
		// no need to do anything
		if (statisticsDescriptorList != null)
			return statisticsDescriptorList;

		DataDictionary dd = getDataDictionary();
		return statisticsDescriptorList = dd.getStatisticsDescriptors(this);
	}

    /**
     * Marks the cardinality statistics for the indexes associated with this
     * table for update if they are considered stale, or for creation if they
     * don't exist, and if it is considered useful to update/create them.
     *
     * @param tableRowCountEstimate row count estimate for this table
     * @throws StandardException if obtaining index statistics fails
     */
    public void markForIndexStatsUpdate(long tableRowCountEstimate)
            throws StandardException {
        List<StatisticsDescriptor> sdl = getStatistics();
        if (sdl.isEmpty() && tableRowCountEstimate >= ISTATS_CREATE_THRESHOLD) {
            // No statistics exists, create them.
            indexStatsUpToDate = false;
            indexStatsUpdateReason = "no stats, row-estimate=" +
                    tableRowCountEstimate;
            return;
        }

        // Check the state of the existing indexes (if any).
        for (StatisticsDescriptor sd : sdl) {
            long indexRowCountEstimate = sd.getStatistic().getRowEstimate();
            long diff = Math.abs(tableRowCountEstimate - indexRowCountEstimate);
            // TODO: Set a proper limit here to avoid too frequent updates.
            if (diff >= ISTATS_ABSDIFF_THRESHOLD) {
                double cmp = Math.abs(
                        Math.log(indexRowCountEstimate) -
                        Math.log(tableRowCountEstimate));
                if (Double.compare(cmp, ISTATS_LNDIFF_THRESHOLD) == 1) {
                    indexStatsUpToDate= false;
                    indexStatsUpdateReason = "t-est=" + tableRowCountEstimate +
                            ", i-est=" + indexRowCountEstimate + " => cmp=" +
                            cmp;
                    break;
                }
            }
        }
    }

    /**
     * Tells if the index statistics for the indexes associated with this table
     * are consideres up-to-date, and clears the state.
     *
     * @return {@code true} if the statistics are considered up-to-date,
     *      {@code false} if not.
     */
    public boolean getAndClearIndexStatsIsUpToDate() {
        // TODO: Consider adding a flag telling if statistics update has been
        //       scheduled already.
        boolean tmp = indexStatsUpToDate;
        indexStatsUpToDate = true;
        return tmp;
    }

    /**
     * Returns the update criteria telling why the statistics are considered
     * stale.
     * <p>
     * This method is used for debugging.
     *
     * @return A string describing the update criteria that were met.
     */
    public String getIndexStatsUpdateReason() {
        return indexStatsUpdateReason;
    }

	/** 
	 * Are there statistics for this particular conglomerate.
	 *
	 * @param cd	Conglomerate/Index for which we want to check if statistics
	 * exist. cd can be null in which case user wants to know if there are any
	 * statistics at all on the table.
	 */
	public boolean statisticsExist(ConglomerateDescriptor cd)
		throws StandardException
	{
        List<StatisticsDescriptor> sdl = getStatistics();

		if (cd == null)
			return (sdl.size() > 0);

		UUID cdUUID = cd.getUUID();

        for (StatisticsDescriptor statDesc : sdl) {
            if (cdUUID.equals(statDesc.getReferenceID())) {
				return true;
            }
		}

		return false;
	}

	/**
	 * For this conglomerate (index), return the selectivity of the first
	 * numKeys. This basically returns the reciprocal of the number of unique
	 * values in the leading numKey columns of the index. It is assumed that
	 * statistics exist for the conglomerate if this function is called.
     * However, no locks are held to prevent the statistics from being dropped,
     * so the method also handles the case of missing statistics by using a
     * heuristic to estimate the selectivity.
	 *
	 * @param cd		ConglomerateDescriptor (Index) whose
	 * cardinality we are interested in.
	 * @param numKeys	Number of leading columns of the index for which
	 * cardinality is desired.

	 */
	public double selectivityForConglomerate(ConglomerateDescriptor cd,
											 int numKeys) 
		throws StandardException
	{
		UUID referenceUUID = cd.getUUID();

        for (StatisticsDescriptor statDesc : getStatistics())
		{
			if (!referenceUUID.equals(statDesc.getReferenceID()))
				continue;
			
			if (statDesc.getColumnCount() != numKeys)
				continue;
			
			return statDesc.getStatistic().selectivity((Object[])null);
		}

        // Didn't find statistics for these columns. Assume uniform 10%
        // selectivity for each column in the key.
        return Math.pow(0.1, numKeys);
	}

	/** @see TupleDescriptor#getDescriptorName */
    @Override
	public String getDescriptorName() { return tableName; }

	/** @see TupleDescriptor#getDescriptorType */
    @Override
	public String getDescriptorType() 
	{
		return (tableType == TableDescriptor.SYNONYM_TYPE) ? "Synonym" : "Table/View";
	}

	//////////////////////////////////////////////////////
	//
	// DEPENDENT INTERFACE
	//
	//////////////////////////////////////////////////////
	/**
	 * Check that all of the dependent's dependencies are valid.
	 *
	 * @return true if the dependent is currently valid
	 */
	public synchronized boolean isValid()
	{
		return true;
	}

	/**
	 * Prepare to mark the dependent as invalid (due to at least one of
	 * its dependencies being invalid).
	 *
	 * @param action	The action causing the invalidation
	 * @param p		the provider
	 *
	 * @exception StandardException thrown if unable to make it invalid
	 */
	public void prepareToInvalidate(Provider p, int action,
					LanguageConnectionContext lcc) 
		throws StandardException
	{
		DependencyManager dm = getDataDictionary().getDependencyManager();

		switch (action)
		{
			/*
			** Currently, the only thing we are dependent
			** on is an alias descriptor for an ANSI UDT.
			*/
		    default:

				throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_DEPENDENT_TABLE, 
									dm.getActionString(action), 
									p.getObjectName(),
									getQualifiedName());
		}
	}

	/**
	 * Mark the dependent as invalid (due to at least one of
	 * its dependencies being invalid).  Always an error
	 * for a table -- should never have gotten here.
	 *
	 * @param	action	The action causing the invalidation
	 *
	 * @exception StandardException thrown if called in sanity mode
	 */
	public void makeInvalid(int action, LanguageConnectionContext lcc) 
		throws StandardException
	{
		/* 
		** We should never get here, we should have barfed on 
		** prepareToInvalidate().
		*/
		if (SanityManager.DEBUG)
		{
			DependencyManager dm;
	
			dm = getDataDictionary().getDependencyManager();

			SanityManager.THROWASSERT("makeInvalid("+
				dm.getActionString(action)+
				") not expected to get called");
		}
	}
    
    /** Make the name of an identity sequence generator from a table ID */
    public  static  String  makeSequenceName( UUID tableID )
    {
        return tableID.toANSIidentifier();
    }

    
    /**
     * Privileged lookup of a Context. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Context    getContextOrNull( final String contextID )
    {
        if ( System.getSecurityManager() == null )
        {
            return ContextService.getContextOrNull( contextID );
        }
        else
        {
            return AccessController.doPrivileged
                (
                 new PrivilegedAction<Context>()
                 {
                     public Context run()
                     {
                         return ContextService.getContextOrNull( contextID );
                     }
                 }
                 );
        }
    }

}


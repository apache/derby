/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.TableDescriptor

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.GenericDescriptorList;

import org.apache.derby.iapi.sql.depend.Provider;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;

import org.apache.derby.iapi.reference.SQLState;
import	org.apache.derby.catalog.Dependable;
import org.apache.derby.iapi.services.sanity.SanityManager;

import java.util.Vector;
import java.util.Enumeration;
import java.util.List;
import java.util.Iterator;

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
	* @author Jeff Lichtman
	*/

public class TableDescriptor extends TupleDescriptor
	implements UniqueSQLObjectDescriptor, Provider
{
	public static final int BASE_TABLE_TYPE = 0;
	public static final int SYSTEM_TABLE_TYPE = 1;
	public static final int VIEW_TYPE = 2;
	public static final int GLOBAL_TEMPORARY_TABLE_TYPE = 3;

	public static final char	ROW_LOCK_GRANULARITY = 'R';
	public static final char	TABLE_LOCK_GRANULARITY = 'T';
	public static final char	DEFAULT_LOCK_GRANULARITY = ROW_LOCK_GRANULARITY;

	/**
	*/

	// implementation
	private char					lockGranularity;
	private boolean					onCommitDeleteRows; //true means on commit delete rows, false means on commit preserve rows of temporary table.
	private boolean					onRollbackDeleteRows; //true means on rollback delete rows. This is the only value supported.
	SchemaDescriptor				schema;
	String							tableName;
	UUID							oid;
	int								tableType;
	long							heapConglomNumber = -1;
	ColumnDescriptorList		columnDescriptorList;
	ConglomerateDescriptorList		conglomerateDescriptorList;
	ConstraintDescriptorList		constraintDescriptorList;
	private	GenericDescriptorList	triggerDescriptorList;
	ViewDescriptor					viewDescriptor;
	FormatableBitSet							referencedColumnMap;

	/** A list of statistics pertaining to this table-- 
	 */
	private List					statisticsDescriptorList;

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
		this.triggerDescriptorList = new GenericDescriptorList();
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
		//quoteStringIfNecessary is for bug 3476. If the schemaName and/or tableName has
		//double quotes in it, this method will put them in quotes and replace every
		//double quote with 2 double quotes.
		return quoteStringIfNecessary(getSchemaName()) + "." +
			quoteStringIfNecessary(getName());
	}

	/**
	 * If the name has double quotes in it, put two double quotes for every single
	 * double quote.
	 * For eg, if table name is m"n, return it as "m""n". For now, this is used
	 * by DMLModStatementNode.parseCheckConstraint().
	 *
	 * @param name	The String with or without double quotes
	 *
	 * @return	The quoted String
	 */

	private String quoteStringIfNecessary(String name)
	{
		String quotedString = name;
		int quotePos = name.indexOf("\"");

		if (quotePos == -1)
			return name;

		//string does have quotes in it.
		while(quotePos != -1) {
			quotedString = quotedString.substring(0,quotePos) + "\"" +
				quotedString.substring(quotePos);
			quotePos = quotedString.indexOf("\"",quotePos+2);
		}
		return "\"" + quotedString + "\"";

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
		DataDictionary dd = getDataDictionary();

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
		return referencedColumnMap;
	}

	/**
	 * Set the referenced column map of the table.
	 *
	 * @param	referencedColumnMap	FormatableBitSet of referenced columns.
	 *
	 * @return	void.
	 *
	 */
	public void setReferencedColumnMap(FormatableBitSet referencedColumnMap)
	{
		this.referencedColumnMap = referencedColumnMap;
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
		int cdlSize = getColumnDescriptorList().size();
		for (int index = 0; index < cdlSize; index++)
		{
			ColumnDescriptor cd = (ColumnDescriptor) columnDescriptorList.elementAt(index);
			maxColumnID = Math.max( maxColumnID, cd.getPosition() );
		}

		return maxColumnID;
	}

	/**
	 * Sets the UUID of the table
	 *
	 * @param oid	The UUID of the table to be set in the descriptor
	 *
	 * @return	Nothing
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
	 *	@param cm Current ContextManager
	 *
	 *	@return	the row.
	 *  @exception StandardException		Thrown on failure
	 */
	public ExecRow getEmptyExecRow( ContextManager cm)
		 throws StandardException
	{
		int							columnCount = getNumberOfColumns();
		ExecutionContext			ec = (ExecutionContext) cm.getContext(ExecutionContext.CONTEXT_ID);
		ExecRow result = ec.getExecutionFactory().getValueRow(columnCount);

		for (int index = 0; index < columnCount; index++)
		{
			ColumnDescriptor cd = (ColumnDescriptor) columnDescriptorList.elementAt(index);
			//String name = column.getColumnName();
			DataValueDescriptor dataValue = cd.getType().getNull();
			result.setColumn(index + 1, dataValue);
		}
		return result;
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
	 *
	 * @return Nothing.
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
	public boolean isPersistent()
	{
		if (tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
			return false;
		else
			return(super.isPersistent());
	}

	/**
	 * Gets the number of indexes on the table, including the backing indexes.
	 *
	 * @return the number of columns in the table.
	 *
	 */
	public int getTotalNumberOfIndexes()
		throws StandardException
	{
		int totalNumberOfIndexes = 0;
		ConglomerateDescriptor[]	cds = getConglomerateDescriptors();

		for (int index = 0; index < cds.length; index++)
		{
			if (cds[index].isIndex()) { totalNumberOfIndexes++; }
		}

		return totalNumberOfIndexes;
	}


    /**
	  *	Builds a list of all triggers which are relevant to a
	  *	given statement type, given a list of updated columns.
	  *
	  *	@param	statementType		defined in StatementType
	  *	@param	changedColumnIds	array of changed columns
	  *	@param	relevantTriggers	IN/OUT. Passed in as an empty list. Filled in as we go.
	  *
	  *	@return	list of relevant triggers
	  *
	  * @exception StandardException		Thrown on error
	  */
	public	void	getAllRelevantTriggers
	(
		int						statementType,
		int[]					changedColumnIds,
		GenericDescriptorList	relevantTriggers
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
		Enumeration descs = dd.getTriggerDescriptors(this).elements();

		while (descs.hasMoreElements())
		{
			TriggerDescriptor tgr = (TriggerDescriptor)descs.nextElement();

			if (tgr.needsToFire(statementType, changedColumnIds))
			{
				relevantTriggers.add(tgr);
			}
		}
	}

    /**
	  *	Gets all of the relevant constraints for a statement, given its
	  *	statement type and its list of updated columns.
	  *
	  *	@param	statementType			As defined in StatementType.
	  * @param	skipCheckConstraints	Skip check constraints
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
		boolean						skipCheckConstraints,
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

			if (skipCheckConstraints &&
					(cd.getConstraintType() == DataDictionary.CHECK_CONSTRAINT))
			{
				continue;
			}

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
		if (referencedColumnMap == null) 
			return	getDependableFinder(StoredFormatIds.TABLE_DESCRIPTOR_FINDER_V01_ID);
		else
			return getColumnDependableFinder(StoredFormatIds.COLUMN_DESCRIPTOR_FINDER_V01_ID,
											 referencedColumnMap.getByteArray());
	}

	/**
	 * Return the name of this Provider.  (Useful for errors.)
	 *
	 * @return String	The name of this provider.
	 */
	public String getObjectName()
	{
		if (referencedColumnMap == null)
			return tableName;
		else
		{
			String name = new String(tableName);
			boolean first = true;
			for (int i = 0; i < columnDescriptorList.size(); i++)
			{
				ColumnDescriptor cd = (ColumnDescriptor) columnDescriptorList.elementAt(i);
				if (referencedColumnMap.isSet(cd.getPosition()))
				{
					if (first)
					{
						name += "(" + cd.getColumnName();
						first = false;
					}
					else
						name += ", " + cd.getColumnName();
				}
			}
			if (! first)
				name += ")";
			return name;
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
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			String tempString = "SCHEMA:\n" + schema + "\ntableName: " + tableName + "\n" +
				"oid: " + oid + " tableType: " + tableType + "\n" +
				"conglomerateDescriptorList: " + conglomerateDescriptorList + "\n" +
				"columnDescriptorList: " + columnDescriptorList + "\n" +
				"constraintDescriptorList: " + constraintDescriptorList + "\n" +
				"heapConglomNumber: " + heapConglomNumber + "\n";
			if (tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
			{
				tempString = tempString + "onCommitDeleteRows: " + "\n" + onCommitDeleteRows + "\n";
				tempString = tempString + "onRollbackDeleteRows: " + "\n" + onRollbackDeleteRows + "\n";
			} else
				tempString = tempString + "lockGranularity: " + lockGranularity + "\n";
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
	 * @return Nothing.
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
	public GenericDescriptorList getTriggerDescriptorList()
		throws StandardException
	{
		return triggerDescriptorList;
	}

	/**
	 * Sets the trigger descriptor list
	 *
	 * @param newCDL	The new trigger descriptor list for this table descriptor
	 */
	public void setTriggerDescriptorList(GenericDescriptorList newCDL)
	{
		triggerDescriptorList = newCDL;
	}

	/**
	 * Empty the trigger descriptor list
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void emptyTriggerDescriptorList()
		throws StandardException
	{
		// Easier just to get a new CDL then to clean out the current one
		this.triggerDescriptorList = new GenericDescriptorList();
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
	 * @param	The conglomerate descriptor 
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
	 * @param	The constraint descriptor 
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
	 * @param columnNumber	The ordinal position of the column in the table
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
	 *
	 * @exception StandardException		Thrown on failure
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
	 * Does the table have an autoincrement column or not?
	 * 
	 * @return TRUE if the table has atleast one autoincrement column, false
	 * otherwise 
	 */
	public boolean tableHasAutoincrement()
	{
		int cdlSize = getColumnDescriptorList().size();
		for (int index = 0; index < cdlSize; index++)
		{
			ColumnDescriptor cd = 
				(ColumnDescriptor) columnDescriptorList.elementAt(index);
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
	private synchronized List getStatistics() throws StandardException
	{
		// if table already has the statistics descriptors initialized
		// no need to do anything
		if (statisticsDescriptorList != null)
			return statisticsDescriptorList;

		DataDictionary dd = getDataDictionary();
		return statisticsDescriptorList = dd.getStatisticsDescriptors(this);
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
		List sdl = getStatistics();

		if (cd == null)
			return (sdl.size() > 0);

		UUID cdUUID = cd.getUUID();

		for (Iterator li = sdl.iterator(); li.hasNext(); )
		{
			StatisticsDescriptor statDesc = (StatisticsDescriptor) li.next();
			if (cdUUID.equals(statDesc.getReferenceID()))
				return true;

		}

		return false;
	}

	/**
	 * For this conglomerate (index), return the selectivity of the first
	 * numKeys. This basically returns the reciprocal of the number of unique
	 * values in the leading numKey columns of the index. It is assumed that
	 * statistics exist for the conglomerate if this function is called.
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
		if (!statisticsExist(cd))
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT("no statistics exist for conglomerate"
										  + cd);
			}
			else 
			{
				double selectivity = 0.1;
				for (int i = 0; i < numKeys; i++)
					selectivity *= 0.1;
				return selectivity;
			}
		}
		
		UUID referenceUUID = cd.getUUID();

		List sdl = getStatistics();
		for (Iterator li = sdl.iterator(); li.hasNext(); )
		{
			StatisticsDescriptor statDesc = (StatisticsDescriptor) li.next();

			if (!referenceUUID.equals(statDesc.getReferenceID()))
				continue;
			
			if (statDesc.getColumnCount() != numKeys)
				continue;
			
			return statDesc.getStatistic().selectivity((Object[])null);
		}
		
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("Internal Error-- statistics not found in selectivityForConglomerate.\n cd = " + cd + "\nnumKeys = " + numKeys);
		return 0.1;				// shouldn't come here.
	}

	/** @see TupleDescriptor#getDescriptorName */
	public String getDescriptorName() { return tableName; }

	/** @see TupleDescriptor#getDescriptorType */
	public String getDescriptorType() { return "Table/View"; }
}

/*

   Derby - Class org.apache.derby.impl.store.access.btree.BTree

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

package org.apache.derby.impl.store.access.btree;


import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.services.stream.InfoStreams;


import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.ScanManager;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowLocationRetRowSource;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.ContainerKey;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.impl.store.access.conglomerate.ConglomerateUtil;
import org.apache.derby.impl.store.access.conglomerate.GenericConglomerate;
import org.apache.derby.impl.store.access.conglomerate.OpenConglomerateScratchSpace;
import org.apache.derby.impl.store.access.conglomerate.TemplateRow;


import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectInput;

import java.util.Properties;


/**

  A b-tree object corresponds to an instance of a b-tree conglomerate.  It 
  contains the static information about a conglomerate which is built at 
  create conglomerate time.
  <p>
  This generic implementation is expected to be extended by the concreate
  implementations.
  <P>
  The fields are set when the conglomerate is created and never changed 
  thereafter.  When alter table is supported then it will change under the
  control of a table level lock.
  <p>
  They have package scope because they're read by the scans and controllers.
  <p>
  A table of all conglomerates in the system is maintained by the accessmanager.
  A cache of conglomerates is maintained in the accessmanager, and references
  to the read only objects are handed out.  A copy of the Conglomerate
  object is kept in the control row of the root page, so that during logical
  undo this information can be read without needing to access the possibly
  corrupt table maintained by the access manager.
**/

public abstract class BTree extends GenericConglomerate
{
    /**************************************************************************
     * Public Constants of BTree class:
     **************************************************************************
     */

    /**
     * The page number of the root page is always at the fixed page number:
     * ROOTPAGEID.  This means that given an open container, during logical
     * undo one can always find the root page and look up the conglomerate
     * information.
     **/
    public static final long ROOTPAGEID = ContainerHandle.FIRST_PAGE_NUMBER;

	/** 
    Property name for the maximum number of rows to place in a btree page (leaf
    or branch).  Equal to 'derby.access.btreeMaxRowPerPage'.  Used by tests
    and debugging to exactly control split points, and to make it easier to test
    tall trees without needing lots of data.
	*/
	public static final String PROPERTY_MAX_ROWS_PER_PAGE_PARAMETER = 
        (SanityManager.DEBUG ?  "derby.access.btreeMaxRowPerPage" : null);

    /* properties of a btree see create(). */
    public static final String PROPERTY_ALLOWDUPLICATES = "allowDuplicates";
    public static final String PROPERTY_NKEYFIELDS      = "nKeyFields";
    public static final String PROPERTY_NUNIQUECOLUMNS  = "nUniqueColumns";
    public static final String PROPERTY_PARENTLINKS     = "maintainParentLinks";



    /**************************************************************************
     * Protected Fields of BTree class:
     **************************************************************************
     */

	/**
	The id of the container in which this b-tree is stored. 
	**/
	protected ContainerKey id;

	/**
	The number of key fields.
	**/
	protected int nKeyFields;

	/**
	The number of uniqueness columns.  These are the columns that
	are considered for the purpose of detecting duplicate keys and rows.
	**/
	int nUniqueColumns;

	/**
	Whether the index allows duplicates or not.
	**/
	boolean allowDuplicates;

	/**
	Whether the parent should maintain links from child pages to their parent.
	These links are only used for consistency checking purposes.  They improve
	consistency checking at the cost of run-time efficiency.
	**/
	boolean maintainParentLinks;

    /**
    Maximum rows per page to place on a btree leaf or nonleaf page.  Used
    by testing to finely control split points.  Only changed for debugging
    purposes.

    RESOLVE (mikem) - this should not be static.  Need to design a way in
    debugging mode to get btree created with a persistent "maxRowsPerPage".
    This hack makes all btrees get created with the "last" maxRowsPerPage 
    value set.
    **/
    static int maxRowsPerPage = Integer.MAX_VALUE;

	/**
    Format id of the conglomerate.
	**/
	protected int conglom_format_id;

    /**
    The array of format id's, one for each column in the template.
    **/
    int[]    format_ids;

	//columns sorting order information
	// true - Ascending Order ; false -Descending Order
	protected boolean[]	ascDescInfo;

	/*
	** Private Methods of BTree.
	*/

	/*
	** Public Methods of BTree.
	*/


    /**************************************************************************
     * Abstract Protected locking methods of BTree:
     *     getBtreeLockingPolicy
     *     lockScan
     *     unlockScan
     *     lockPreviousRow
     *     lockRowOnPage
     *     lockRow
     *     lockTable
     **************************************************************************
     */

    /**
     * Create a new btree locking policy from scratch.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    abstract protected BTreeLockingPolicy getBtreeLockingPolicy(
    Transaction             rawtran,
    int                     lock_level,
    int                     mode,
    int                     isolation_level,
    ConglomerateController  base_cc,
    OpenBTree               open_btree)
		throws StandardException;

    /**
     * Lock the base table.
     * <p>
     * Assumes that segment of the base container is the same as the segment
     * of the btree segment.
     * <p>
     * RESOLVE - we really want to get the lock without opening the container.
     * raw store will be providing this.
     *
     * @param xact_manager Transaction to associate the lock with.
     * @param forUpdate    Whether to lock exclusive or share.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    abstract public ConglomerateController lockTable(
    TransactionManager  xact_manager,
    int                 open_mode,
    int                 lock_level,
    int                 isolation_level)
		throws StandardException;


    /**************************************************************************
     * Private/Protected methods of BTree:
     **************************************************************************
     */


    /**
     * Create a branch row template for this conglomerate.
     * <p>
     * Reads the format id's of each of the columns and manufactures object of
     * the given type for each.  It then uses these "empty" objects to create
     * a template row.  The object passed in is then added to the last column
     * of the row.
     *
	 * @return The new template.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    final DataValueDescriptor[] createBranchTemplate(
    DataValueDescriptor page_ptr)
        throws StandardException
    {
        return(TemplateRow.newBranchRow(format_ids, page_ptr));
    }


    /**************************************************************************
     * Public methods of BTree:
     **************************************************************************
     */

    /**
     * Create a template for this conglomerate.
     * <p>
     * Reads the format id's of each of the columns and manufactures object of
     * the given type for each.  It then uses these "empty" objects to create
     * a template row.
     * <p>
     * This method is public so that B2IUndo() can call it.
     *
	 * @return The new template.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    final public DataValueDescriptor[] createTemplate()
        throws StandardException
    {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(format_ids != null);

        return(TemplateRow.newRow((FormatableBitSet) null, format_ids));
    }

    /**
     * Is this a "unique" index?
     **/
    final public boolean isUnique()
    {
        return(nKeyFields != nUniqueColumns);
    }

    /**************************************************************************
     * Public Methods of Conglomerate Interface:
     **************************************************************************
     */

    /**
     * Add a column to the conglomerate.
     * <p>
     * Currently B2I does not support this operation.
     * input template column.  
     * 
     * @param xact_manager      Transaction to associate the lock with.
     * @param column_id        The column number to add this column at.
     * @param template_column  An instance of the column to be added to table.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public void addColumn(
    TransactionManager  xact_manager,
    int                 column_id,
    Storable            template_column)
        throws StandardException
    {
        throw StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE);
    }

    /**
     * Get the id of the container of the conglomerate.
     * <p>
     * Will have to change when a conglomerate could have more than one 
     * container.  The ContainerKey is a combination of the container id
     * and segment id.
     *
	 * @return The ContainerKey.
     **/
    public final ContainerKey getId()
    {
        return(id);
    }


	/**
	Do the generic part of creating a b-tree conglomerate.  This method 
    is called from the concrete subclass (which may also read some properties).
    <p>
    This method processes all properties which are generic to all BTree's.  It
    creates the container for the btree.
    <p>

    The following properties are generic to a b-tree conglomerate.  :

    <UL>
    <LI>"allowDuplicates" (boolean).  If set to true the table will allow 
    rows which are duplicate in key column's 0 through (nUniqueColumns - 1).
    Currently only supports "false".
    This property is optional, defaults to false.
    <LI>"nKeyFields"  (integer) Columns 0 through (nKeyFields - 1) will be 
    included in key of the conglomerate.
    This implementation requires that "nKeyFields" must be the same as the
    number of fields in the conglomerate, including the rowLocationColumn.
    Other implementations may relax this restriction to allow non-key fields
    in the index.
    This property is required.
    <LI>"nUniqueColumns" (integer) Columns 0 through "nUniqueColumns" will be 
    used to check for uniqueness.  So for a standard SQL non-unique index 
    implementation set "nUniqueColumns" to the same value as "nKeyFields"; and
    for a unique index set "nUniqueColumns" to "nKeyFields" - 1 (ie. don't 
    include the rowLocationColumn in the uniqueness check).
    This property is required.
    <LI>"maintainParentLinks" (boolean)
    Whether the b-tree pages maintain the page number of their parent.  Only
    used for consistency checking.  It takes a certain amount more effort to
    maintain these links, but they're really handy for ensuring that the index
    is consistent.
    This property is optional, defaults to true.
    </UL>

    @exception StandardException Thrown by underlying raw store, or thrown by
    this routine on an invalid containerid.
    
	**/

	public void create(
    Transaction             rawtran,
    int                     segmentId,
    long                    input_containerid,
    DataValueDescriptor[]   template,
    Properties              properties,
    int                     conglom_format_id,
	int                     tmpFlag
    )
        throws StandardException
	{
        String result_string;

        if (properties == null)
        {
            throw(
                StandardException.newException(
                    SQLState.BTREE_PROPERTY_NOT_FOUND, PROPERTY_NKEYFIELDS));
        }

        // Check input arguments
        allowDuplicates = (Boolean.valueOf(
            properties.getProperty(PROPERTY_ALLOWDUPLICATES, "false"))).booleanValue();

        result_string = properties.getProperty(PROPERTY_NKEYFIELDS);
        if (result_string == null)
        {
            throw(
                StandardException.newException(
                    SQLState.BTREE_PROPERTY_NOT_FOUND, PROPERTY_NKEYFIELDS));
        }
        else
        {
            nKeyFields = Integer.parseInt(result_string);
        }

        result_string = properties.getProperty(PROPERTY_NUNIQUECOLUMNS);
        if (result_string == null)
        {
            throw(StandardException.newException(
                SQLState.BTREE_PROPERTY_NOT_FOUND, PROPERTY_NUNIQUECOLUMNS));
        }
        else
        {
            nUniqueColumns = Integer.parseInt(result_string);
        }


        if (SanityManager.DEBUG)
        {
            result_string = 
                properties.getProperty(PROPERTY_MAX_ROWS_PER_PAGE_PARAMETER);

            if (result_string != null)
            {
                maxRowsPerPage = Integer.parseInt(result_string);
            }
        }

        maintainParentLinks = (Boolean.valueOf(
            properties.getProperty(PROPERTY_PARENTLINKS, "true"))).booleanValue();

        // RESOLVE (mikem) - true for now, if we want to support non-key 
        // fields eventually this assert may be wrong.
        if (SanityManager.DEBUG)
        {
			if (template.length != nKeyFields)
			{
				SanityManager.THROWASSERT(
					"template.length (" + template.length +
					") expected to equal nKeyFields (" + 
					nKeyFields + ")");
			}
            SanityManager.ASSERT((nUniqueColumns == nKeyFields) || 
                                 (nUniqueColumns == (nKeyFields - 1)));
        }

        // get format id's from each column in template and store it in the
        // conglomerate state.
        format_ids = ConglomerateUtil.createFormatIds(template);

        // copy the format id of the conglomerate.
        this.conglom_format_id = conglom_format_id;

		// Create a container for the b-tree with default page size and 
        // fill up pages.
		properties.put(RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER, "0");
		properties.put(RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER, "1");
		properties.put(RawStoreFactory.PAGE_REUSABLE_RECORD_ID, "true");

		long containerid = 
            rawtran.addContainer(
                segmentId, input_containerid, 
                ContainerHandle.MODE_DEFAULT, properties, tmpFlag);

		// Make sure the container was actually created.
		// Open segment will get cleaned up when transaction is.
		if (containerid <= 0)
        {
            throw(StandardException.newException(
                    SQLState.BTREE_CANT_CREATE_CONTAINER)); 
        }

        if (SanityManager.DEBUG)
        {
            if (input_containerid != ContainerHandle.DEFAULT_ASSIGN_ID)
                SanityManager.ASSERT(containerid == input_containerid);
        }

		id = new ContainerKey(segmentId, containerid);
	}

	/**
	Drop this btree.
	This must be done by a concrete implementation.
	@see Conglomerate#drop

    @exception StandardException Standard exception policy.
	**/
	public abstract void drop(TransactionManager xact_manager)
		throws StandardException;

	/**
	Load a b-tree.  This must be done by a concrete implementation.
	@see Conglomerate#load

    @exception StandardException Standard exception policy.
	**/
	public abstract long load(
	TransactionManager      xact_manager,
	boolean                 createConglom,
	RowLocationRetRowSource rowSource)
		throws StandardException;

    public long getContainerid()
    {
        return(this.id.getContainerId());
    }

    /**
     * Return dynamic information about the conglomerate to be dynamically 
     * reused in repeated execution of a statement.
     * <p>
     * The dynamic info is a set of variables to be used in a given 
     * ScanController or ConglomerateController.  It can only be used in one 
     * controller at a time.  It is up to the caller to insure the correct 
     * thread access to this info.  The type of info in this is a scratch 
     * template for btree traversal, other scratch variables for qualifier 
     * evaluation, ...
     * <p>
     *
	 * @return The dynamic information.
     *
     * @param conglomId The identifier of the conglomerate to open.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public DynamicCompiledOpenConglomInfo getDynamicCompiledConglomInfo(
    long        conglomId)
		throws StandardException
    {
        return(new OpenConglomerateScratchSpace(format_ids));
    }


    /**
     * Is this conglomerate temporary?
     * <p>
     *
	 * @return whether conglomerate is temporary or not.
     **/
    public boolean isTemporary()
    {
        return (id.getSegmentId() == ContainerHandle.TEMPORARY_SEGMENT);
    }

	/**
	Open a b-tree controller.
	This must be done by a concrete implementation.
	@see Conglomerate#open

    @exception StandardException Standard exception policy.
	**/
	public abstract ConglomerateController open(
    TransactionManager              xact_manager,
    Transaction                     rawtran,
    boolean                         hold,
    int                             open_mode,
    int                             lock_level,
    LockingPolicy                   locking_policy,
    StaticCompiledOpenConglomInfo   static_info,
    DynamicCompiledOpenConglomInfo  dynamic_info)
		throws StandardException;



    /**************************************************************************
     * Public Methods of Storable Interface (via Conglomerate):
     *     This class is responsible for re/storing its own state.
     **************************************************************************
     */


	/**
	Return whether the value is null or not.
	The containerid being zero is what determines nullness;  subclasses
	are not expected to override this method.
	@see org.apache.derby.iapi.services.io.Storable#isNull
	**/
	public boolean isNull()
	{
		return id == null;
	}

	/**
	Restore the in-memory representation to the null value.
	The containerid being zero is what determines nullness;  subclasses
	are not expected to override this method.

	@see org.apache.derby.iapi.services.io.Storable#restoreToNull
	**/
	public void restoreToNull()
	{
		id = null;
	}

	/**
	Restore the in-memory representation from the stream.

	@exception ClassNotFoundException Thrown if the stored representation is
	serialized and a class named in the stream could not be found.

    @exception IOException thrown by readObject()

	
	@see java.io.Externalizable#readExternal
	*/
	public void readExternal(ObjectInput in) 
        throws IOException, ClassNotFoundException
	{
        // read in the conglomerate format id.
        conglom_format_id = FormatIdUtil.readFormatIdInteger(in);

		// XXX (nat) need to improve error handling
		long containerid         = in.readLong();
		int segmentid			= in.readInt();
		nKeyFields          = in.readInt();
		nUniqueColumns      = in.readInt();
		allowDuplicates     = in.readBoolean();
		maintainParentLinks = in.readBoolean();

        // read in the array of format id's
        format_ids = ConglomerateUtil.readFormatIdArray(this.nKeyFields, in);

		id = new ContainerKey(segmentid, containerid);
	}

	public void readExternalFromArray(ArrayInputStream in) 
        throws IOException, ClassNotFoundException
	{
        // read in the conglomerate format id.
        conglom_format_id = FormatIdUtil.readFormatIdInteger(in);

		// XXX (nat) need to improve error handling
		long containerid         = in.readLong();
		int segmentid			= in.readInt();
		nKeyFields          = in.readInt();
		nUniqueColumns      = in.readInt();
		allowDuplicates     = in.readBoolean();
		maintainParentLinks = in.readBoolean();

        // read in the array of format id's
        format_ids = ConglomerateUtil.readFormatIdArray(this.nKeyFields, in);

		id = new ContainerKey(segmentid, containerid);
	}

	
	/**
	Store the stored representation of the column value in the stream.
	It might be easier to simply store the properties - which would certainly
	make upgrading easier.

    @exception IOException thrown by writeObject()

	*/
	public void writeExternal(ObjectOutput out) 
        throws IOException
    {
        FormatIdUtil.writeFormatIdInteger(out, conglom_format_id);

		out.writeLong(id.getContainerId());
		out.writeInt((int) id.getSegmentId());
		out.writeInt((nKeyFields));
		out.writeInt((nUniqueColumns));
		out.writeBoolean((allowDuplicates));
		out.writeBoolean((maintainParentLinks));

        ConglomerateUtil.writeFormatIdArray(format_ids, out);
	}

    /**************************************************************************
     * Public toString() Method:
     **************************************************************************
     */

    public String toString()
    {
        if (SanityManager.DEBUG)
        {
            return  ("BTREE: containerid = " + 
                     (this.id == null ? "null" : this.id.toString()) +
                     ";nKeyFields = " + nKeyFields +
                     ";nUniqueColumns = " + nUniqueColumns +
                     ";allowDuplicates = " + allowDuplicates);
        }
        else
        {
            return(super.toString());
        }
    }
}

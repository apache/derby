/* IBM Confidential
 *
 * Product ID: 5697-F53
 *

 * Copyright 1997, 2004.WESTHAM

 *
 * The source code for this program is not published or otherwise divested
 * of its trade secrets, irrespective of what has been deposited with the
 * U.S. Copyright Office.
 */

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.impl.store.raw.data.BasePage;

import org.apache.derby.impl.store.raw.data.LongColumnException;
import org.apache.derby.impl.store.raw.data.OverflowInputStream;
import org.apache.derby.impl.store.raw.data.PageVersion;
import org.apache.derby.impl.store.raw.data.RecordId;
import org.apache.derby.impl.store.raw.data.RawField;
import org.apache.derby.impl.store.raw.data.ReclaimSpace;
import org.apache.derby.impl.store.raw.data.StoredFieldHeader;
import org.apache.derby.impl.store.raw.data.StoredRecordHeader;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.FormatIdInputStream;
import org.apache.derby.iapi.services.io.FormatIdOutputStream;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.StreamStorable;
import org.apache.derby.iapi.services.io.TypedFormat;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowUtil;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.PageKey;
import org.apache.derby.iapi.store.raw.PageTimeStamp;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.Orderable;

import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.services.io.ArrayOutputStream;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.iapi.services.io.CompressedNumber;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;
import org.apache.derby.iapi.services.io.LimitObjectInput;
import org.apache.derby.iapi.services.io.ErrorObjectInput;


import java.util.zip.CRC32;

import java.io.IOException;
import java.io.EOFException;
import java.io.Externalizable;
import java.io.InvalidClassException;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;


/**
	StoredPage is a sub class of CachedPage that stores page data in a
	fixed size byte array and is designed to be written out to a file
	through a DataInput/DataOutput interface. A StoredPage can exist
	in its clean or dirty state without the FileContainer it was created
	from being in memory.

  <P><B>Page Format</B><BR>
  The page is broken into five sections
  <PRE>
  +----------+-------------+-------------------+-------------------+----------+
  | formatId | page header | records           | slot offset table | checksum |
  +----------+-------------+-------------------+-------------------+----------+
  </PRE>
  <BR><B>FormatId</B><BR>
  The formatId is a 4 bytes array, it contains the format Id of this page.
  <BR><B>Page Header</B><BR>
  The page header is a fixed size, 56 bytes 
  <PRE>
  1 byte  boolean			is page an overflow page
  1 byte  byte				page status (a field maintained in base page)
  8 bytes long				pageVersion (a field maintained in base page)
  2 bytes unsigned short	number of slots in slot offset table
  4 bytes integer			next record identifier
  4 bytes integer			generation number of this page (Future Use)
  4 bytes integer			previous generation of this page (Future Use)
  8 bytes bipLocation		the location of the beforeimage page (Future Use)
  2 bytes unsigned short	number of deleted rows on page. (new release 2.0)
  2 bytes unsigned short    % of the page to keep free for updates
  2 bytes short				spare for future use
  4 bytes long				spare for future use (encryption uses to write 
                                                  random bytes here).
  8 bytes long				spare for future use
  8 bytes long				spare for future use

  </PRE>

  Note that spare space has been guaranteed to be writen with "0", so
  that future use of field should not either not use "0" as a valid data 
  item or pick 0 as a valid default value so that on the fly upgrade can 
  assume that 0 means field was never assigned.

  <BR><B>Records</B>
  The records section contains zero or more records, the format of each record
  follows.
  minimumRecordSize is the minimum user record size, excluding the space we
  use for the record header and field headers.  When a record is inserted, it
  is stored in a space at least as large as the sum of the minimumRecordSize
  and total header size.
		For example,
			If minimumRecordSize is 10 bytes,
			the user record is 7 bytes,
			we used 5 bytes for record and field headers,
			this record will take (10 + 5) bytes of space, extra 3 bytes is 
            put into reserve.

			If minimumRecordSize is 10 bytes,
			user record is 17 bytes,
			we used 5 bytes for record and field headers,
			this record will take (17 + 5) bytes of space, no reserve space 
            here.

  minimumRecordSize is defined by user on per container basis.
  The default for minimumRecordSize is set to 1.

  This implementation always keeps occupied bytes at the low end of the record 
  section.  Thus removing (purging) a record moves all other records down, and
  their slots are also moved down.
  A page has no empty slot (an empty page has no slot)

   <BR><B>Record & Field Format</B>

  Record Header format is defined in the StoredRecordHeader class.
  
<PRE>	
  <BR><B>Fields</B>

  1 byte	Boolean	- is null, if true no more data follows.
  4 bytes   Integer - length of field that follows (excludes these four bytes).

  StoredPage will use the static method provided by StoredFieldHeader
  to read/write field status and field data length.

  Field Header format is defined in the StoredFieldHeader class.
  <data>

  </PRE>
	<BR><B>Slot Offset Table</B><BR>
	The slot offset table is a table of 6 or 12 bytes per record, depending on
    the pageSize being less or greater than 64K:
	2 bytes (unsigned short) or 4 bytes (int) page offset for the record that
    is assigned to the slot, and 2 bytes (unsigned short) or 4 bytes (int) 
    for the length of the record on this page.
	2 bytes (unsigned short) or 4 bytes (int) for the length of the reserved 
    number of bytes for this record on this page.
	First slot is slot 0.  The slot table grows backwards. Slots are never
    left empty.
	<BR><B>Checksum</B><BR>
	8 bytes of a java.util.zip.CRC32 checksum of the entire's page contents 
    without the 8 bytes representing the checksum.

	<P><B>Page Access</B>
	The page data is accessed in this class by one of three methods.
	<OL>
	<LI>As a byte array using pageData (field in cachedPage). This is the 
    fastest.
	<LI>As an ArrayInputStream (rawDataIn) and ArrayOutputStream (rawDataOut),
	this is used to set limits on any one reading the page logically.
	<LI>Logically through rawDataIn (ArrayInputStream) and 
    logicalDataOut (FormatIdOutputStream), this provides the methods to write
    logical data (e.g. booleans and integers etc.) and the ObjectInput
	and ObjectOutput interfaces for DataValueDescriptor's. These logical
    streams are constructed using the array streams.
	</OL>

	@see java.util.zip.CRC32
	@see ArrayInputStream
	@see ArrayOutputStream
 **/



    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of XXXX class:
     **************************************************************************
     */

public class StoredPage extends CachedPage
{ 
    /**************************************************************************
     * static final Fields of the class
     **************************************************************************
     */

	/*
	 * typed format
	 */

	public static final int FORMAT_NUMBER = 
        StoredFormatIds.RAW_STORE_STORED_PAGE;

    /**
     * Return my format identifier.
     **/
	public int getTypeFormatId()
	{
		return StoredFormatIds.RAW_STORE_STORED_PAGE;
	}


    /**
     * Constants used to find different portions of data on the page.  
     * <p>
     * The page is laid out as follows:
     * The page is broken into five sections
     * +----------+-------------+---------+-------------------+----------+
     * | formatId | page header | records | slot offset table | checksum |
     * +----------+-------------+---------+-------------------+----------+
     *
     * offset               size                    section
     * ------               -------------------     --------------------------
     * 0                    PAGE_FORMAT_ID_SIZE     formatId
     * PAGE_FORMAT_ID_SIZE: PAGE_HEADER_SIZE (56)   page header
     * RECORD_SPACE_OFFSET: variable                records
     **/


    /**
     * Start of page, formatId must fit in 4 bytes.
     * <p>
	 * where the page header starts - page format is mandated by cached page
     **/
	protected static final int PAGE_HEADER_OFFSET   = PAGE_FORMAT_ID_SIZE;


    /**
     * Fixed size of the page header
     **/
	protected static final int PAGE_HEADER_SIZE     = 56;


	/** 
		Start of the record storage area
	*/
    /**
     * Start of the record storage area.
     * <p>
     * Note: a subclass may change the start of the record storage area.  
     * Don't always count on this number.
     **/
	protected static final int RECORD_SPACE_OFFSET = 
        PAGE_HEADER_OFFSET + PAGE_HEADER_SIZE;

    /**
     * offset of the page version number
     **/
	protected static final int PAGE_VERSION_OFFSET = PAGE_HEADER_OFFSET + 2;

    /**
     * SMALL_SLOT_SIZE are for pages smaller than 64K,
     * LARGE_SLOT_SIZE is for pages bigger than 64K.
     **/
	protected static final int SMALL_SLOT_SIZE  = 2;
	protected static final int LARGE_SLOT_SIZE  = 4;

    /**
     * Size of the checksum stored on the page.
     *
     * The checksum is stored in the last 8 bytes of the page, the slot table
     * grows backward up the page starting at the end of the page just before
     * the checksum.
     **/
	protected static final int CHECKSUM_SIZE    = 8;

    /**
     * OVERFLOW_POINTER_SIZE - Number of bytes to reserve for overflow pointer
     * 
     * The overflow pointer is the pointer that the takes the place of the 
     * last column of a row if the row can't fit on the page.  The pointer
     * then points to another page where the next column of the row can be
     * found.  The overflow pointer can be bigger than a row, so when 
     * overflowing a row the code must overflow enough columns so that there
     * is enough free space to write the row.  Note this means that the
     * minimum space a row can take on a page must allow for at least the
     * size of the overflow pointers so that if the row is updated it can 
     * write the over flow pointer.
     *
     **/
	protected static final int OVERFLOW_POINTER_SIZE = 12;

    /**
     * OVERFLOW_PTR_FIELD_SIZE - Number of bytes of an overflow field
     * 
     * This is the length to reserve for either an column or row overflow
     * pointer field.  It includes the size of the field header plus the 
     * maxium length of the overflow pointer (it could be shorter due to
     * compressed storage).
     *
     * The calcualtion is:
     *
	 * OVERFLOW_PTR_FIELD_SIZE = 
     *     OVERFLOW_POINTER_SIZE + 
     *     sizeof(status byte) + 
     *     sizeof(field length field for a field which is just an overflow ptr)
     *     
     *
     **/
	protected static final int OVERFLOW_PTR_FIELD_SIZE = 
        OVERFLOW_POINTER_SIZE + 1 + 1;

    /**
     * In memory buffer used as scratch space for streaming columns.
     **/
	ByteHolder bh = null;

    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */


    /**
     * Constants used in call to logColumn
     * <p>
     * Action taken in this routine is determined by the kind of column as
     * specified in the columnFlag:
     *     COLUMN_NONE	 - the column is insignificant
     *     COLUMN_FIRST  - this is the first column in a logRow() call
     *     COLUMN_LONG   - this is a known long column, therefore we will 
     *                     store part of the column on the current page and 
     *                     overflow the rest if necessary.
     **/
	protected static final int COLUMN_NONE  = 0;
	protected static final int COLUMN_FIRST = 1;
	protected static final int COLUMN_LONG  = 2;


    /**
     * maxFieldSize is a worst case calculation for the size of a record
     * on an empty page, with a single field, but still allow room for 
     * an overflow pointer if another field is to be added.  See initSpace().
     * maxFieldSize is a worst case calculation for the size of a record
     * 
     * This is used as the threshold for a long column.
     * 
     * maxFieldSize = 
     *      totalSpace * (1 - spareSpace/100) - 
     *      slotEntrySize * - 16 - OVERFLOW_POINTER_SIZE;
     **/
	protected int maxFieldSize;


    /**
     * The page header is a fixed size, 56 bytes, following are variables used
     * to access the fields in the header:
     * <p>
     *  1 byte  boolean isOverflowPage  is page an overflow page
     *  1 byte  byte	pageStatus      page status (field in base page)
     *  8 bytes long	pageVersion     page version (field in base page)
     *  2 bytes ushort	slotsInUse      number of slots in slot offset table
     *  4 bytes integer	nextId          next record identifier
     *  4 bytes integer	generation      generation number of this page(FUTURE USE)
     *  4 bytes integer	prevGeneration  previous generation of page (FUTURE USE)
     *  8 bytes long    bipLocation     the location of the BI page (FUTURE USE)
     *  2 bytes ushort  deletedRowCount number of deleted rows on page.(rel 2.0)
     *  2 bytes long		            spare for future use
     *  4 bytes long		            spare (encryption writes random bytes)
     *  8 bytes long		            spare for future use
     *  8 bytes long		            spare for future use
     *
     *  Note that spare space has been guaranteed to be writen with "0", so
     *  that future use of field should not either not use "0" as a valid data 
     *  item or pick 0 as a valid default value so that on the fly upgrade can 
     *  assume that 0 means field was never assigned.
     *
     **/
	private boolean	isOverflowPage;     // is page an overflow page?
	private int		slotsInUse;         // number of slots in slot offset table.
	private int		nextId;             // next record identifier
	private int		generation;         // (Future Use) generation number of this page
	private int		prevGeneration;     // (Future Use) previous generation of page
	private long	bipLocation;        // (Future Use) the location of the BI page
	private int		deletedRowCount;    // number of deleted rows on page.

    /**
     * Is the header in the byte array out of date wrt the fields.
     * <p>
     * this field must be set to true whenever one of the above header fields 
     * is modified.  Ie any of (isOverflowPage, slotsInUse, nextId, generation,
     * prevGeneration, bipLocation, deletedRowCount)
     **/
	private boolean headerOutOfDate;

    /**
     * holder for the checksum.
     **/
	private	CRC32		checksum;

    /**
     * Minimum space to reserve for record portion length of row.
     * <p>
     * minimumRecordSize is stored in the container handle.  It is used to 
     * reserved minimum space for recordPortionLength.  Default is 1.  To
     * get the value from the container handle: 
     * myContainer.getMinimumRecordSize();
     *
     * minimumRecordSize is the minimum user record size, excluding the space we
     * use for the record header and field headers.  When a record is inserted,
     * it is stored in a space at least as large as the sum of the 
     * minimumRecordSize and total header size.
     *
     * For example,
     * If minimumRecordSize is 10 bytes,
     *     the user record is 7 bytes,
     *     we used 5 bytes for record and field headers,
     *     this record will take (10 + 5) bytes of space, extra 3 bytes is 
     *     put into reserve.
     *
     * If minimumRecordSize is 10 bytes,
     *     user record is 17 bytes,
     *     we used 5 bytes for record and field headers,
     *     this record will take (17 + 5) bytes of space, no reserve space 
     *     here.
     *
     * minimumRecordSize is defined by user on per container basis.
     * The default for minimumRecordSize is set to 1.
     *
     **/
	protected int minimumRecordSize;

    /**
     * scratch variable used to keep track of the total user size for the row.
     * the information is used by logRow to maintain minimumRecordSize
     * on Page.  minimumRecordSize is only considered for main data pages,
     * therefore, the page must be latched during an insert operation.
     **/
	private int userRowSize;
	
    /**
     * slot field and slot entry size.
     * <p>
     * The size of these fields is dependant on the page size.
     * These 2 variables should be set when pageSize is determined, and should
     * not be changed for that page.
     *
     * Each slot entry contains 3 fields (slotOffet, recordPortionLength and
     * reservedSpace) for the record the slot is pointing to.
     * slotFieldSize is the size for each of the slot field.
     * slotEntrySize is the total space used for a single slot entry.
     **/
	private int	slotFieldSize;
	private int	slotEntrySize;

    /**
     * Offset of the first entry in the slot table.
     * <p>
     * Offset table is located at end of page, just before checksum.  It
     * grows backward as an array from this point toward the middle of the
     * page.
     * <p>
     * slotTableOffsetToFirstEntry is the offset to the beginning of the
     * first entry (slot[0]) in the slot table.  This allows the following
     * math to get to the offset of N'th entry in the slot table:
     *
     *     offset of slot[N] = slotTableOffsetToFirstEntry + (N * slotEntrySize)
     **/
	private int slotTableOffsetToFirstEntry;

    /**
     * Offset of the record length entry in the 1st slot table entry.
     * <p>
     * Offset table is located at end of page, just before checksum.  It
     * grows backward as an array from this point toward the middle of the
     * page.  The record length is stored as the second "field" of the 
     * slot table entry.
     * <p>
     * slotTableOffsetToFirstRecordLengthField is the offset to the beginning 
     * of the record length field in the first entry (slot[0]) in the slot 
     * table.  This allows the following
     * math to get to the record length field of N'th entry in the slot table:
     *
     *     offset of record length of slot[N] slot entry = 
     *         slotTableOffsetToFirstRecordLengthField + (N * slotEntrySize)
     **/
	private int slotTableOffsetToFirstRecordLengthField;


    /**
     * Offset of the reserved space length entry in the 1st slot table entry.
     * <p>
     * Offset table is located at end of page, just before checksum.  It
     * grows backward as an array from this point toward the middle of the
     * page.  The reserved space length is stored as the third "field" of the 
     * slot table entry.
     * <p>
     * slotTableOffsetToFirstReservedSpaceField is the offset to the beginning 
     * of the reserved space field in the first entry (slot[0]) in the slot 
     * table.  This allows the following
     * math to get to the reserved space field of N'th entry in the slot table:
     *
     *     offset of reserved space of slot[N] slot entry = 
     *         slotTableOffsetToFirstReservedSpaceField + (N * slotEntrySize)
     **/
	private int slotTableOffsetToFirstReservedSpaceField;

    /**
     * total usable space on a page.
     * <p>
     * This is the space not taken by page hdr, page table, and existing
     * slot entries/rows.
     **/
	protected int	totalSpace;			// total usable space on a page

	// freeSpace and firstFreeByte are initliazed to a minimum value.
	protected int freeSpace		= Integer.MIN_VALUE; // free space on the page
	private   int firstFreeByte	= Integer.MIN_VALUE; // 1st free byte on page


    /**
     * % of page to keep free for updates.
     * <p>
     * How much of a head page should be reserved as "free" so that the space
     * can be used by update which expands the row without needing to overflow
     * it.  1 means save 1% of the free space for expansion.
     **/
	protected int	spareSpace;

    /**
     * Scratch variable used when you need a overflowRecordHeader.  Declared
     * globally so that object is only allocated once per page.
     **/
	private StoredRecordHeader  overflowRecordHeader;

    /**
     * Input streams used to read/write bytes to/from the page byte array.
     **/
	protected ArrayInputStream		rawDataIn;
	protected ArrayOutputStream     rawDataOut;
	protected FormatIdOutputStream  logicalDataOut;


    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */


    /**
     * Simple no-arg constructor for StoredPage.
     **/
	public StoredPage()
	{
		super();
	}

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**
     * get scratch space for over flow record header.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    
	private StoredRecordHeader getOverFlowRecordHeader()
		throws StandardException
    {
        return(
            overflowRecordHeader != null ? 
                overflowRecordHeader : 
                (overflowRecordHeader = new StoredRecordHeader()));
    }

    /**
     * Initialize the StoredPage.
     * <p>
     * Initialize the object, ie. perform work normally perfomed in constructor.
     * Called by setIdentity() and createIdentity() - the Cacheable interfaces
     * which are used to move a page in/out of cache.
     *
	 * @return void
     **/
	protected void initialize()
	{
		super.initialize();

		if (rawDataIn == null) 
        {
			rawDataIn            = new ArrayInputStream();
			checksum             = new CRC32();
		}

		if (pageData != null)
			rawDataIn.setData(pageData);
	}


    /**
     * Create the output streams.
     * <p>
     * Create the output streams, these are created on demand
     * to avoid creating unrequired objects for pages that are
     * never modified during their lifetime in the cache.
     * <p>
     *
	 * @return void
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private void createOutStreams()
	{
		rawDataOut = new ArrayOutputStream();
		rawDataOut.setData(pageData);

		logicalDataOut = new FormatIdOutputStream(rawDataOut);
	}

    /**
     * Tie the logical output stream to a passed in OutputStream.
     * <p>
     * Tie the logical output stream to a passed in OutputStream with
     * no limit as to the number of bytes that can be written.
     **/
	private void setOutputStream(OutputStream out)
	{
		if (rawDataOut == null)
			createOutStreams();

		logicalDataOut.setOutput(out);
	}

    /**
     * Reset the logical output stream.
     * <p>
     * Reset the logical output stream (logicalDataOut) to be attached
     * to the page array stream as is the norm, no limits are placed 
     * on any writes.
     *
     **/
	private void resetOutputStream()
	{

		logicalDataOut.setOutput(rawDataOut);
	}

    /**************************************************************************
     * Protected Methods of CachedPage class: (create, read and write a page.)
     **************************************************************************
     */

    /**
	 * use this passed in page buffer as this object's page data
     * <p>
	 * The page content may not have been read in from disk yet.
	 * For pagesize smaller than 64K:
	 *		Size of the record offset stored in a slot (unsigned short)
	 *		Size of the record portion length stored in a slot (unsigned short)
	 *		Size of the record portion length stored in a slot (unsigned short)
	 *	For pagesize greater than 64K, but less than 2gig:
	 *		Size of the record offset stored in a slot (int)
	 *		Size of the record portion length stored in a slot (int)
	 *		Size of the record portion length stored in a slot (int)
     * <p>
     *
     * @param pageBuffer    The array of bytes to use as the page buffer.
     **/
	protected void usePageBuffer(byte[] pageBuffer)
	{
		pageData = pageBuffer;

		int pageSize = pageData.length;
		if (rawDataIn != null)
			rawDataIn.setData(pageData);

        initSpace();

		if (pageSize >= 65536)
			slotFieldSize = LARGE_SLOT_SIZE;
		else
			slotFieldSize = SMALL_SLOT_SIZE;
		
		slotEntrySize = 3 * slotFieldSize;

        // offset of slot table entry[0]
        slotTableOffsetToFirstEntry = 
            (pageSize - CHECKSUM_SIZE - slotEntrySize);

        // offset of record length field in slot table entry[0]
	    slotTableOffsetToFirstRecordLengthField = 
            slotTableOffsetToFirstEntry + slotFieldSize;

        // offset of reserved space field in slot table entry[0]
	    slotTableOffsetToFirstReservedSpaceField =
            slotTableOffsetToFirstEntry + (2 * slotFieldSize);

		if (rawDataOut != null)
			rawDataOut.setData(pageData);
	}


    /**
     * Create a new StoredPage.
     * <p>
     * Make this object represent a new page (ie. a page that never existed
     * before, as opposed to reading in an existing page from disk).
     * <p>
     *
     * @param newIdentity   The key describing page (segment,container,page).
     * @param args          information stored about the page, once in the 
     *                      container header and passed in through the array.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected void createPage(
    PageKey newIdentity, 
    int[]   args) 
		 throws StandardException
	{
		// arg[0] is the formatId of the page
		// arg[1] is whether to sync the page to disk or not

		int pageSize        = args[2];
		spareSpace          = args[3];
		minimumRecordSize   = args[4];

        setPageArray(pageSize);

		cleanPage();			// clean up the page array

		setPageVersion(0);		// page is being created for the first time

		nextId          = RecordHandle.FIRST_RECORD_ID; // first record Id
		generation      = 0;
		prevGeneration  = 0;		// there is no previous generation
		bipLocation     = 0L;

		createOutStreams();
	}

    /**
     * Initialize the page from values in the page buffer.
     * <p>
     * Initialize in memory structure using the buffer in pageData.  This
     * is how a StoredPage object is intialized to represent page read in
     * from disk.
     * <p>
     *
     * @param myContainer   The container to read the page in from.
     * @param newIdentity   The key representing page being read in (segment,
     *                      container, page number)
     *
	 * @exception StandardException If the page cannot be read correctly, 
     *                              or is inconsistent.
     **/
	protected void initFromData(
    FileContainer   myContainer, 
    PageKey         newIdentity)
		 throws StandardException 
	{
		if (myContainer != null)
		{
            // read in info about page stored once in the container header.

			spareSpace          = myContainer.getSpareSpace();
			minimumRecordSize   = myContainer.getMinimumRecordSize();
		}

		// if it is null, assume spareSpace and minimumRecordSize is the
		// same.  We would only call initFromData after a restore then.

		try 
        {
			readPageHeader();
			initSlotTable();
		}
        catch (IOException ioe) 
        {
			// i/o methods on the byte array have thrown an IOException
			throw dataFactory.markCorrupt(
                StandardException.newException(
                    SQLState.DATA_CORRUPT_PAGE, ioe, newIdentity));
		}

		try
		{
			validateChecksum(newIdentity);
		}
		catch (StandardException se)
		{
			if (se.getMessageId().equals(SQLState.FILE_BAD_CHECKSUM))
			{
				// it is remotely possible that the disk transfer got garbled, 
				// i.e., the page is actually fine on disk but the version we
				// got has some rubbish on it.  Double check.
				int pagesize        = getPageSize();
				byte[] corruptPage  = pageData;
				pageData            = null;	// clear this

				// set up the new page array
				setPageArray(pagesize);

				try 
                {
					myContainer.readPage(newIdentity.getPageNumber(), pageData);
				} 
                catch (IOException ioe) 
                {
					throw dataFactory.markCorrupt(
                        StandardException.newException(
                            SQLState.DATA_CORRUPT_PAGE, ioe, newIdentity));
				}

				if (SanityManager.DEBUG)
				{
					SanityManager.DEBUG_CLEAR("TEST_BAD_CHECKSUM");
				}
				
				// see if this read confirms the checksum error
				try
				{
					validateChecksum(newIdentity);
				}
				catch (StandardException sse)
				{
					// really bad
					throw dataFactory.markCorrupt(se);
				}

				// If we got here, this means the first read is bad but the
				// second read is good.  This could be due to disk I/O error or
				// a bug in the way the file pointer is mis-managed.
				String firstImage   = pagedataToHexDump(corruptPage);
				String secondImage  = 
                    (SanityManager.DEBUG) ? 
                        toString() : pagedataToHexDump(corruptPage);

				throw StandardException.newException(
                        SQLState.FILE_IO_GARBLED, se,
                        newIdentity, firstImage, secondImage);
			}
			else
			{
				throw se;
			}
		}
	

	}

    /**
     * Validate the check sum on the page.
     * <p>
     * Compare the check sum stored in the page on disk with the checksum
     * calculated from the bytes on the page.
     * <p>
     *
     * @param id     The key that describes the page.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected void validateChecksum(PageKey id) 
        throws StandardException
	{
		long onDiskChecksum;

		try 
        {
	        // read the checksum stored on the page on disk.  It is stored
            // in the last "CHECKSUM_SIZE" bytes of the page, and is a long.

			rawDataIn.setPosition(getPageSize() - CHECKSUM_SIZE);
			onDiskChecksum = rawDataIn.readLong();
		} 
        catch (IOException ioe) 
        {

			// i/o methods on the byte array have thrown an IOException
			throw dataFactory.markCorrupt(
                StandardException.newException(
                    SQLState.DATA_CORRUPT_PAGE, ioe, id));
		}

		// Force the checksum to be recalculated based on the current page.
		checksum.reset();
		checksum.update(pageData, 0, getPageSize() - CHECKSUM_SIZE);
		
		// force a bad checksum error
		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON("TEST_BAD_CHECKSUM"))
            {
                // set on disk checksum to wrong value
				onDiskChecksum = 123456789;	
            }
		}

		if (onDiskChecksum != checksum.getValue())
		{
			// try again using new checksum object to be doubly sure
			CRC32 newChecksum = new CRC32();
			newChecksum.reset();
			newChecksum.update(pageData, 0, getPageSize()-CHECKSUM_SIZE);
			if (onDiskChecksum != newChecksum.getValue())
			{
				throw StandardException.newException(
                    SQLState.FILE_BAD_CHECKSUM,
                    id, 
                    new Long(checksum.getValue()), 
                    new Long(onDiskChecksum), 
                    pagedataToHexDump(pageData));
			}
			else
			{
				// old one is bad, get rid of it
				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT("old checksum gets wrong value");

				checksum = newChecksum;
			}
		}
	}

    /**
     * Recalculate checksum and write it to the page array.
     * <p>
     * Recalculate the checksum of the page, and write the result back into
     * the last bytes of the page.
     *
	 * @exception  IOException  if writing to end of array fails.
     **/
	protected void updateChecksum() throws IOException
	{
		checksum.reset();
		checksum.update(pageData, 0, getPageSize() - CHECKSUM_SIZE);

		rawDataOut.setPosition(getPageSize() - CHECKSUM_SIZE);
		logicalDataOut.writeLong(checksum.getValue());
	}

    /**
     * Write information about page from variables into page byte array.
     * <p>
     * This routine insures that all information about the page is reflected
     * in the page byte buffer.  This involves moving information from local
     * variables into encoded version on the page in page header and checksum.
     * <p>
     *
	 * @return void.
     *
     * @param identity  The key of this page.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected void writePage(PageKey identity) 
        throws StandardException 
	{
		if (SanityManager.DEBUG) 
        {
            // some consistency checks on fields of the page, good to check
            // before we write them into the page.

			if ((freeSpace < 0) || 
                (firstFreeByte + freeSpace) != (getSlotOffset(slotsInUse - 1))) 
            {
                // make sure free space is not negative and does not overlap
                // used space.

				SanityManager.THROWASSERT("slotsInUse = " + slotsInUse
					+ ", firstFreeByte = " + firstFreeByte
					+ ", freeSpace = " + freeSpace 
					+ ", slotOffset = " + (getSlotOffset(slotsInUse - 1))
					+ ", page = " + this);
			}

			if ((slotsInUse == 0) &&
				(firstFreeByte != (getPageSize() - totalSpace - CHECKSUM_SIZE))) 
            {
				SanityManager.THROWASSERT("slotsInUse = " + slotsInUse
					+ ", firstFreeByte = " + firstFreeByte
					+ ", freeSpace = " + freeSpace 
					+ ", slotOffset = " + (getSlotOffset(slotsInUse - 1))
					+ ", page = " + this);
            }

		}

		try 
        {
			if (headerOutOfDate)
            {
				updatePageHeader();
            }
			else
            {
				// page version always need to be updated if page is dirty,
				// either do it in updatePageHeader or by itself
				updatePageVersion();
            }

			updateChecksum();

		} 
        catch (IOException ioe) 
        {
			// i/o methods on the byte array have thrown an IOException
			throw dataFactory.markCorrupt(
                StandardException.newException(
                    SQLState.DATA_CORRUPT_PAGE, ioe, identity));
		}

	}

    /**
     * Write out the format id of this page
     *
     * @param identity  The key of this page.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected void writeFormatId(PageKey identity) throws StandardException
	{
		try
		{
			if (rawDataOut == null)
				createOutStreams();

			rawDataOut.setPosition(0);

			FormatIdUtil.writeFormatIdInteger(
                logicalDataOut, getTypeFormatId());

		} 
        catch (IOException ioe) 
        {
			// i/o methods on the byte array have thrown an IOException
			throw dataFactory.markCorrupt(
                StandardException.newException(
                    SQLState.DATA_CORRUPT_PAGE, ioe, identity));
		}
	}


    /**************************************************************************
     * Protected Methods of Cacheable Interface:
     **************************************************************************
     */

    /**************************************************************************
     * Protected OverRidden Methods of BasePage:
     **************************************************************************
     */

    /**
     * Ensure that the page is released from the cache when it is unlatched.
     *
     * @see org.apache.derby.impl.store.raw.data.BasePage#releaseExclusive
     *
     **/
	protected void releaseExclusive()
	{
		super.releaseExclusive();

		pageCache.release(this);
	}


    /**
     * Return the total number of bytes used, reserved, or wasted by the
     * record at this slot.
     * <p>
     * The amount of space the record on this slot is currently taking on the 
     * page.
     *
     * If there is any reserve space or wasted space, count that in also
     * Do NOT count the slot entry size
     * <p>
     *
	 * @return The number of bytes used by the row at slot "slot".
     *
     * @param slot  look at row at this slot.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public int getTotalSpace(int slot) 
        throws StandardException
	{
		try 
        {
            // A slot entry looks like the following:
            //     1st field:   offset of the record on the page
            //     2nd field:   length of the record on the page
            //     3rd field:   amount of space reserved for the record to grow.

            // position the read at the beginning of the 2nd field.
			rawDataIn.setPosition(getSlotOffset(slot) + slotFieldSize);

            // return the size of the record + size of the reserved space. 
            // the size of the fields to read is determined by slotFieldSize.

            return(
			    ((slotFieldSize == SMALL_SLOT_SIZE) ?
				    (rawDataIn.readUnsignedShort() + 
                     rawDataIn.readUnsignedShort())     :
				    (rawDataIn.readInt() + 
                     rawDataIn.readInt())));
                
		} 
        catch (IOException ioe) 
        {
			throw dataFactory.markCorrupt(
                StandardException.newException(
                    SQLState.DATA_CORRUPT_PAGE, ioe, getPageId()));
		}
	}

    /**
     * Is there minimal space for insert?
     * <p>
     * Does quick calculation to see if average size row on this page could
     * be inserted on the page.  This is done because the actual row size
     * being inserted isn't known until we actually copy the columns from
     * their object form into their on disk form which is expensive.  So
     * we use this calculation so that in the normal case we only do one 
     * copy of the row directly onto the page.
     * <p>
     *
	 * @return true if we think the page will allow an insert, false otherwise.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public boolean spaceForInsert() 
        throws StandardException
	{
		// is this an empty page
		if (slotsInUse == 0)
			return(true);

		if (!allowInsert())
			return(false);

		int usedSpace   = totalSpace - freeSpace;
		int bytesPerRow = usedSpace / slotsInUse;

		return(bytesPerRow <= freeSpace);
	}

    /**
     * Is row guaranteed to be inserted successfully on this page?
     * <p>
     * Return true if this record is guaranteed to be inserted successfully 
     * using insert() or insertAtSlot(). This guarantee is only valid while
     * the row remains unchanged and the page latch is held.
     * <p>
     *
	 * @return bolean indicating if row can be inserted on this page.
     *
     * @param row                   The row to check for insert.
     * @param validColumns          bit map to interpret valid columns in row.
     * @param overflowThreshold     The percentage of the page to use for the
     *                              insert.  100 means use 100% of the page,
     *                              50 means use 50% of page (ie. make sure
     *                              2 rows fit per page).
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public boolean spaceForInsert(
    Object[]   row, 
    FormatableBitSet                 validColumns, 
    int                     overflowThreshold)
		throws StandardException
	{

		// is this an empty page
		if (slotsInUse == 0)
			return true;
		
        // does the estimate think it won't fit, if not return false to avoid
        // cost of calling logRow() just to figure out if the row will fit.
		if (!allowInsert())
			return false;

		DynamicByteArrayOutputStream out = new DynamicByteArrayOutputStream();

		try 
        {
			// This is a public call, start column is rawstore only.  
			// set the starting Column for the row to be 0.
			logRow(
                0, true, nextId, row, validColumns, out, 
                0, Page.INSERT_DEFAULT, -1, -1, overflowThreshold);

		} 
        catch (NoSpaceOnPage nsop) 
        {
			return false;
		} 
        catch (IOException ioe) 
        {
			throw StandardException.newException(
                SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}

		return true;
	}

    /**
     * Is row guaranteed to be inserted successfully on this page?
     * <p>
     * Return true if this record is guaranteed to be inserted successfully 
     * using insert() or insertAtSlot(). This guarantee is only valid while
     * the row remains unchanged and the page latch is held.
     * <p>
     * This is a private call only used when calculating whether an overflow
     * page can be used to insert part of an overflow row/column.
     *
	 * @return bolean indicating if row can be inserted on this page.
     *
     * @param row                   The row to check for insert.
     * @param validColumns          bit map to interpret valid columns in row.
     * @param overflowThreshold     The percentage of the page to use for the
     *                              insert.  100 means use 100% of the page,
     *                              50 means use 50% of page (ie. make sure
     *                              2 rows fit per page).
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private boolean spaceForInsert(
    Object[]   row, 
    FormatableBitSet                 validColumns,
    int                     spaceNeeded,
    int                     startColumn, 
    int                     overflowThreshold)
		throws StandardException 
	{
		if (!(spaceForInsert() && (freeSpace >= spaceNeeded)))
			return false;

		DynamicByteArrayOutputStream out = new DynamicByteArrayOutputStream();

		try 
        {
			logRow(
                0, true, nextId, row, validColumns, out, startColumn, 
                Page.INSERT_DEFAULT, -1, -1, overflowThreshold);

		} 
        catch (NoSpaceOnPage nsop) 
        {
			return false;
		} 
        catch (IOException ioe) 
        {
			throw StandardException.newException(
                SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}

		return true;
	}

    /**
     * Is this page unfilled?
     * <p>
     * Returns true if page is relatively unfilled, 
     * which means the page is < 1/2 full and has enough space to insert an
     * "average" sized row onto the page.
     * <p>
     *
	 * @return true if page is relatively unfilled.
     **/
	public boolean unfilled()
	{
		return (allowInsert() && (freeSpace > (getPageSize() / 2)));
	}

    /**
     * Is there enough space on the page to insert a minimum size row?
     * <p>
     * Calculate whether there is enough space on the page to insert a 
     * minimum size row.  The calculation includes maintaining the required
     * reserved space on the page for existing rows to grow on the page.
     * <p>
     *
	 * @return boolean indicating if a minimum sized row can be inserted.
     **/
	public boolean allowInsert()
	{
		// is this an empty page
		if (slotsInUse == 0)
			return true;

		int spaceAvailable = freeSpace;

		spaceAvailable -= slotEntrySize;	// need to account new slot entry

		if (spaceAvailable < minimumRecordSize)
			return false;

		// see that we reserve enough space for existing rows to grow on page
		if (((spaceAvailable * 100) / totalSpace) < spareSpace)
			return false;

		return true;
	}

    /**
     * Does this page have enough space to insert the input rows?
     * <p>
     * Can the rows with lengths spaceNeeded[0..num_rows-1] be copied onto
     * this page?
     * <p>
     *
	 * @return true if the sum of the lengths will fit on the page.
     *
     * @param num_rows      number of rows to check for.
     * @param spaceNeeded   array of lengths of the rows to insert.
     **/
	public boolean spaceForCopy(int num_rows, int[] spaceNeeded)
	{
		// determine how many more bytes are needed for the slot entries
		int bytesNeeded = slotEntrySize * num_rows;

		for (int i = 0; i < num_rows; i++) 
        {
			if (spaceNeeded[i] > 0) 
            {
                // add up the space needed by the rows, add in minimumRecordSize
                // if length of actual row is less than minimumRecordSize.

				bytesNeeded += 
                    (spaceNeeded[i] >= minimumRecordSize ? 
                         spaceNeeded[i] : minimumRecordSize);
			}
		}

		return((freeSpace - bytesNeeded) >= 0);
	}

    /**
     * Read the record at the given slot into the given row.
     * <P>
     * This reads and initializes the columns in the row array from the raw 
     * bytes stored in the page associated with the given slot.  If validColumns
     * is non-null then it will only read those columns indicated by the bit
     * set, otherwise it will try to read into every column in row[].  
     * <P>
     * If there are more columns than entries in row[] then it just stops after
     * every entry in row[] is full.
     * <P>
     * If there are more entries in row[] than exist on disk, the requested 
     * excess columns will be set to null by calling the column's object's
     * restoreToNull() routine (ie.  ((Object) column).restoreToNull() ).
     * <P>
     * If a qualifier list is provided then the row will only be read from
     * disk if all of the qualifiers evaluate true.  Some of the columns may
     * have been read into row[] in the process of evaluating the qualifier.
     * <p>
     * This routine should only be called on the head portion of a row, it
     * will call a utility routine to read the rest of the row if it is a
     * long row.
     *
     *
     * @param slot              the slot number
     * @param row (out)         filled in sparse row
     * @param validColumns      A bit map indicating which columns to return, if
     *                          null return all the columns.
     * @param qualifier_list    An array of qualifiers to apply to the row, only
     *                          return row if qualifiers are all true, if array
     *                          is null always return the row.
     * @param materializedCols  If a non-null qualifier_list is provided, then 
     *                          this array of int's will be used to track which
     *                          cols have been materialized during the 
     *                          qualification phase, so that they are not 
     *                          materialized again during the fetch phase.
     * @param recordToLock      the record handle for the row at top level,
     *                          and is used in OverflowInputStream to lock the 
     *                          row for Blobs/Clobs.
     * @param isHeadRow         The row on this page includes the head record
     *                          handle.  Will be false for the overflow portions
     *                          of a "long" row, where columns of a row span
     *                          multiple pages.
     *
     * @return  false if a qualifier_list is provided and the row does not 
     *          qualifier (no row read in that case), else true.
     *
     * @exception StandardException	Standard Cloudscape error policy
     **/
	protected boolean restoreRecordFromSlot(
    int                     slot, 
    Object[]                row, 
    FetchDescriptor         fetchDesc,
    RecordHandle            recordToLock,
    StoredRecordHeader      recordHeader,
    boolean                 isHeadRow)
		throws StandardException
	{
		try 
        {
			int offset_to_row_data = 
                getRecordOffset(slot) + recordHeader.size();

			if (SanityManager.DEBUG) 
            {
				if (getRecordOffset(slot) < 
                        (PAGE_HEADER_OFFSET + PAGE_HEADER_SIZE)) 
                {
					SanityManager.THROWASSERT(
                        "Incorrect offset.  offset = " + 
                            getRecordOffset(slot) + 
                        ", offset should be < " + 
                        "(PAGE_HEADER_OFFSET + PAGE_HEADER_SIZE) = " + 
                            (PAGE_HEADER_OFFSET + PAGE_HEADER_SIZE) + 
                        ", current slot = " + slot + 
                        ", total slotsInUse = " + slotsInUse);
				}

                SanityManager.ASSERT(
                    isHeadRow, "restoreRecordFromSlot called on a non-headrow");
                SanityManager.ASSERT(
                    !isOverflowPage(), 
                    "restoreRecordFromSlot called on an overflow page.");
			}

            // position the array reading stream at beginning of row data just
            // past the record header.
			ArrayInputStream lrdi = rawDataIn;
			lrdi.setPosition(offset_to_row_data);

			if (!recordHeader.hasOverflow())
            {
                if (isHeadRow)
                {
                    if (fetchDesc != null && 
                        fetchDesc.getQualifierList() != null)
                    {
                        fetchDesc.reset();

                        if (!qualifyRecordFromSlot(
                                row, 
                                offset_to_row_data, 
                                fetchDesc,
                                recordHeader,
                                recordToLock))
                        {
                            return(false);
                        }
                        else
                        {
                            // reset position back for subsequent record read.
                            lrdi.setPosition(offset_to_row_data);
                        }
                    }
                }

                // call routine to do the real work.  Note that 
                // readRecordFromStream() may return false for non-overflow
                // record, this is in the case where caller requests more 
                // columns than exist on disk.  In that case we still return
                // true at this point as there are no more columns that we
                // can return.
                if (fetchDesc != null)
                {
                    readRecordFromArray(
                        row, 
                        (fetchDesc.getValidColumns() == null) ?
                            row.length -1 : fetchDesc.getMaxFetchColumnId(), 
                        fetchDesc.getValidColumnsArray(), 
                        fetchDesc.getMaterializedColumns(),
                        lrdi, 
                        recordHeader,
                        (ErrorObjectInput) null /* always null */, 
                        recordToLock);
                }
                else
                {
                    readRecordFromArray(
                        row, 
                        row.length - 1,
                        (int[]) null,
                        (int[]) null,
                        lrdi, 
                        recordHeader,
                        (ErrorObjectInput) null /* always null */, 
                        recordToLock);
                }

                return(true);
            }
            else
            {
                if (fetchDesc != null)
                {
                    if (fetchDesc.getQualifierList() != null)
                    {
                        fetchDesc.reset();
                    }

                    readRecordFromArray(
                        row, 
                        (fetchDesc.getValidColumns() == null) ?
                            row.length - 1 : fetchDesc.getMaxFetchColumnId(), 
                        fetchDesc.getValidColumnsArray(), 
                        fetchDesc.getMaterializedColumns(),
                        lrdi, 
                        recordHeader,
                        (ErrorObjectInput) null /* always null */, 
                        recordToLock);
                }
                else
                {
                    readRecordFromArray(
                        row, 
                        row.length - 1,
                        (int[]) null,
                        (int[]) null,
                        lrdi, 
                        recordHeader,
                        (ErrorObjectInput) null /* always null */, 
                        recordToLock);
                }

                // call routine to loop through all the overflow portions of
                // the row, reading it into "row".
                while (recordHeader != null)
                {
                    // The record is a long row, loop callng code to read the 
                    // pieces of the row located in a linked list of rows on 
                    // overflow pages.
                    StoredPage overflowPage = 
                        getOverflowPage(recordHeader.getOverflowPage());
                     
                    if (SanityManager.DEBUG)
                    {
                        if (overflowPage == null)
                            SanityManager.THROWASSERT(
                                "cannot get overflow page");
                    }

                    // This call reads in the columns of the row that reside
                    // on "overflowPage", and if there is another piece it
                    // returns the recordHeader of the row on overFlowPage,
                    // from which we can find the next piece of the row.  A
                    // null return means that we have read in the entire row,
                    // and are done.
                    recordHeader = 
                        overflowPage.restoreLongRecordFromSlot(
                            row, 
                            fetchDesc,
                            recordToLock,
                            recordHeader);

                    overflowPage.unlatch();
                    overflowPage = null;
                }

                // for overflow rows just apply qualifiers at end for now.

                if ((fetchDesc != null) && 
                    (fetchDesc.getQualifierList() != null))
                {
                    if (!qualifyRecordFromRow(
                            row, fetchDesc.getQualifierList()))
                    {
                        return(false);
                    }
                }

                return(true);
			}
		} 
        catch (IOException ioe) 
        {

			if (SanityManager.DEBUG)
			{
				if (pageData == null)
				{
					SanityManager.DEBUG_PRINT("DEBUG_TRACE",
						"caught an IOException in restoreRecordFromSlot " +
						(PageKey)getIdentity() + " slot " + slot + 
						", pageData is null");
				}
				else
				{
					SanityManager.DEBUG_PRINT("DEBUG_TRACE",
						"caught an IOException in reestoreRecordFromSlot, " + 
						(PageKey)getIdentity() + " slot " + slot + 
						", pageData.length = " + 
						pageData.length + " pageSize = " + getPageSize());
					SanityManager.DEBUG_PRINT("DEBUG_TRACE",
						"Hex dump of pageData \n " +
						"--------------------------------------------------\n" +
						pagedataToHexDump(pageData) + 
						"--------------------------------------------------\n");
					SanityManager.DEBUG_PRINT("DEBUG_TRACE",
						"Attempt to dump page " + this.toString());
				}
			}

			// i/o methods on the byte array have thrown an IOException
			throw dataFactory.markCorrupt(
                StandardException.newException(
                    SQLState.DATA_CORRUPT_PAGE, ioe, getPageId()));
		}
	}

	private StoredRecordHeader restoreLongRecordFromSlot(
    Object[]                row, 
    FetchDescriptor         fetchDesc,
    RecordHandle            recordToLock,
    StoredRecordHeader      parent_recordHeader)
		throws StandardException
	{

        int slot = 
            findRecordById(
                parent_recordHeader.getOverflowId(), Page.FIRST_SLOT_NUMBER);

        StoredRecordHeader recordHeader = getHeaderAtSlot(slot);

        try 
        {
            int offset_to_row_data = 
                getRecordOffset(slot) + recordHeader.size();

            if (SanityManager.DEBUG) 
            {
                if (getRecordOffset(slot) < 
                        (PAGE_HEADER_OFFSET + PAGE_HEADER_SIZE)) 
                {
                    SanityManager.THROWASSERT(
                        "Incorrect offset.  offset = " + 
                            getRecordOffset(slot) + 
                        ", offset should be < " + 
                        "(PAGE_HEADER_OFFSET + PAGE_HEADER_SIZE) = " + 
                            (PAGE_HEADER_OFFSET + PAGE_HEADER_SIZE) + 
                        ", current slot = " + slot + 
                        ", total slotsInUse = " + slotsInUse);
                }
            }

            // position the array reading stream at beginning of row data 
            // just past the record header.
            ArrayInputStream lrdi = rawDataIn;
            lrdi.setPosition(offset_to_row_data);

            if (fetchDesc != null)
            {
                if (fetchDesc.getQualifierList() != null)
                {
                    fetchDesc.reset();
                }

                readRecordFromArray(
                    row, 
                    (fetchDesc.getValidColumns() == null) ?
                        row.length - 1 : fetchDesc.getMaxFetchColumnId(), 
                    fetchDesc.getValidColumnsArray(), 
                    fetchDesc.getMaterializedColumns(),
                    lrdi, 
                    recordHeader,
                    (ErrorObjectInput) null /* always null */, 
                    recordToLock);
            }
            else
            {
                readRecordFromArray(
                    row, 
                    row.length - 1,
                    (int[]) null,
                    (int[]) null,
                    lrdi, 
                    recordHeader,
                    (ErrorObjectInput) null /* always null */, 
                    recordToLock);
            }

            return(recordHeader.hasOverflow() ? recordHeader : null);
        }
        catch (IOException ioe) 
        {
            if (SanityManager.DEBUG)
            {
                if (pageData == null)
                {
                    SanityManager.DEBUG_PRINT("DEBUG_TRACE",
                        "caught an IOException in restoreRecordFromSlot " +
                        (PageKey)getIdentity() + " slot " + slot + 
                        ", pageData is null");
                }
                else
                {
                    SanityManager.DEBUG_PRINT("DEBUG_TRACE",
                        "caught an IOException in reestoreRecordFromSlot, " + 
                        (PageKey)getIdentity() + " slot " + slot + 
                        ", pageData.length = " + 
                        pageData.length + " pageSize = " + getPageSize());
                    SanityManager.DEBUG_PRINT("DEBUG_TRACE",
                        "Hex dump of pageData \n " +
                        "--------------------------------------------------\n" +
                        pagedataToHexDump(pageData) + 
                        "--------------------------------------------------\n");
                    SanityManager.DEBUG_PRINT("DEBUG_TRACE",
                        "Attempt to dump page " + this.toString());
                }
            }

            // i/o methods on the byte array have thrown an IOException
            throw dataFactory.markCorrupt(
                StandardException.newException(
                    SQLState.DATA_CORRUPT_PAGE, ioe, getPageId()));
        }
	}

    /**
     * Create a new record handle.
     * <p>
     * Return the next record id for allocation.  Callers of this interface
     * expect the next id to get bumped some where else - probably by
     * storeRecordForInsert().
     * <p>
     *
	 * @return The next id to assing to a row.
     **/
	public int newRecordId()
	{
		return nextId;
	}

    /**
     * Create a new record handle, and bump the id.
     * <p>
     * Create a new record handle, and bump the id while holding the latch
     * so that no other user can ever see this record id.  This will lead
     * to unused record id's in the case where an insert fails because there
     * is not enough space on the page.
     * <p>
     *
	 * @return The next id to assing to a row.
     **/
	public int newRecordIdAndBump()
	{
        // headerOutOfDate must be bumped as nextId is changing, and must
        // eventually be updated in the page array.
		headerOutOfDate = true;	
							
		return nextId++;
	}


    /**
     * Create a new record id based on current one passed in.
     * <p>
     * This interface is used for the "copy" insert interface of raw store
     * where multiple rows are inserted into a page in a single logged 
     * operation.  We don't want to bump the id until the operation is logged
     * so we just allocated each id in order and then bump the next id at
     * the end of the operation.
     * <p>
     *
	 * @return the next id based on the input id.
     *
     * @param recordId  The id caller just used, return the next one.
     *
     **/
	protected int newRecordId(int recordId)
	{
		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(
                recordId >= nextId, 
                "should not create a record Id that is already given out");
		}

		return recordId + 1;
	}

	public boolean isOverflowPage()
	{
		return isOverflowPage;
	}



    /**************************************************************************
     * Public Methods specific to StoredPage:
     **************************************************************************
     */

    /**
     * Get the full size of the page.
     **/
	public final int getPageSize()
	{
		return pageData.length;
	}


    /**
     * Zero out a portion of the page.
     * <p>
     **/
	protected final void clearSection(int offset, int length)
	{
		int endOffset = offset + length;

		while (offset < endOffset) 
			pageData[offset++] = 0;
	}

    /**
     * The maximum free space on this page possible.
     * <p>
     * The the maximum amount of space that can be used on the page
     * for the records and the slot offset table.
     * NOTE: subclass may have overwitten it to report less freeSpace
     *
	 * @return the maximum free space on this page possible.
     *
     **/
	protected int getMaxFreeSpace()
	{
		return getPageSize() - RECORD_SPACE_OFFSET - CHECKSUM_SIZE;
	}

    /**
     * The current free space on the page.
     **/
	protected int getCurrentFreeSpace()
	{
		return freeSpace;
	}

    /**************************************************************************
     * Page header routines
     **************************************************************************
     */

    /**
     * Read the page header from the page array.
     * <p>
     * Read the page header from byte form in the page array into in memory
     * variables.
     **/
	private void readPageHeader() 
        throws IOException
	{
		// these reads are always against the page array
		ArrayInputStream lrdi = rawDataIn;

		lrdi.setPosition(PAGE_HEADER_OFFSET);
		long spare;

		isOverflowPage  =	lrdi.readBoolean();
		setPageStatus		(lrdi.readByte());
		setPageVersion		(lrdi.readLong());
		slotsInUse      =	lrdi.readUnsignedShort();
		nextId          =	lrdi.readInt();
		generation      =	lrdi.readInt();     // page generation (Future Use)
		prevGeneration  =	lrdi.readInt();     // previous generation (Future Use)
		bipLocation     =	lrdi.readLong();	// BIPage location (Future Use)

		// number of deleted rows on page, we start to store this release 2.0.
		// for upgrade reasons, a 0 on disk means -1, so, we subtract one here.
		deletedRowCount =	lrdi.readUnsignedShort() - 1;

        // the next 4 (total 22 bytes) are reserved for future
		spare           =   lrdi.readUnsignedShort();	
		spare           =   lrdi.readInt();     // used by encryption
		spare           =   lrdi.readLong();
		spare           =   lrdi.readLong();
	}


    /**
     * Update the page header in the page array.
     * <p>
     * Write the bytes of the page header, taking the values from those 
     * in the in memory variables.
     **/
	private void updatePageHeader() 
        throws IOException
	{
		rawDataOut.setPosition(PAGE_HEADER_OFFSET);

		logicalDataOut.writeBoolean(isOverflowPage);
		logicalDataOut.writeByte(getPageStatus());
		logicalDataOut.writeLong(getPageVersion());
		logicalDataOut.writeShort(slotsInUse);
		logicalDataOut.writeInt(nextId);
		logicalDataOut.writeInt(generation);     // page generation (Future Use)
		logicalDataOut.writeInt(prevGeneration); // previous generation (Future Use)
		logicalDataOut.writeLong(bipLocation);	 // BIPage location (Future Use)

		// number of deleted rows on page, we start to store this release 2.0.
		// for upgrade reasons, a 0 on disk means -1, so, we add one when we 
        // write it to disk.
		logicalDataOut.writeShort(deletedRowCount + 1);

		logicalDataOut.writeShort(0);	         // reserved for future
		logicalDataOut.writeInt(
                dataFactory.random());	         // random bytes for encryption  
		logicalDataOut.writeLong(0);             // reserved for future
		logicalDataOut.writeLong(0);             // reserved for future

		// we put a random value int into the page if the database is encrypted
		// so that the checksum will be very different even with the same
		// page image, when we encrypt or decrypt the page, we move the
		// checksum to the front so that the encrypted page will look very
		// different even with just the one int difference.  We never look at
		// the value of the random number and we could have put it anywhere in
		// the page as long as it doesn't obscure real data.
		
		headerOutOfDate = false;
	}

    /**
     * Update the page version number in the byte array
     **/
	private void updatePageVersion() 
        throws IOException 
	{
		rawDataOut.setPosition(PAGE_VERSION_OFFSET);
		logicalDataOut.writeLong(getPageVersion());
	}

    /**************************************************************************
     * Slot Offset & Length table manipulation
     **************************************************************************
     */

    /**
     * Get the page offset of a given slot entry.
     * <p>
     * Get the page offset of a slot entry, this is not the offset of
     * the record stored in the slot, but the offset of the actual slot.
     *
	 * @return The page offset of a given slot entry.
     *
     * @param slot  The array entry of the slot to find.
     **/
	private int getSlotOffset(int slot)
	{
        // slot table grows backward from the spot at the end of the page just
        // before the checksum which is located in the last 8 bytes of the page.

        return(slotTableOffsetToFirstEntry - (slot * slotEntrySize));
	}

    /**
     * Get the page offset of the record associated with the input slot.
     * <p>
     * This is the actual offset on the page of the beginning of the record.
     *
	 * @return The page offset of the record associated with the input slot.
     *
     * @param slot  The array entry of the slot to find.
     **/
	private int getRecordOffset(int slot) 
	{
        byte[] data   = pageData;
        int    offset = slotTableOffsetToFirstEntry - (slot * slotEntrySize);

        // offset on the page of the record is stored in the first 2 or 4 bytes
        // of the slot table entry.  Code has been inlined for performance
        // critical low level routine.
        //
        // return( 
        //  (slotFieldSize == SMALL_SLOT_SIZE) ?
        //       readUnsignedShort() : readInt());

        return(
            (slotFieldSize == SMALL_SLOT_SIZE)  ?

             ((data[offset++]  & 0xff) <<  8) | 
              (data[offset]    & 0xff)          :

             (((data[offset++] & 0xff) << 24) |
              ((data[offset++] & 0xff) << 16) |
              ((data[offset++] & 0xff) <<  8) |
              ((data[offset]   & 0xff)      )));
	}

    /**
     * Set the page offset of the record associated with the input slot.
     * <p>
     * This is the actual offset on the page of the beginning of the record.
     *
     * @param slot          The array entry of the slot to set.
     * @param recordOffset  the new offset to set.
     **/
	private void setRecordOffset(int slot, int recordOffset) 
        throws IOException
	{
		rawDataOut.setPosition(getSlotOffset(slot));

		if (slotFieldSize == SMALL_SLOT_SIZE)
			logicalDataOut.writeShort(recordOffset);
		else
			logicalDataOut.writeInt(recordOffset);
	}

    /**
     * Return length of row on this page.
     * <p>
     * Return the total length of data and header stored on this page for 
     * this record.  This length is stored as the second "field" of the
     * slot table entry.
     *
	 * @return The length of the row on this page.
     *
     * @param slot   the slot of the row to look up the length of.
     *
     **/
	protected int getRecordPortionLength(int slot) 
        throws IOException
	{
		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(getRecordOffset(slot) != 0);
		}

		// these reads are always against the page array
		ArrayInputStream lrdi = rawDataIn;

		lrdi.setPosition(
            slotTableOffsetToFirstRecordLengthField - (slot * slotEntrySize));

        return( 
            (slotFieldSize == SMALL_SLOT_SIZE) ?
                lrdi.readUnsignedShort() : lrdi.readInt());
	}

    /**
     * Return reserved length of row on this page.
     * <p>
     * Return the reserved length of this record.  
     * This length is stored as the third "field" of the slot table entry.
     *
	 * @return The reserved length of the row on this page.
     *
     * @param slot   the slot of the row to look up the length of.
     *
     **/
	public int getReservedCount(int slot) throws IOException
	{
		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(getRecordOffset(slot) != 0);
		}

		// these reads are always against the page array
		ArrayInputStream lrdi = rawDataIn;

		lrdi.setPosition(
            slotTableOffsetToFirstReservedSpaceField - (slot * slotEntrySize));

        return( 
            (slotFieldSize == SMALL_SLOT_SIZE) ?
                lrdi.readUnsignedShort() : lrdi.readInt());
	}


	/**
		Update the length of data stored on this page for this record
	*/
    /**
     * Update the length of data stored on this page for this record
     * <p>
     * Update both the record length "field" and the reserved space "field"
     * of the slot table entry associated with "slot".  This length is stored 
     * as the second "field" of the slot table entry.  The changes to these
     * 2 fields are represented as the delta to apply to each field as input
     * in "delta" and "reservedDelta."
     * <p>
     *
     * @param slot              the slot of the record to set.
     * @param delta             The amount the record length changed.
     * @param reservedDelta     The amount the reserved length changed.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private void updateRecordPortionLength(
    int slot, 
    int delta, 
    int reservedDelta)
		throws IOException
	{
		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(getRecordOffset(slot) != 0);

			if ((delta + reservedDelta) < 0)
				SanityManager.THROWASSERT(
					"total space of record is not allowed to shrink, delta == "
					+ delta + " reservedDelta = " + reservedDelta);

			if ((getRecordPortionLength(slot) + delta) < 0)
				SanityManager.THROWASSERT(
					"record portion length cannot be < 0.recordPortionLength = "
					+ getRecordPortionLength(slot) + " delta = " + delta);

			if ((getReservedCount(slot) + reservedDelta) < 0)
				SanityManager.THROWASSERT(
					"reserved space for record cannot be < 0.  reservedCount = "
					+ getReservedCount(slot) + " reservedDelta = "
					+ reservedDelta);
		}

        // position the stream to beginning of 2nd field of slot entry.
		rawDataOut.setPosition(
            slotTableOffsetToFirstRecordLengthField - (slot * slotEntrySize));

        // write the new record length to 2nd field
		if (slotFieldSize == SMALL_SLOT_SIZE)
			logicalDataOut.writeShort(getRecordPortionLength(slot) + delta);
		else
			logicalDataOut.writeInt(getRecordPortionLength(slot) + delta);

        // if necessary, write the 3rd field - above write has positioned the
        // stream to the 3rd field.
		if (reservedDelta != 0) 
        {
			if (slotFieldSize == SMALL_SLOT_SIZE)
            {
				logicalDataOut.writeShort(
                    getReservedCount(slot) + reservedDelta);
            }
			else
            {
				logicalDataOut.writeInt(
                    getReservedCount(slot) + reservedDelta);
            }
		}
	}

    /**
     * Initialize the in-memory slot table.
     * <p>
     * Initialize the in-memory slot table, ie. that of our super-class 
     * BasePage.  Go through all the records on the page and set the 
     * freeSpace and firstFreeByte on page.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private void initSlotTable() 
        throws StandardException
	{
		int localSlotsInUse = slotsInUse;

		// must initialize the header now
		initializeHeaders(localSlotsInUse);

		// mark all the space on the page as free
		clearAllSpace();
		
		// first count the space occupied by the slot table
		freeSpace -= localSlotsInUse * slotEntrySize;

		int lastSlotOnPage      = -1;
		int lastRecordOffset    = -1;
		
		try 
        {
			for (int slot = 0; slot < localSlotsInUse; slot++) 
            {
				if (SanityManager.DEBUG) 
                {
					if (!isOverflowPage() && 
                        minimumRecordSize > getTotalSpace(slot))
                    {
						SanityManager.THROWASSERT(
							" slot " + slot +
							" minimumRecordSize = " + minimumRecordSize + 
							" totalSpace = " + getTotalSpace(slot) + 
							"recordPortionLength = " + 
                                getRecordPortionLength(slot) 
							+ " reservedCount = " + getReservedCount(slot));
                    }
				}

				int recordOffset = getRecordOffset(slot);

				// check that offset points into the record space area.
				if ((recordOffset < RECORD_SPACE_OFFSET) || 
                    (recordOffset >= (getPageSize() - CHECKSUM_SIZE))) 
                {
                    throw dataFactory.markCorrupt(
                        StandardException.newException(
                            SQLState.DATA_CORRUPT_PAGE, getPageId()));
				}

				if (recordOffset > lastRecordOffset) 
                {
					lastRecordOffset = recordOffset;
					lastSlotOnPage = slot;
				}
			}

			bumpRecordCount(localSlotsInUse);

			if (lastSlotOnPage != -1) 
            {
				// Calculate the firstFreeByte for the page, 
                // and the freeSpace on Page

				firstFreeByte = 
                    lastRecordOffset + getTotalSpace(lastSlotOnPage);
				freeSpace    -= firstFreeByte - RECORD_SPACE_OFFSET;
			}

			if (SanityManager.DEBUG) 
            {
				if ((freeSpace < 0) || 
                    ((firstFreeByte + freeSpace) != 
                         (getSlotOffset(slotsInUse - 1)))) 
                {
					SanityManager.THROWASSERT(
                        "firstFreeByte = " + firstFreeByte
						+ ", freeSpace = " + freeSpace
						+ ", slotOffset = " + (getSlotOffset(slotsInUse - 1))
						+ ", slotsInUse = " + localSlotsInUse);
				}

				if (localSlotsInUse == 0)
                {
					SanityManager.ASSERT(
                        firstFreeByte == 
                            (getPageSize() - totalSpace - CHECKSUM_SIZE));
                }
			}

			// upgrade issue. Pre 1.5 release, we do not store deletedRowCount
			// therefore, if we are accessing an older database,
			// we need to calculate the deletedRowCount here.
			if (deletedRowCount == -1) 
            {
				int count = 0;
				int	maxSlot = slotsInUse;
				for (int slot = FIRST_SLOT_NUMBER ; slot < maxSlot; slot++) 
                {
					if (isDeletedOnPage(slot))
						count++;
				}
				deletedRowCount = count;
			}

		} 
        catch (IOException ioe) 
        {
			// i/o methods on the byte array have thrown an IOException
            throw dataFactory.markCorrupt(
                StandardException.newException(
                    SQLState.DATA_CORRUPT_PAGE, ioe, getPageId()));
		}
	}

    
    /**
     * Set up a new slot entry.
     * <p>
     *
     * @param slot                  the slot to initialize.
     * @param recordOffset          the offset on the page to find the record.
     * @param recordPortionLength   the actual length of record+hdr on page.
     * @param reservedSpace         the reserved length associated with record.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private void setSlotEntry(
    int slot, 
    int recordOffset, 
    int recordPortionLength, 
    int reservedSpace) 
		throws IOException
	{
		rawDataOut.setPosition(getSlotOffset(slot));

		if (SanityManager.DEBUG) 
        {
			if ((recordPortionLength < 0)               || 
                (reservedSpace < 0)                     || 
                (recordPortionLength >= getPageSize())  || 
                (reservedSpace >= getPageSize())) 
            {
				SanityManager.THROWASSERT(
					"recordPortionLength and reservedSpace must " + 
                    "be > 0, and < page size."
					+ "  slot = " + slot
					+ ", in use = " + slotsInUse
					+ ", recordOffset = " + recordOffset
					+ ", recordPortionLength = " + recordPortionLength
					+ ", reservedSpace = " + reservedSpace);
			}

			if (recordOffset < (PAGE_HEADER_OFFSET + PAGE_HEADER_SIZE)) 
            {
				SanityManager.THROWASSERT(
                    "Record offset must be after the page header."
					+ "  slot = " + slot
					+ ", in use = " + slotsInUse
					+ ", recordOffset = " + recordOffset
					+ ", recordPortionLength = " + recordPortionLength
					+ ", reservedSpace = " + reservedSpace);
			}
		}

		if (slotFieldSize == SMALL_SLOT_SIZE) 
        {
			logicalDataOut.writeShort(recordOffset);
			logicalDataOut.writeShort(recordPortionLength);
			logicalDataOut.writeShort(reservedSpace);
		} 
        else 
        {
			logicalDataOut.writeInt(recordOffset);
			logicalDataOut.writeInt(recordPortionLength);
			logicalDataOut.writeInt(reservedSpace);
		}
	}

    /**
     * Insert a new slot entry into the current slot array.
     * <p>
     * Shift the existing slots from slot to (slotsInUse - 1) up by one.
     * Up here means from low slot to high slot (e.g from slot 2 to slot 3).
     * Our slot table grows backward so we have to be careful here.
     *
     * @param param1 param1 does this.
     * @param param2 param2 does this.
     *
     * @param slot                  Position the new slot will take
     * @param recordOffset          Offset of the record for the new slot
     * @param recordPortionLength   Length of the record stored in the new slot
     * @param recordPortionLength   Length of reserved space of record in slot
     *
     **/
	private void addSlotEntry(
    int slot, 
    int recordOffset, 
    int recordPortionLength, 
    int reservedSpace)
		throws IOException
	{
		if (SanityManager.DEBUG) 
        {
			if ((slot < 0) || (slot > slotsInUse))
				SanityManager.THROWASSERT("invalid slot " + slot);
			
			if ((recordPortionLength < 0) || (reservedSpace < 0))
				SanityManager.THROWASSERT(
					"recordPortionLength and reservedSpace must be > 0." +
                    "recordPortionLength = " + recordPortionLength + 
                    " reservedSpace = " + reservedSpace);

			if (recordOffset < (PAGE_HEADER_OFFSET + PAGE_HEADER_SIZE)) 
            {
                SanityManager.THROWASSERT(
                    "Record offset must be after the page header."
						+ "  slot = " + slot
						+ ", in use = " + slotsInUse
						+ ", recordOffset = " + recordOffset
						+ ", recordPortionLength = " + recordPortionLength
						+ ", reservedSpace = " + reservedSpace);
			}
		}

		int newSlotOffset;

        // TODO - (mikem) - I think the math below could be slightly optimized.

		if (slot < slotsInUse) 
        {
            // inserting a slot into the middle of array so shift all the 
            // slots from "slot" logically up by one

			int startOffset = 
                getSlotOffset(slotsInUse - 1);

			int length      = 
                (getSlotOffset(slot) + slotEntrySize) - startOffset;

			newSlotOffset = getSlotOffset(slotsInUse);

			System.arraycopy(
                pageData, startOffset, pageData, newSlotOffset, length);
		} 
        else 
        {
            // We are adding at end of slot table, so no moving necessary.
			newSlotOffset = getSlotOffset(slot); 
		}

		freeSpace -= slotEntrySize;

		slotsInUse++;
		headerOutOfDate = true;	// headerOutOfDate must be set after setDirty
								// because isDirty may be called unlatched

		setSlotEntry(slot, recordOffset, recordPortionLength, reservedSpace);
	}

    /**
     * Remove slot entry from slot array.
     * <p>
     * Remove a storage slot at slot. Shift the existing slots from
     * slot+1 to (slotsInUse - 1) down by one..
     * Down here means from high slot to low slot (e.g from slot 3 to slot 2)
     *
     * @param slot                  The slot to delete.
     *
     **/
	private void removeSlotEntry(int slot) 
        throws IOException 
	{
		if (SanityManager.DEBUG) 
        {
			if ((slot < 0) || (slot >= slotsInUse))
				SanityManager.THROWASSERT("invalid slot " + slot);
		}

		int oldEndOffset = getSlotOffset(slotsInUse - 1);
		int newEndOffset = getSlotOffset(slotsInUse - 2);

		if (slot != slotsInUse - 1) 
		{
            // if not removing the last slot, need to shift 

			// now shift all the slots logically down by one
			// from (slot+1 to slotsInUse-1) to (slot and slotsInUse-2)
			int length = getSlotOffset(slot) - oldEndOffset;

			System.arraycopy(
                pageData, oldEndOffset, pageData, newEndOffset, length);
		}

		// clear out the last slot
		clearSection(oldEndOffset, slotEntrySize);

		// mark the space as free after we have removed the slot 
		// no need to keep the space reserved for rollback as this is only
		// called for purge.
		freeSpace += slotEntrySize;

		slotsInUse--;

		headerOutOfDate = true;	// headerOutOfDate must be set after setDirty
								// because isDirty maybe called unlatched
	}

    /**
     * create the record header for the specific slot.
     * <p>
     * Create a new record header object, initialize it, and add it
     * to the array of cache'd record headers on this page.  Finally return
     * reference to the initialized record header.
     *
	 * @return The record header for the specific slot.
     *
     * @param slot   return record header of this slot.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public StoredRecordHeader recordHeaderOnDemand(int slot)
	{
		StoredRecordHeader recordHeader = 
            new StoredRecordHeader(pageData, getRecordOffset(slot));

		setHeaderAtSlot(slot, recordHeader);

		return recordHeader;
	}

    /**************************************************************************
     * Record based routines.
     **************************************************************************
     */

    /**
     * Is entire record on the page?
     * <p>
     *
	 * @return true if the entire record at slot is on this page, 
     *         i.e, no overflow row or long columns.
     *
     * @param slot   Check record at this slot.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public boolean entireRecordOnPage(int slot)
		 throws StandardException
	{
		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(isLatched());
		}

		StoredRecordHeader recordHeader = getHeaderAtSlot(slot);

		if (recordHeader.hasOverflow())
			return false;

		// the row chain does not overflow, we need to walk all the fields to
		// make sure they are not long columns.

		try 
        {

			int offset = getRecordOffset(slot);
		
			if (SanityManager.DEBUG) 
            {
				if (offset < (PAGE_HEADER_OFFSET + PAGE_HEADER_SIZE)) 
                {
					SanityManager.THROWASSERT(
                        "Incorrect offset.  offset = " + offset + 
                        ", offset should be < " +
                        "(PAGE_HEADER_OFFSET + PAGE_HEADER_SIZE) = " + 
                             (PAGE_HEADER_OFFSET + PAGE_HEADER_SIZE) + 
                        ", current slot = " + slot + 
                        ", total slotsInUse = " + slotsInUse);
				}

				SanityManager.ASSERT(recordHeader.getFirstField() == 0,
                     "Head row piece should start at field 0 but is not");
			}

			int numberFields = recordHeader.getNumberFields();

			// these reads are always against the page array
			ArrayInputStream lrdi = rawDataIn;

            // position after the record header, at 1st column.
			lrdi.setPosition(offset + recordHeader.size());
		
			for (int i = 0; i < numberFields; i++) 
            {
				int fieldStatus = StoredFieldHeader.readStatus(lrdi);
				if (StoredFieldHeader.isOverflow(fieldStatus))
					return false;

				int fieldLength = 
                    StoredFieldHeader.readFieldDataLength(
                        lrdi, fieldStatus, slotFieldSize);

				if (fieldLength != 0)
					lrdi.setPosition(lrdi.getPosition() + fieldLength);
			}
		} 
        catch (IOException ioe) 
        {
			throw dataFactory.markCorrupt(
                StandardException.newException(
                    SQLState.DATA_CORRUPT_PAGE, ioe, getPageId()));
		}

		// we have examined all the fields on this page and none overflows
		return true;
	}

    /**
     * Purge one row on an overflow page.  
     * <p>
     * HeadRowHandle is the recordHandle pointing to the head row piece.
     * <p>
     *
     * @param slot              slot number of row to purge.
     * @param headRowHandle     recordHandle of the head row piece.
	 * @param needDataLogged    when true data is logged for purges otherwise just headers.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected void purgeOverflowAtSlot(
    int             slot, 
    RecordHandle    headRowHandle,
	boolean         needDataLogged)
		 throws StandardException
	{
		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(isLatched());
			SanityManager.ASSERT(isOverflowPage());
		}

		if ((slot < 0) || (slot >= slotsInUse))
        {
			throw StandardException.newException(
                    SQLState.DATA_SLOT_NOT_ON_PAGE);
        }

        // TODO (mikem) - should a global scratch variable be used?

		// this is an overflow page purge, no need to lock the head row (it
		// has already been locked, hopefully).  No need to check for long rows
		// (they have already been deleted, hopefully).
		RawTransaction  t           = owner.getTransaction();
		int[]           recordId    = new int[1];

		recordId[0]                 = getHeaderAtSlot(slot).getId();

		owner.getActionSet().actionPurge(t, this, slot, 1, recordId, needDataLogged);
	}

    /**
     * Purge the column chain that starts at overflowPageId, overflowRecordId
     * <p>
     * Purge just the column chain that starts at the input address.
     * The long column chain is pointed at by a field in a row.  The long
     * column is then chained as a sequence of "rows", the last column then
     * points to the next segment of the chain on each page.
     * Long columns chains currently are only one row per page so the next
     * slot of a row in a long row chain should always be the first slot.
     * <p>
     *
     * @param overflowPageId    The page where the long column chain starts.
     * @param overflowRecordId  The record id where long column chain starts.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private void purgeOneColumnChain(
    long    overflowPageId, 
    int     overflowRecordId)
		 throws StandardException
	{
		StoredPage pageOnColumnChain = null;
		boolean removePageHappened = false;

		try
		{
			while (overflowPageId != ContainerHandle.INVALID_PAGE_NUMBER) 
            {

				// Now loop over the column chain and get all the column pieces.
				pageOnColumnChain   = getOverflowPage(overflowPageId);
				removePageHappened  = false;

				if (pageOnColumnChain == null) 
                {
					if (SanityManager.DEBUG)
						SanityManager.THROWASSERT(
							  "got null page following long column chain.  " +
								"Head column piece at " + getIdentity() + 
								" null page at " + overflowPageId);

					break;	// Don't know what to do here, the column chain
							// is broken.  Don't bomb, go to the next field.
				}
					
				int overflowSlotId = FIRST_SLOT_NUMBER;
				if (SanityManager.DEBUG) 
                {
					int checkSlot = 
                        pageOnColumnChain.findRecordById(
                                overflowRecordId, FIRST_SLOT_NUMBER);

                    if (overflowSlotId != checkSlot)
                    {
                        SanityManager.THROWASSERT(
                            "Long column is not at the expected " +
						    FIRST_SLOT_NUMBER + " slot, instead at slot " + 
						    checkSlot);
                    }

					SanityManager.ASSERT(pageOnColumnChain.recordCount() == 1,
						 "long column page has > 1 record");
				}

				// Hold on to the pointer to next page on the chain before
				// we remove the long column page.
				RecordHandle nextColumnPiece =
					pageOnColumnChain.getNextColumnPiece(overflowSlotId); 

				if (pageOnColumnChain.recordCount() == 1)
				{
					removePageHappened = true;
					owner.removePage(pageOnColumnChain);
				}
				else
				{
					if (SanityManager.DEBUG)
						SanityManager.THROWASSERT(
						  "page on column chain has more then one record" +
						  pageOnColumnChain.toString()); 

					pageOnColumnChain.unlatch();
					pageOnColumnChain = null;
				}

				// Chase the column chain pointer.
				if (nextColumnPiece != null) 
                {
					overflowPageId      = nextColumnPiece.getPageNumber();
					overflowRecordId    = nextColumnPiece.getId();
				} 
                else
                {
					// terminate the loop
					overflowPageId      = ContainerHandle.INVALID_PAGE_NUMBER;
                }
			}
		} 
        finally 
        {
			// if we raised an exception before the page is removed, make sure
			// we unlatch the page 
            
			if (!removePageHappened && pageOnColumnChain != null) 
            {
				pageOnColumnChain.unlatch();
				pageOnColumnChain = null;
			}
		}
	}

    /**
     * purge long columns chains which eminate from this page.
     * <p>
     * Purge all the long column chains emanating from the record on this slot
     * of this page.  The headRowHandle is the record handle of the head row
     * piece of this row - if this page is the head row, then headRowHandle is
     * the record handle at the slot.  Otherwise, headRowHandle points to a
     * row on a different page, i.e., the head page.
     * <p>
     *
     * @param t             The raw transaction doing the purging.
     * @param slot          The slot of the row to purge.
     * @param headRowHandle The RecordHandle of the head row.
     *
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private void purgeColumnChains(
    RawTransaction  t, 
    int             slot, 
    RecordHandle    headRowHandle)
		 throws StandardException
	{
		try
		{
			StoredRecordHeader recordHeader = getHeaderAtSlot(slot); 

			int numberFields    = recordHeader.getNumberFields();

			// these reads are always against the page array
			ArrayInputStream lrdi = rawDataIn;

            // position the stream to just after record header.
			int offset          = getRecordOffset(slot) + recordHeader.size();
			lrdi.setPosition(offset);

			for (int i = 0; i < numberFields; i++) 
            {
				int fieldStatus = StoredFieldHeader.readStatus(lrdi);
				int fieldLength = 
                    StoredFieldHeader.readFieldDataLength(
                        lrdi, fieldStatus, slotFieldSize);

				if (!StoredFieldHeader.isOverflow(fieldStatus)) 
                {
					// skip this field, it is not an long column
					if (fieldLength != 0)
                        lrdi.setPosition(lrdi.getPosition() + fieldLength);
					continue;
				}
                else
                {

                    // Got an overflow field.  The column value is the 
                    // <pageId, recordId> pair where the next column piece is 
                    // residing 

                    long overflowPageId = 
                        CompressedNumber.readLong((InputStream)lrdi);
                    int  overflowRecordId = 
                        CompressedNumber.readInt((InputStream)lrdi);

                    purgeOneColumnChain(overflowPageId, overflowRecordId);
                }
			}
		} 
        catch (IOException ioe) 
        {
			throw dataFactory.markCorrupt(
                StandardException.newException(
                    SQLState.DATA_CORRUPT_PAGE, ioe, getPageId()));
		}
	}

    /**
     * Purge all the overflow columns and overflow rows of the record at slot.
     * <p>
     * Purge all the overflow columns and overflow rows of the record at slot.
     * This is called by BasePage.purgeAtSlot, the head row piece is purged
     * there. 
     * <p>
     *
     * @param t             The raw transaction doing the purging.
     * @param slot          The slot of the row to purge.
     * @param headRowHandle The RecordHandle of the head row.
	 * @param needDataLogged    when true data is logged for purges otherwise just headers.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected void purgeRowPieces(
    RawTransaction  t, 
    int             slot, 
    RecordHandle    headRowHandle,
	boolean         needDataLogged) 
		 throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isOverflowPage() == false,
				 "not expected to call purgeRowPieces on a overflow page");

		// purge the long columns which start on this page.
		purgeColumnChains(t, slot, headRowHandle);

		// drive this loop from the head page. Walk each "long" row piece in 
        // the row chain.
		StoredRecordHeader recordHeader = getHeaderAtSlot(slot);

		while (recordHeader.hasOverflow()) 
        {

			// nextPageInRowChain, is the page with the next row piece
			StoredPage nextPageInRowChain = 
                getOverflowPage(recordHeader.getOverflowPage());

			if (nextPageInRowChain == null) 
            {
				if (SanityManager.DEBUG)
                {
					SanityManager.THROWASSERT(
							"got null page following long row chain.  " +
							"Head row piece at " + getIdentity() + " slot " +
							slot + " headRecord " + headRowHandle + 
							".  Broken row chain at " +
							recordHeader.getOverflowPage() + ", " +
							recordHeader.getOverflowId());

                }

                break;	// Don't know what to do here, the row chain is
						// broken.  Don't bomb, just return.
			}

			try 
            {

				int nextPageSlot = 
                    getOverflowSlot(nextPageInRowChain, recordHeader);

				// First get rid of all long columns from the next row piece.
				nextPageInRowChain.purgeColumnChains(
                    t, nextPageSlot, headRowHandle);

				// Before we purge the next row piece, get the row header to
				// see if we need to continue the loop.
				recordHeader = nextPageInRowChain.getHeaderAtSlot(nextPageSlot);

				// Lastly, purge the next row piece.  If the next row piece is
				// the only thing in the entire page, just deallocate the page.
				// We can do this because the page is deallocated in this
				// transaction.  If we defer this to post commit processing,
				// then we have to first purge the row piece and also remember
				// the page time stamp.

				if (nextPageSlot == 0 && nextPageInRowChain.recordCount() == 1)
				{
					// This is an overflow page and we just purged the last row.
					// Free the page.  Cannot do it in post commit because the
					// head row is gone and cannot be locked at post commit to
					// stablelize the row chain.

					try 
                    {
						owner.removePage(nextPageInRowChain);
					}
					finally 
                    {
						// Remove Page guarantees to unlatch the page even
						// if an exception is thrown, need not unlatch it
						// again. 
						nextPageInRowChain = null;
					}
				}
				else
				{
					nextPageInRowChain.purgeOverflowAtSlot(
                        nextPageSlot, headRowHandle, needDataLogged);

					nextPageInRowChain.unlatch();
					nextPageInRowChain = null;
				}
			} 
            finally 
            {
				// Unlatch the next row piece before getting the next page in
				// the row chain.
				if (nextPageInRowChain != null) 
                {
					nextPageInRowChain.unlatch();
					nextPageInRowChain = null;
				}
			}
		}
	}


    /**
     * Remove a column chain that may have been orphaned by an update.  
     * <p>
     * Remove a column chain that may have been orphaned by an update.  This
     * is executed as a post commit operation.  This page is the head page of
     * the row which used to point to the column chain in question.  The
     * location of the orphaned column chain is in the ReclaimSpace record.
     * <BR>
     * MT - latched.  No lock will be gotten, the head record must already be
     * locked exclusive with no outstanding changes that can be rolled back.
     * <p>
     *
     * @param work          object describing the chain to remove.
     * @param containerHdl  open container handle to use to remove chain.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	/* package */ 
	void removeOrphanedColumnChain(
    ReclaimSpace    work, 
    ContainerHandle containerHdl)
		 throws StandardException
	{
		// First we need to make sure that this is the first and only time
		// this long column is begin reclaimed, to do this we get the first
		// page on the long column chain and compare its page time stamp.
		// If it is different, don't do anything.
		//
		// Next we need to make sure the update operation commits - we do
		// this by finding the row headed by headRecord, go to the column
		// in question and see if it points to the first page of the long
		// column chain we want to reclaim.  If it does then the update
		// operation has rolled back and we don't want to reclaim it.
		//
		// After we do the above 2 checks, we can reclaim the column
		// chain.
		StoredPage headOfChain =
			(StoredPage)containerHdl.getPageNoWait(work.getColumnPageId());

		// If someone has it latched, not reclaimable
		if (headOfChain == null) 
			return;

		// If the column has been touched, it is not orphaned.  Not reclaimable.
		boolean pageUnchanged = 
            headOfChain.equalTimeStamp(work.getPageTimeStamp());

		headOfChain.unlatch();	// unlatch it for now.

		if (pageUnchanged == false)
			return;

		// Now get to the column in question and make sure it is no longer
		// pointing to the column chain.

		RecordHandle headRowHandle = work.getHeadRowHandle();

		if (SanityManager.DEBUG) 
        {
            // System.out.println("Executing in removeOrphanedColumnChain.");
            // System.out.println("work =  " + work);
            // System.out.println("head = " + headOfChain);
            // System.out.println("this = " + this);

			SanityManager.ASSERT(isLatched());
			SanityManager.ASSERT(
                headRowHandle.getPageNumber() == getPageNumber(), 
                "got wrong head page");
		}	

		// First get the row.
		int slot = 
            findRecordById(
                headRowHandle.getId(), headRowHandle.getSlotNumberHint());

		// If slot < 0, it means the whole record is gone, the column chain is
		// definitely orphaned.

		if (slot >= 0) 
        {
            if (SanityManager.DEBUG) 
            {
                if (isOverflowPage())
                {
                    SanityManager.THROWASSERT(
                        "Page " + getPageNumber() + " is overflow " +
                        "\nwork = " + work +
                        "\nhead = " + headOfChain +
                        "\nthis = " + this);
                }
            }	

			// Find the page with the column in question on it.
			StoredPage pageInRowChain = this; // Start with the head page.

			try 
            {

				int columnId = work.getColumnId();
				StoredRecordHeader recordHeader = getHeaderAtSlot(slot); 

				if (SanityManager.DEBUG)
					SanityManager.ASSERT(recordHeader.getFirstField() == 0,
						"Head row piece should start at field 0 but is not");

				// See if columnId is on pageInRowChain.
				while ((recordHeader.getNumberFields() +
						recordHeader.getFirstField()) <= columnId) 
                {
					// The column in question is not on pageInRowChain.

					if (pageInRowChain != this) 
                    {
						// Keep the head page latched.
						pageInRowChain.unlatch();
						pageInRowChain = null;
					}

					if (recordHeader.hasOverflow())	
                    {
						// Go to the next row piece
						pageInRowChain = 
                            getOverflowPage(recordHeader.getOverflowPage());
						recordHeader = 
                            pageInRowChain.getHeaderAtSlot(
                                getOverflowSlot(pageInRowChain, recordHeader));
					} 
                    else 
                    {
						//  Don't know why, but this is the last column.
						//  Anyway, the column chain is definite orphaned.
						//  This can happen if the update, or subsequent
						//  updates, shrink the number of columns in the row. 
						break;
					}
				}

				if ((recordHeader.getNumberFields() + 
                            recordHeader.getFirstField()) > columnId) 
                {
					// RecordHeader is the record header of the row piece on
					// pageInRowChain.  The column in question exists and is in
					// that row piece.
					if (!pageInRowChain.isColumnOrphaned(
                            recordHeader, columnId, 
                            work.getColumnPageId(), work.getColumnRecordId()))
					{
						// The column is not orphaned, row still points to it.
						if (pageInRowChain != this) 
                        {
							// Keep the head page latched.
							pageInRowChain.unlatch();
							pageInRowChain = null;
						}
						return;
					}
				}

			} 
            catch (IOException ioe) 
            {
				throw StandardException.newException(
                        SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
			} 
            finally 
            {
				if (pageInRowChain != this && pageInRowChain != null)
					pageInRowChain.unlatch();
			}
		}

		// If we get this far, we have verified that the column chain is indeed
		// orphaned. Get rid of the column chain.

		long nextPageId     = work.getColumnPageId();
		int  nextRecordId   = work.getColumnRecordId();

		purgeOneColumnChain(nextPageId, nextRecordId);
	}

    /**
     * See if there is a orphaned long colum chain or not.  
     * <p>
     * See if there is a orphaned long colum chain or not.  This is a helper
     * function for removeOrphanedChain.  This page, which may be a head page
     * or overflow page, contains the column specified in columnId.  It used to
     * point to a long column chain at oldPageId and oldRecordId.  Returns true
     * if it no longer points to that long column chain.
     * <p>
     *
	 * @return true if page no longer points to the long column chain.
     *
     * @param recordHeader  record header which used to point at the long column
     * @param columnId      column id of the long column in head.
     * @param oldPageId     the page id where the long column used to be.
     * @param oldRecordId   the record id where the long column used to be.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private boolean isColumnOrphaned(
    StoredRecordHeader  recordHeader, 
    int                 columnId,
    long                oldPageId, 
    long                oldRecordId)
		 throws StandardException, IOException
	{
		int slot = findRecordById(recordHeader.getId(), Page.FIRST_SLOT_NUMBER);

		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(slot >= 0, "overflow row chain truncated");

			SanityManager.ASSERT(
                columnId >= recordHeader.getFirstField(),
                "first column on page > expected");
		}

		// these reads are always against the page array
		ArrayInputStream lrdi = rawDataIn;

        // set read position to data portion of record to check.
		int offset = getRecordOffset(slot);
		lrdi.setPosition(offset + recordHeader.size());

        // skip until you get to the record in question.
		for (int i = recordHeader.getFirstField(); i < columnId; i++)
			skipField(lrdi);

        // read in the info of the column we are interested in.
		int fieldStatus = StoredFieldHeader.readStatus(lrdi);
		int fieldLength = StoredFieldHeader.readFieldDataLength
				(lrdi, fieldStatus, slotFieldSize);

		if (StoredFieldHeader.isOverflow(fieldStatus)) 
        {
            // it is still an overflow field, check if it still points to 
            // overflow column in question.

			long ovflowPage = CompressedNumber.readLong((InputStream) lrdi);
			int  ovflowRid  = CompressedNumber.readInt((InputStream) lrdi);

			if (ovflowPage == oldPageId && ovflowRid == oldRecordId) 
            { 
				// This field still points to the column chain, the
				// update must have rolled back.
				return false;
			}
		}

		// Else, either the field is no longer a long column, or it doesn't
		// point to oldPageId, oldRecordId.  The column chain is orphaned. 
		return true;
	}

	/**
	    @return a recordHandle pointing to the next piece of the column chain.
		This page must be an overflow page that is in a column chain.  If this
		is the last piece of the overflow colum, return null.

		@param slot the slot number where the current piece of overflow column
		is at.
		@exception StandardException Cloudscape Standard Error Policy
	 */
    /**
     * Return the next recordHandle in a long column chain.
     * <p>
     * Return a recordHandle pointing to the next piece of the column chain.
     * This page must be an overflow page that is in a column chain.  If this
     * is the last piece of the overflow colum, return null.
     * <p>
     *
	 * @return The next record handle in a long column chain.
     *
     * @param slot   The slot of the current long column piece.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private RecordHandle getNextColumnPiece(int slot) 
        throws StandardException
	{
		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(isLatched());
			SanityManager.ASSERT(isOverflowPage(), 
				"not expected to call getNextColumnPiece on non-overflow page");

            if (recordCount() != 1)
            {
                SanityManager.THROWASSERT(
                    "getNextColumnPiece called on a page with " +
                    recordCount() + " rows");
            }
		}

		try 
        {
			StoredRecordHeader recordHeader = getHeaderAtSlot(slot);
			int numberFields = 
                recordHeader.getNumberFields();

			if (SanityManager.DEBUG) 
            {
				if ((numberFields > 2) || (numberFields < 1))
                {
					SanityManager.THROWASSERT(
						"longColumn record header must have 1 or 2 fields." +
                        " numberFields = " + numberFields);
                }
			}

			if (numberFields != 2) // End of column chain.
				return null;

			// these reads are always against the page array
			ArrayInputStream lrdi = rawDataIn;

			// The 2nd field is the pointer to the next page in column chain.

			int offset = getRecordOffset(slot) + recordHeader.size();
			lrdi.setPosition(offset);

			// skip the first field
			skipField(lrdi);

			// the 2nd field should be <pageId, recordId> pair, return the
			// pageId part and skip over the length.
			int fieldStatus = StoredFieldHeader.readStatus(lrdi);
			int fieldLength = StoredFieldHeader.readFieldDataLength
				(lrdi, fieldStatus, slotFieldSize);

			long ovflowPage = CompressedNumber.readLong((InputStream) lrdi);
			int  ovflowRid  = CompressedNumber.readInt((InputStream) lrdi);

			if (SanityManager.DEBUG) 
            {
				if (!StoredFieldHeader.isOverflow(fieldStatus)) 
                {
					// In version 1.5, the first field is overflow and the
					// second is not. In version 2.0 onwards, the first field
					// is not overflow and the second is overflow (the overflow
					// bit goes with the overflow pointer).  Check first field
					// to make sure its overflow bit is set on.  
					// Offset still points to the first column.
					lrdi.setPosition(offset);
					fieldStatus = StoredFieldHeader.readStatus(lrdi);
					SanityManager.ASSERT(
                            StoredFieldHeader.isOverflow(fieldStatus));
				}
			}

			// RESOLVE : this new can get expensive if the column chain is very
			// long.  The reason we do this is because we need to return the
			// page number and the rid, if we assume that the long column is
			// always at slot 0, we can return only the page.

			return owner.makeRecordHandle(ovflowPage, ovflowRid);

		} 
        catch (IOException ioe) 
        {
			throw dataFactory.markCorrupt(
                StandardException.newException(
                    SQLState.DATA_CORRUPT_PAGE, ioe, getPageId()));
		}
	}
	 

    /**************************************************************************
     * Page space usage
     **************************************************************************
     */

    /**
     * initialize the in memory variables associated with space maintenance.
     * <p>
     * Get the total available space on an empty page.
     * initSlotTable() must be called after the page has been read in.
     **/
	private void initSpace()
	{
		// NOTE: subclass may have overwitten it to report less freeSpace,
		// always call getMaxFreeSpace() to get total space.
		totalSpace = getMaxFreeSpace();

		// estimate RH will be about 16 bytes:
		// (1 - status, 1 - id, 1 - #fields, 1 - 1stField, 12 - overflow ptr)

        // RESOLVED: track# 3370, 3368
        // In the old code below, spareSpace/100 is integer division. This means
        // that you get a value of 0 for it as long as spareSpace is between 0
        // and 99. But if spareSpace is 100 you get a value of 1. This resulted
        // in a negative value for maxFieldSize. This caused e.g. the isLong 
        // method to behave incorrectly when spareSpace is 100.
        //
        // RESOLVED: track# 4385
        // maxFieldSize is a worst case calculation for the size of a record
        // on an empty page, with a single field, but still allow room for 
        // an overflow pointer if another field is to be added.  If you don't
        // account for the overflow pointer then you can get into the situation
        // where the code puts the field on the page (not making it a long 
        // column), then runs out of space on next column but can't fit overflow
        // pointer, so backs up and removes the column from page, and tries
        // again on next overflow page - looping forever.
        //
		// maxFieldSize = 
        //     totalSpace * (1 - spareSpace/100) - slotEntrySize 
        //     - 16 - OVERFLOW_POINTER_SIZE;

		maxFieldSize = totalSpace - slotEntrySize - 16 - OVERFLOW_POINTER_SIZE;

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(maxFieldSize >= 0);
	}

    /**
     * Initialize the freeSpace count and set the firstFreeByte on page
     **/
	private void clearAllSpace()
	{
		freeSpace     = totalSpace;
		firstFreeByte = getPageSize() - totalSpace - CHECKSUM_SIZE;
	}

    /**
     * Compress out the space specified by startByte and endByte.
     * <p>
     * As part of moving rows, updating rows, purging rows compact the space
     * left between rows.
     * <p>
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
     * @param startByte compress out space starting at startByte offset
     * @param endByte   compress out space ending   at endByte   offset
     *
     **/
	private void compressPage(
    int startByte, 
    int endByte) 
        throws IOException
	{
		if (SanityManager.DEBUG) 
        {
			if (((endByte + 1) > firstFreeByte) || (startByte > firstFreeByte))
            {
				SanityManager.THROWASSERT(
					"startByte = " + startByte + " endByte = " + endByte +
					" firstFreeByte = " + firstFreeByte);
            }
		}

		int lengthToClear = endByte + 1 - startByte;

		// see if these were not the last occupied record space on the page
		if ((endByte + 1) != firstFreeByte) 
        {
			// Shift everything down the page.
			int moveLength = (firstFreeByte - endByte - 1);

			System.arraycopy(
                pageData, (endByte + 1), pageData, startByte, moveLength);

			// fix the page offsets of the rows further down the page
			for (int slot = 0; slot < slotsInUse; slot++) 
            {
				int offset = getRecordOffset(slot);

				if (offset >= (endByte + 1)) 
                {
					offset -= lengthToClear;
					setRecordOffset(slot, offset);
				}
			}
		}
		
		freeSpace     += lengthToClear;
		firstFreeByte -= lengthToClear;

		clearSection(firstFreeByte, lengthToClear);
	}

    /**
     * Free up required bytes by shifting rows "down" the page.
     * <p>
     * Expand page, move all the data from start Offset down the page by
     * the amount required to free up the required bytes.
     *
     * @param startOffset   offset on page to begin the shift
     * @param requiredBytes the number of bytes that must be freed.
     *
     * @exception IOException	If IOException is raised during the page mod.
     **/
	protected void expandPage(
    int startOffset, 
    int requiredBytes) 
        throws IOException
	{
		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(requiredBytes <= freeSpace);
			SanityManager.ASSERT(startOffset <= firstFreeByte);
		}

		int totalLength = firstFreeByte - startOffset;

		if (totalLength > 0)
		{
			System.arraycopy(
                pageData, startOffset, 
                pageData, startOffset + requiredBytes, totalLength);

			// fix the page offsets of the rows further down the page
			for (int slot = 0; slot < slotsInUse; slot++) 
            {
				int offset = getRecordOffset(slot);
				if (offset >= startOffset) 
                {
					offset += requiredBytes;
					setRecordOffset(slot, offset);
				}
			}
		}

		freeSpace     -= requiredBytes;
		firstFreeByte += requiredBytes;
	}

    /**
     * Shrink page. 
     * <p>
     * move all the data from start Offset up the page by the amount shrunk. 
     *
     *
     * @param startOffset   offset on page to begin the shift
     * @param shrinkBytes   the number of bytes that must be moved.
     *
     * @exception IOException	some IOException is raised during the page mod,
     *                          (unlikely as this is just writing to array).
     **/
	private void shrinkPage(int startOffset, int shrinkBytes) 
		 throws IOException 
	{
		// the number of bytes that needs to be moved up.
		int totalLength = firstFreeByte - startOffset;

		if (SanityManager.DEBUG) 
        {
			SanityManager.DEBUG(
                "shrinkPage", "page " + getIdentity() + 
                " shrinking " + shrinkBytes + 
                " from offset " + startOffset +
                " to offset " + (startOffset-shrinkBytes) +
                " moving " + totalLength + 
                " bytes.  FirstFreeByte at " + firstFreeByte);

			SanityManager.ASSERT(
                totalLength >= 0, "firstFreeByte - startOffset <= 0");

			SanityManager.ASSERT(
                (startOffset-shrinkBytes) > RECORD_SPACE_OFFSET ,
                "shrinking too much ");

			if (startOffset != firstFreeByte)
			{
				// make sure startOffset is at the beginning of a record
				boolean foundslot = false;
				for (int slot = 0; slot < slotsInUse; slot++) 
                {
					if (getRecordOffset(slot) == startOffset) 
                    {
						foundslot = true;
						break;
					}
				}

                if (!foundslot)
                {
                    SanityManager.THROWASSERT(
                        "startOffset " + startOffset + 
                        " not at the beginning of a record");
                }
			}
		}

		if (totalLength > 0) 
        {
			System.arraycopy(
                pageData, startOffset,
                pageData, startOffset-shrinkBytes , totalLength);

			// fix the page offsets of the rows further down the page
			for (int slot = 0; slot < slotsInUse; slot++) 
            {
				int offset = getRecordOffset(slot);
				if (offset >= startOffset) 
                {
					offset -= shrinkBytes;
					setRecordOffset(slot, offset);
				}
			}
		}

		freeSpace     += shrinkBytes;
		firstFreeByte -= shrinkBytes;
	}

	public int getRecordLength(int slot) throws IOException
	{
		return getRecordPortionLength(slot);
	}
	protected  boolean getIsOverflow(int slot) throws IOException
	{
		return getHeaderAtSlot(slot).hasOverflow();
	}

	/**
		Log a row into the StoreOuput stream.

		<P>

		@exception StandardException	Standard Cloudscape error policy
		@exception IOException			RESOLVE

	*/
    /**
     * Log a row into the StoreOuput stream.
     * <p>
     * Write the row in its record format to the stream. Record format is a 
     * record header followed by each field with its field header. See this 
     * class's description for the specifics of these headers.
     *
     * startColumn is used to specified which column for this logRow to 
     * start logging.  When realStartColumn is specified, that means part of 
     * the row has already been logged.  startColumn here indicates that the 
     * first column was logged in the logBuffer, need to continue log the rest
     * of the row starting at realStartColumn.
     *
     * This is used when a longColumn is encountered during a long row.
     * After done logging the long column, we need to continue logging the 
     * rest of the row.
     * A -1 value for realStartColumn, means that it is not significant.
     *
     * logRow will not throw an noSpaceOnPage exception, if it is an overflow 
     * page, and the record we are inserting is the only record on the page.
     * We are supporting rows expanding multiple pages through this mechanism.
     *
     * logRow expects row to be a sparse row.
     * <p>
     *
	 * @return the "realStartColumn" value, -1 if not a long row.
     *
     * @param slot              the slot of the row being logged.
     * @param forInsert         this is logging an insert (not update/delete).
     * @param recordId          record id of the row being logged.
     * @param row               actual data of row in object form.  If row is
     *                          null then we are logging an overflow pointer.
     * @param validColumns      bit map describing valid columns in row.
     * @param out               stream to log to.
     * @param startColumn       what column to start with (see above for detail)
     * @param insertFlag        flag indicating mode we are in, 
     *                              INSERT_DEFAULT - default insert
     *                              INSERT_SPLIT   - splitting a row/column 
     *                                               across pages.
     * @param realStartColumn   If -1 ignore variable, else part of row has
     *                          already been logged, and should continue with
     *                          this column.
     * @param realSpaceOnPage   Use this as space on page if realStartColumn
     *                          is not -1.
     * @param overflowThreshold How much of the page to use before deciding
     *                          to overflow a row.
     *
     * @exception  IOException		  RESOLVE
	 * @exception  StandardException  Standard exception policy.
     *
     * @see BasePage#logRow
     **/
	public int logRow(
    int                     slot, 
    boolean                 forInsert, 
    int                     recordId,
    Object[]                row, 
    FormatableBitSet                 validColumns, 
    DynamicByteArrayOutputStream  out,
    int                     startColumn, 
    byte                    insertFlag, 
    int                     realStartColumn, 
    int                     realSpaceOnPage,
    int                     overflowThreshold)
		throws StandardException, IOException
	{
		// Is this an update that just went through handleIncompleteLogRow
		// and handleIncompleteLogRow threw an excepiton. In this case the
		// buffer is already finished.
		if (!forInsert) 
        {
			if ((realStartColumn != -1) && (realSpaceOnPage == -1)) 
            {
				return realStartColumn;
			}
		}

		int spaceAvailable = freeSpace;
		setOutputStream(out);
		int beginPosition = out.getPosition();

		// if we are inserting in the headPage,
		// we need to make sure that there is enough room
		// on the page for the reserve space.
		userRowSize = 0;
		boolean calcMinimumRecordSize = false;

		if (realStartColumn != (-1)) 
        {
            // in the middle of logging a long row/column.

			spaceAvailable  = realSpaceOnPage;
			beginPosition   = out.getBeginPosition();
		} 
        else 
        {
            // logging row part that is on head page.

			if (!forInsert) 
            {
				// an update can use the total space of the record,
				// even if not all of the fields are being updated.
				// If the updated columns will not fit then some
				// columns will move off the page to a new chunk.
				spaceAvailable += getTotalSpace(slot);

			} 
            else 
            {
				// need to account for the slot table using extra space...
				spaceAvailable -= slotEntrySize;

				if (startColumn == 0)
					calcMinimumRecordSize = true;
			}

			// <= is ok here as we know we want to write at least one more byte
			if (spaceAvailable <= 0)
				throw new NoSpaceOnPage(isOverflowPage());
		}

		try 
        {
			if (row == null) 
            {
				// if the row is null, we must be writing an overflow pointer.

				return(logOverflowRecord(slot, spaceAvailable, out));
			}    

            int                numberFields = 0;
            StoredRecordHeader recordHeader;

			if (forInsert) 
            {
				recordHeader = new StoredRecordHeader();
			} 
            else 
            {
				// Get a full copy of the record header since we might change 
                // it,  and we can't modify the one on the page
				recordHeader = 
                    new StoredRecordHeader(getHeaderAtSlot(slot));

				// an update always starts at the first column on this page
				startColumn = recordHeader.getFirstField();
			}

			if (validColumns == null)
            {
                // all columns in row[] are valid, we will be logging them all.

				numberFields = row.length - startColumn;
            }
			else 
            {
                // RESOLVE (mikem) - counting on validColumns.length may be bad
                // for performance.

				for (int i = validColumns.getLength() - 1; 
                     i >= startColumn; 
                     i--) 
                {
					if (validColumns.isSet(i)) 
                    {
						numberFields = i + 1 - startColumn;
						break;
					}
				}
			}

			int onPageNumberFields = -1; // only valid for update

			if (forInsert) 
            {
				recordHeader.setId(recordId);
				recordHeader.setNumberFields(numberFields);
			} 
            else 
            {
				// an update

				onPageNumberFields = recordHeader.getNumberFields();

				if (numberFields > onPageNumberFields) 
                {
					// number of fields *might* be increasing
					if (recordHeader.hasOverflow()) 
                    {
						// other fields will be handled in next portion update
                        
						numberFields = onPageNumberFields;
					} 
                    else 
                    {
						// number of fields is increasing

						recordHeader.setNumberFields(numberFields);
					}
				} 
                else if (numberFields < onPageNumberFields) 
                {
					if (validColumns == null) 
                    {
						// number of fields is decreasing,
						// but only allowed when the complete
						// row is being updated.
						recordHeader.setNumberFields(numberFields);

						// RESOLVE - 
                        // need some post commit work if row has overflow

						// if (recordHeader.hasOverflow()) {
						// remove overflow portion after commit.
						// }

					} 
                    else 
                    {
						// we process all the fields, the unchanged ones
						// at the end will have a single byte written out
						// indicating they are unchanged (nonexistent)
						numberFields = onPageNumberFields;
					}
				}
			}

			int endFieldExclusive = startColumn + numberFields;

			if (realStartColumn >= endFieldExclusive) 
            {
				// The realStartColumn is greater than the last column we need
                // to log, so we are done.
				return (-1);
			}

			if ((insertFlag & Page.INSERT_DEFAULT) != Page.INSERT_DEFAULT) 
            {
                // if this is not logging the part of the row being inserted
                // on the main page, then use startColumn as first field.
				recordHeader.setFirstField(startColumn);
			} 

            // what column to start with?

			int firstColumn = realStartColumn;
			if (realStartColumn == (-1)) 
            {
                // logging on the head page.

				int recordHeaderLength = recordHeader.write(logicalDataOut);

				spaceAvailable -= recordHeaderLength;
				if (spaceAvailable < 0)
                {
                    // ran out of space just writing the record header.
                    throw new NoSpaceOnPage(isOverflowPage());
                }

				firstColumn = startColumn;
			}


			boolean monitoringOldFields = false;
            int validColumnsSize = 
                (validColumns == null) ? 0 : validColumns.getLength();
            
			if (validColumns != null) 
            {
				if (!forInsert) 
                {
					// we monitor the length of the old fields by skipping them
					// but only on a partial update.
					if ((validColumns != null) && 
                        (firstColumn < (startColumn + onPageNumberFields)))
                    {
						rawDataIn.setPosition(
                            getFieldOffset(slot, firstColumn));

						monitoringOldFields = true;
					}
				}
			}

			int lastSpaceAvailable              = spaceAvailable;
			int recordSize                      = 0;
			int lastColumnPositionAllowOverflow = out.getPosition();
			int lastColumnAllowOverflow         = startColumn;

			if (spaceAvailable > OVERFLOW_POINTER_SIZE)
				lastColumnPositionAllowOverflow = -1;
			int columnFlag = COLUMN_FIRST;

			for (int i = firstColumn; i < endFieldExclusive; i++) 
            {
                Object              ref          = null;
				boolean             ignoreColumn = false;


                // should we log this column or not?
				if ((validColumns == null) || 
                    (validColumnsSize > i && validColumns.isSet(i))) 
                {
					if (i < row.length)
						ref = row[i];
				} 
                else if (!forInsert) 
                {
					// field is not supplied, log as non-existent
					ignoreColumn = true;
				}

				if (spaceAvailable > OVERFLOW_POINTER_SIZE) 
                {
					lastColumnPositionAllowOverflow = out.getPosition();
					lastColumnAllowOverflow         = i;
				}

				lastSpaceAvailable = spaceAvailable;

				if (ignoreColumn) 
                {
					if (SanityManager.DEBUG) 
                    {
						SanityManager.ASSERT(
                            ref == null, 
                            "ref should be null for an ignored column");

						SanityManager.ASSERT(
                            validColumns != null, 
                            "validColumns should be non-null for ignored col");
					}

					if (i < (startColumn + onPageNumberFields)) 
                    {
						if (SanityManager.DEBUG) 
                        {
							SanityManager.ASSERT(
                                monitoringOldFields, 
                                "monitoringOldFields must be true");
						}

						// need to keep track of the old field lengths
						// as they are remaining in the row.
						int oldOffset = rawDataIn.getPosition();
						skipField(rawDataIn);
						int oldFieldLength = 
                            rawDataIn.getPosition() - oldOffset;

						if (oldFieldLength <= spaceAvailable) 
                        {
                            //  if field doesn't fit, 
                            //      spaceAvailable must be left unchanged.

							logColumn(
                                null, 0, out, Integer.MAX_VALUE, 
                                COLUMN_NONE, overflowThreshold);

							spaceAvailable -= oldFieldLength;
						}

					} 
                    else 
                    {
						// this is an update that is increasing the number of 
                        // columns but not providing any value, strange ...

						spaceAvailable = 
                            logColumn(
                                null, 0, out, spaceAvailable, 
                                columnFlag, overflowThreshold);
					}

				} 
                else 
                {
                    // ignoreColumn is false, we are logging this column.

					if (monitoringOldFields && 
                        (i < (startColumn + onPageNumberFields))) 
                    {
						// skip the old version of the field so that
						// rawDataIn is correctly positioned.
						skipField(rawDataIn);
					}


					try 
                    {
						if (ref == null)
                        {
                            // no new value to provide, use the on page value.
							spaceAvailable = 
                                logColumn(
                                    null, 0, out, spaceAvailable, 
                                    columnFlag, overflowThreshold);
                        }
						else
                        {
                            // log the value provided in the row[i]
							spaceAvailable = 
                                logColumn(
                                    row, i, out, spaceAvailable, 
                                    columnFlag, overflowThreshold);
                        }

					} 
                    catch (LongColumnException lce) 
                    {
                        // logColumn determined that the column would not fit
                        // and that the column length exceeded the long column
                        // threshold so turn this column into a long column.
                        

						if ((insertFlag & Page.INSERT_DEFAULT) == 
                                Page.INSERT_DEFAULT) 
                        {
                            // if default insert, just throw no space exception.

							// if the lce has throw the column as an InputStream,
							// in the following 2 situations
							//    1. If column came in 'row[i]' as InputStream
							//	  2. If the object stream of 'row[i]' is not 
                            //	     null, which means that the object state of
                            //	     the column is null.
                            //
							// we need to set the original InputStream column to
                            // the column that has been thrown by lce.  It is a
                            // store formated InputStream which remembers all 
                            // the bytes that has been read, but not yet stored.
                            // Therefore, we will not lose any bytes.
							//
							// In any other situation, we should not change the
                            // state of the column,
							// i.e. if 'row[i]' has an object state, it should
                            // not be turned into an InputStream.

							if ((lce.getColumn() instanceof InputStream)
									&& (row[i] instanceof StreamStorable) ) 
                            {
								if ((row[i] instanceof InputStream) || 
                                    (((StreamStorable) row[i]).returnStream() 
                                         != null) ) 
                                {
                                    // change state of stream so that it uses
                                    // the stream just created by the lce - 
                                    // which is remembering the bytes it has
                                    // already read from the stream but couldn't
                                    // log as there was not enough room on 
                                    // current page.

									((StreamStorable) row[i]).setStream(
                                                (InputStream) lce.getColumn());
								}
							}

							throw new NoSpaceOnPage(isOverflowPage());
						}

						// When one of the following two conditions is true,
						// we will allow the insert of the long column:
                        //
						// 1.	if this is the last field,
                        //      and overflow field header fits on page.
						// 2.	if it is not the last field,
                        //      and overflow field header fits on page (for col)
                        //      and another overflow ptr fits (for row).
                        //      
                        // 

						if (((spaceAvailable >= OVERFLOW_PTR_FIELD_SIZE) && 
                             (i == (endFieldExclusive - 1))) || 
                            ((spaceAvailable >= (OVERFLOW_PTR_FIELD_SIZE * 2))&&
                             (i < (endFieldExclusive - 1)))) 
                        {
							// If the column is a long column, it must be a 
                            // InputStream.  We have made the input stream into
                            // a RememberBytesInputStream, have to set the 
                            // column to that, in order to preserve the bytes
							// we already read off the stream.

							// caught a long column exception, 
                            // set the variables, and rethrow the error
							out.setBeginPosition(beginPosition);
							lce.setExceptionInfo(out, i, spaceAvailable);
							throw (lce);
						}
					}
				}

				int nextColumn;

				recordSize += (lastSpaceAvailable - spaceAvailable);
				boolean recordIsLong = 
                    (overflowThreshold == 100) ? 
                        false : isLong(recordSize, overflowThreshold);

				// get the no overflow case out of the way asap
				if ((lastSpaceAvailable == spaceAvailable) || recordIsLong) 
                {
					if ((insertFlag & Page.INSERT_DEFAULT) == 
                            Page.INSERT_DEFAULT) 
                    {
						throw new NoSpaceOnPage(isOverflowPage());
					}

					if (recordIsLong) 
                    {
                        // if the record is long because of threshold, 
                        // then, we need to reset the logicalOut.
                        // set position to the end of the previous field

						out.setPosition(out.getPosition() - recordSize);
					}

					// did not write this column
					nextColumn = i;
				} 
                else 
                {
					// assume that all fields will be written to this page.
					nextColumn = endFieldExclusive;
				}

				// See if we have enough room to write an overflow field if the
                // row needs to overflow.  We need overflow if we need to 
                // write another portion or another portion already exists and 
                // we will need to point to it.

				if ((lastSpaceAvailable == spaceAvailable) ||
					((insertFlag & Page.INSERT_FOR_SPLIT) == 
                         Page.INSERT_FOR_SPLIT)) 
                {
					// The current row has filled the page.

					if (spaceAvailable <= OVERFLOW_POINTER_SIZE) 
                    {
						if ((i == startColumn) || 
                            (lastColumnPositionAllowOverflow < 0))  
                        {
							// not enough room for the overflow recordheader,
                            // and this is the first column on this page so 
                            // need to try another page.
							throw new NoSpaceOnPage(isOverflowPage());
						} 
                        else 
                        {
							// we need to go back to the last column
							// that left enough room for an overflow pointer.

							out.setPosition(lastColumnPositionAllowOverflow);
							nextColumn = lastColumnAllowOverflow;
						}
					}
				}

				if (nextColumn < endFieldExclusive) 
                {
                    // If the number of cols has been reduced.

					int actualNumberFields = nextColumn - startColumn;

					// go back and update that numberFields in recordHeader.
					// no need to update spaceAvailable here, because if we are
                    // here, we will be returning any way, and spaceAvailable 
                    // will be thrown away.

					int oldSize = recordHeader.size();
					recordHeader.setNumberFields(actualNumberFields);

					int newSize = recordHeader.size();
					
					// now we are ready to write the new record header.
					int endPosition = out.getPosition();

					if (oldSize > newSize) 
                    {
						// if the old size is bigger than the new size, then 
                        // leave extra bytes at the beginning of byte stream.

						int delta = oldSize - newSize;
						out.setBeginPosition(beginPosition + delta);
						out.setPosition(beginPosition + delta);
					} 
                    else if (newSize > oldSize) 
                    {
						out.setPosition(beginPosition);

					} 
                    else 
                    {
						out.setBeginPosition(beginPosition);
						out.setPosition(beginPosition);
					}

					int realLen = recordHeader.write(logicalDataOut);
					if (SanityManager.DEBUG) 
                    {
						if ((realLen + (oldSize - newSize)) != oldSize)
                        {
							SanityManager.THROWASSERT(
                                "recordHeader size incorrect.  realLen = " + 
                                realLen + ", delta = " + 
                                (oldSize - newSize) + ", oldSize = " + oldSize);
                        }
					}

					out.setPosition(endPosition);

					if (!forInsert) 
                    {
						// The update is incomplete, fields beyond this
						// point will have to move off the page. For any fields
						// that are not being updated we have to save their
						// values from this page to insert into an overflow 
                        // portion.
						// 
						// When the complete row is being updated there is no
						// need to save any fields so just return.
						if (validColumns != null) 
                        {
							handleIncompleteLogRow(
                                slot, nextColumn, validColumns, out);
						}
					}

					return (nextColumn);
				}
				
				columnFlag = COLUMN_NONE;
			}

			out.setBeginPosition(beginPosition);
			startColumn = -1;

			if ((calcMinimumRecordSize) && 
                (spaceAvailable < (minimumRecordSize - userRowSize)))
            {
				throw new NoSpaceOnPage(isOverflowPage()); 
            }

		} 
        finally 
        {
			resetOutputStream();
		}

		return (startColumn);
	}

    /**
     * Handle an update of a record portion that is incomplete.
     * <p>
     * Handle an update of a record portion that is incomplete.
     * Ie. Columns have expanded that require other columns to move
     * off the page into a new portion.
     * <P> 
     * This method works out of the columns that need to be moved which are not
     * being updated and makes a copy of their data. It then throws an 
     * exception with this data, much like the long column exception which will
     * then allow the original insert to complete.  
     * <P> 
     * If no columns need to be saved (ie all the ones that would move are 
     * being updated) then no exception is thrown, logRow() will return and the
     * update completes normally.
     * <p>
     *
     * @param slot          slot of the current update.
     * @param startColumn   column to start at, handles start in middle of row
     * @param columnList    bit map indicating which columns are being updated.
     * @param out           place to lot to.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private void handleIncompleteLogRow(
    int                     slot, 
    int                     startColumn, 
    FormatableBitSet                 columnList, 
    DynamicByteArrayOutputStream  out)
		throws StandardException 
    {
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(columnList != null);

		StoredRecordHeader rh = getHeaderAtSlot(slot);

		int endFieldExclusive = rh.getFirstField() + rh.getNumberFields();

		// first see if any fields are not being modified
		boolean needSave = false;
		int columnListSize = columnList.size();
		for (int i = startColumn; i < endFieldExclusive; i++) 
        {
			if (!(columnListSize > i && columnList.get(i))) 
            {
				needSave = true;
				break;
			}
		}
		if (!needSave)
			return;

		Object[] savedFields = 
            new Object[endFieldExclusive - startColumn];

		ByteArrayOutputStream fieldStream = null;

		for (int i = startColumn; i < endFieldExclusive; i++) 
        {
			// row is being updated - ignore
			if (columnListSize > i && columnList.get(i))
				continue;

			// save the data

			try 
            {
				// use the old value - we use logField to ensure that we
				// get the raw contents of the field and don't follow
				// any long columns. In addition we save this as a RawField
				// so that we preserve the state of the field header.
				if (fieldStream == null)
					fieldStream = new ByteArrayOutputStream();
				else
					fieldStream.reset();

				logField(slot, i, fieldStream);

				savedFields[i - startColumn] = 
                    new RawField(fieldStream.toByteArray());

			} 
            catch (IOException ioe) 
            {
                throw dataFactory.markCorrupt(
                    StandardException.newException(
                        SQLState.DATA_CORRUPT_PAGE, ioe, getPageId()));
			}
		}

		// Use a long column exception to notify the caller of the need
		// to perform an insert of the columns that need to move

		LongColumnException lce = new LongColumnException();
		lce.setExceptionInfo(
            out, startColumn, -1 /* indicates not actual long column */);
		lce.setColumn(savedFields);

		throw lce; 
	}

	/**

		@param row (IN/OUT) the row that is to be restored (sparse representation)
		@param limitInput the limit input stream
		@param objectInput the object input stream

		@exception StandardException	Standard Cloudscape error policy
		@exception IOException  I/O exception in reading meta data.
	*/

    /**
     * Restore a storable row from a LimitInputStream.
     * <p>
     * Restore a storable row from an LimitInputStream - user must supply two 
     * streams on top of the same data, one implements ObjectInput interface 
     * that knows how to restore the object, the other one implements 
     * LimitInputStream.
     * <p>
     * @param in           the limit input stream
     * @param row          (IN/OUT) row that is to be restored 
     *                     (sparse representation)
     * @param validColumns the columns of the row that we are interested in.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public void restoreRecordFromStream(
    LimitObjectInput        in, 
    Object[]   row) 
		throws StandardException, IOException
	{

		StoredRecordHeader recordHeader = new StoredRecordHeader();
		recordHeader.read(in);
		readRecordFromStream(
            row, 
            row.length - 1, 
            (int[]) null,
            (int[]) null, 
            in, 
            recordHeader,
            (ErrorObjectInput) null /* always null */, null);
	}

    /**
     * Process the qualifier list on the row, return true if it qualifies.
     * <p>
     * A two dimensional array is to be used to pass around a AND's and OR's in
     * conjunctive normal form.  The top slot of the 2 dimensional array is 
     * optimized for the more frequent where no OR's are present.  The first 
     * array slot is always a list of AND's to be treated as described above 
     * for single dimensional AND qualifier arrays.  The subsequent slots are 
     * to be treated as AND'd arrays or OR's.  Thus the 2 dimensional array 
     * qual[][] argument is to be treated as the following, note if 
     * qual.length = 1 then only the first array is valid and it is and an 
     * array of and clauses:
     *
     * (qual[0][0] and qual[0][0] ... and qual[0][qual[0].length - 1])
     * and
     * (qual[1][0] or  qual[1][1] ... or  qual[1][qual[1].length - 1])
     * and
     * (qual[2][0] or  qual[2][1] ... or  qual[2][qual[2].length - 1])
     * ...
     * and
     * (qual[qual.length - 1][0] or  qual[1][1] ... or  qual[1][2])
     *
     * 
	 * @return true if the row qualifies.
     *
     * @param row               The row being qualified.
     * @param qual_list         2 dimensional array representing conjunctive
     *                          normal form of simple qualifiers.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private boolean qualifyRecordFromRow(
    Object[]        row, 
    Qualifier[][]   qual_list)
		 throws StandardException
	{
        boolean     row_qualifies = true;

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(row != null);
        }

        // First do the qual[0] which is an array of qualifer terms.

        if (SanityManager.DEBUG)
        {
            // routine should not be called if there is no qualifier
            SanityManager.ASSERT(qual_list != null);
            SanityManager.ASSERT(qual_list.length > 0);
        }

        for (int i = 0; i < qual_list[0].length; i++)
        {
            // process each AND clause 

            row_qualifies = false;

            // process each OR clause.

            Qualifier q = qual_list[0][i];

            // Get the column from the possibly partial row, of the 
            // q.getColumnId()'th column in the full row.
            DataValueDescriptor columnValue = 
                    (DataValueDescriptor) row[q.getColumnId()];

            row_qualifies =
                columnValue.compare(
                    q.getOperator(),
                    q.getOrderable(),
                    q.getOrderedNulls(),
                    q.getUnknownRV());

            if (q.negateCompareResult())
                row_qualifies = !row_qualifies;

            // Once an AND fails the whole Qualification fails - do a return!
            if (!row_qualifies)
                return(false);
        }

        // all the qual[0] and terms passed, now process the OR clauses

        for (int and_idx = 1; and_idx < qual_list.length; and_idx++)
        {
            // loop through each of the "and" clause.

            row_qualifies = false;

            if (SanityManager.DEBUG)
            {
                // Each OR clause must be non-empty.
                SanityManager.ASSERT(qual_list[and_idx].length > 0);
            }

            for (int or_idx = 0; or_idx < qual_list[and_idx].length; or_idx++)
            {
                // Apply one qualifier to the row.
                Qualifier q      = qual_list[and_idx][or_idx];
                int       col_id = q.getColumnId();

                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT(
                        (col_id < row.length),
                        "Qualifier is referencing a column not in the row.");
                }

                // Get the column from the possibly partial row, of the 
                // q.getColumnId()'th column in the full row.
                DataValueDescriptor columnValue = 
                    (DataValueDescriptor) row[q.getColumnId()];

                if (SanityManager.DEBUG)
                {
                    if (columnValue == null)
                        SanityManager.THROWASSERT(
                            "1:row = " + RowUtil.toString(row) +
                            "row.length = " + row.length +
                            ";q.getColumnId() = " + q.getColumnId());
                }

                // do the compare between the column value and value in the
                // qualifier.
                row_qualifies = 
                    columnValue.compare(
                            q.getOperator(),
                            q.getOrderable(),
                            q.getOrderedNulls(),
                            q.getUnknownRV());

                if (q.negateCompareResult())
                    row_qualifies = !row_qualifies;

                // SanityManager.DEBUG_PRINT("StoredPage.qual", "processing qual[" + and_idx + "][" + or_idx + "] = " + qual_list[and_idx][or_idx] );

                // SanityManager.DEBUG_PRINT("StoredPage.qual", "value = " + row_qualifies);

                // processing "OR" clauses, so as soon as one is true, break
                // to go and process next AND clause.
                if (row_qualifies)
                    break;

            }

            // The qualifier list represented a set of "AND'd" 
            // qualifications so as soon as one is false processing is done.
            if (!row_qualifies)
                break;
        }

        return(row_qualifies);
    }

    /**
     * Read just one column from stream into row.
     * <p>
     * The routine reads just one column from the row, it is mostly code
     * taken from readRecordFromStream, but highly optimized to just get
     * one column from a non-overflow row.  It can only be called to read
     * a row from the pageData array as it directly accesses the page array
     * to avoid the Stream overhead while processing non-user data which
     * does not need the limit functionality.
     * <p>
     * It is expected that this code will be called to read in a column 
     * associated with a qualifiers which are applied one column at a time, 
     * and has been specialized to proved the greatest peformance for 
     * processing qualifiers.  This kind of access is done when scanning
     * large datasets while applying qualifiers and thus any performance
     * gain at this low level is multiplied by the large number of rows that
     * may be iterated over.
     * <p>
     * The column is read into the object located in row[qual_colid].
     *
     * @param row           column is read into object in row[qual_colid].
     * @param colid         the column id to read, colid N is row[N]
     * @param dataIn        the stream to read the column in row from.
     * @param recordHeader  the record header of the row to read column from.
     * @param recordToLock  record handle to lock, used by overflow column code.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private final void readOneColumnFromPage(
    Object[]   row, 
    int                     colid,
    int                     offset_to_field_data,
    StoredRecordHeader      recordHeader,
    RecordHandle            recordToLock)
		 throws StandardException, IOException
	{
        ErrorObjectInput    inUserCode = null;

        // Reads in this routine are always against the raw data in the 
        // pageData array, thus it can assume array access to page data array.
        ArrayInputStream lrdi = rawDataIn;

		try
		{
            if (SanityManager.DEBUG)
            {
                if (colid >= row.length)
                    SanityManager.THROWASSERT(
                        "colid = " + colid +
                        ";row length = " + row.length);

                // currently this routine will not work on long rows.
                if (recordHeader.getFirstField() != 0)
                {
                    SanityManager.THROWASSERT(
                        "recordHeader.getFirstField() = " +
                        recordHeader.getFirstField());
                }
            }

            Object column     = row[colid];

            // if the column id exists on this page.
            if (colid <= (recordHeader.getNumberFields() - 1))
            {
                // skip the fields before colid, the column in question
                // existent on this page.

                for (int columnId = colid; columnId > 0; columnId--)
                {
                    offset_to_field_data += 
                        StoredFieldHeader.readTotalFieldLength(
                            pageData, offset_to_field_data);
                }



				// read the field header

                // read the status byte.
				int fieldStatus     = 
                    StoredFieldHeader.readStatus(
                        pageData, offset_to_field_data);

                // read the field data length, and position on 1st byte of data.
				int fieldDataLength = 
                    StoredFieldHeader.readFieldLengthAndSetStreamPosition(
                        pageData, 
                        offset_to_field_data + 
                            StoredFieldHeader.STORED_FIELD_HEADER_STATUS_SIZE,
                        fieldStatus,
                        slotFieldSize,
                        lrdi);

				if (SanityManager.DEBUG) 
                {
					SanityManager.ASSERT(
                        !StoredFieldHeader.isExtensible(fieldStatus), 
                        "extensible fields not supported yet");
				}

				// SRW-DJD code assumes non-extensible case ...

                if (!StoredFieldHeader.isNonexistent(fieldStatus))
                {
                    boolean isOverflow = 
                        StoredFieldHeader.isOverflow(fieldStatus);

                    OverflowInputStream overflowIn = null;

                    if (isOverflow) 
                    {
                        // A fetched long column is returned as a stream
                        long overflowPage   = 
                            CompressedNumber.readLong((InputStream) lrdi);

                        int overflowId      = 
                            CompressedNumber.readInt((InputStream) lrdi);

                        // Prepare the stream for results...
                        // create the byteHolder the size of a page, so, that it 
                        // will fit the field Data that would fit on a page.
                        MemByteHolder byteHolder = 
                            new MemByteHolder(pageData.length);

                        overflowIn = new OverflowInputStream(
                            byteHolder, owner, overflowPage, 
                            overflowId, recordToLock);
                    }

                    // Deal with Storable columns
                    if (column instanceof DataValueDescriptor) 
                    {
                        DataValueDescriptor sColumn = 
                            (DataValueDescriptor) column;

                        // is the column null ?
                        if (StoredFieldHeader.isNull(fieldStatus)) 
                        {
                            sColumn.restoreToNull();
                        }
                        else
                        {
                            // set the limit for the user read
                            if (!isOverflow) 
                            {
                                // normal, non-overflow column case.

                                lrdi.setLimit(fieldDataLength);
                                inUserCode = lrdi;
                                sColumn.readExternalFromArray(lrdi);
                                inUserCode = null;
                                int unread = lrdi.clearLimit();
                                if (unread != 0)
                                    lrdi.skipBytes(unread);
                            }
                            else
                            {
                                // fetched column is a Storable long column.

                                FormatIdInputStream newIn = 
                                    new FormatIdInputStream(overflowIn);

                                if ((sColumn instanceof StreamStorable)) 
                                {
                                    ((StreamStorable)sColumn).setStream(newIn);
                                } 
                                else 
                                {
                                    inUserCode = newIn;
                                    sColumn.readExternal(newIn);
                                    inUserCode = null;
                                }
                            } 
                        }
                    }
                    else
                    {
                        // At this point only non-Storable columns.

                        if (StoredFieldHeader.isNull(fieldStatus))
                        {
                            // Only Storables can be null ...

                            throw StandardException.newException(
                                    SQLState.DATA_NULL_STORABLE_COLUMN,
                                    Integer.toString(colid));
                        }

                        // This is a non-extensible field, which means the 
                        // caller must know the correct type and thus the 
                        // element in row is the correct type or null. It must 
                        // be Serializable.
                        //
                        // We do not support Externalizable here.

                        lrdi.setLimit(fieldDataLength);
                        inUserCode = lrdi;
                        // RESOLVE (no non-storables?)
                        row[colid] = (Object) lrdi.readObject();
                        inUserCode = null;
                        int unread = lrdi.clearLimit();
                        if (unread != 0)
                            lrdi.skipBytes(unread);
                    }

                }
                else
                {
                    // column does not exist in the row, return null.

                    // field is non-existent

                    if (column instanceof DataValueDescriptor) 
                    {
                        // RESOLVE - This is in place for 1.2. In the future
                        // we may want to return this column as non-existent
                        // even if it is a storable column, or maybe use a
                        // supplied default.

                        ((DataValueDescriptor) column).restoreToNull();
                    } 
                    else 
                    {
                        row[colid] = null;
                    }
                }
            }
            else
            {
                // field does not exist on this page.

                if (column instanceof DataValueDescriptor) 
                {
                    // RESOLVE - This is in place for 1.2. In the future
                    // we may want to return this column as non-existent
                    // even if it is a storable column, or maybe use a 
                    // supplied default.
                    ((DataValueDescriptor) column).restoreToNull();
                } 
                else 
                {
                    row[colid] = null;
                }
            }
		} 
        catch (IOException ioe) 
        {
			// an exception during the restore of a user column, this doesn't
			// make the database corrupt, just that this field is inaccessable

			if (inUserCode != null) 
            {
				lrdi.clearLimit();

				if (ioe instanceof EOFException) 
                {
                    if (SanityManager.DEBUG)
                    {
                        SanityManager.DEBUG_PRINT("DEBUG_TRACE",
                            "StoredPage.readOneColumnFromPage - EOF while restoring record: " +
                                recordHeader +
                            "Page dump = " + this);
                        SanityManager.showTrace(ioe);
                    }

					// going beyond the limit in a DataInput class results in
                    // an EOFException when it sees the -1 from a read
					throw StandardException.newException(
                            SQLState.DATA_STORABLE_READ_MISMATCH,
                            ioe, inUserCode.getErrorInfo());
				}

                // some SQLData error reporting
                Exception ne = inUserCode.getNestedException();
                if (ne != null)
                {
                    if (ne instanceof InstantiationException)
                    {
                        throw StandardException.newException(
                            SQLState.DATA_SQLDATA_READ_INSTANTIATION_EXCEPTION,
                            ne, inUserCode.getErrorInfo());
                    }

                    if (ne instanceof IllegalAccessException)
                    {
                        throw StandardException.newException(
                            SQLState.DATA_SQLDATA_READ_ILLEGAL_ACCESS_EXCEPTION,
                            ne, inUserCode.getErrorInfo());
                    }

                    if (ne instanceof StandardException)
                    {
                        throw (StandardException) ne;
                    }
                }

				throw StandardException.newException(
                        SQLState.DATA_STORABLE_READ_EXCEPTION,
                        ioe, inUserCode.getErrorInfo());
			}

			// re-throw to higher levels so they can put it in correct context.
			throw ioe;

		} 
        catch (ClassNotFoundException cnfe) 
        {
			lrdi.clearLimit();

			// an exception during the restore of a user column, this doesn't
			// make the database corrupt, just that this field is inaccessable
			throw StandardException.newException(
                    SQLState.DATA_STORABLE_READ_MISSING_CLASS,
                    cnfe, inUserCode.getErrorInfo());

		} 
        catch (LinkageError le)
        {
			// Some error during the link of a user class
			if (inUserCode != null)
            {
				lrdi.clearLimit();

                throw StandardException.newException(
                        SQLState.DATA_STORABLE_READ_EXCEPTION,
                        le, inUserCode.getErrorInfo());
			}
			throw le;
		}

	}



    /**
     * Process the list of qualifiers on the row in the stream.
     * <p>
     * The rawDataIn stream is expected to be positioned after the record 
     * header.  The inUserCode parameter here is only to get around a 
     * JDK 1.1.x (at least 1.1.7) JIT bug. If inUserCode was a local variable 
     * then it is not correctly set on an exception, the only time we care 
     * about its value. It seems to work when its a parameter. Null should 
     * always be passed in.  This bug is fixed in the JDK 1.2 JIT.
     * <p>
     * Check all qualifiers in the qualifier array against row.  Return true
     * if all compares specified by the qualifier array return true, else
     * return false.
     * <p>
     * This routine assumes client caller has already checked if the row
     * is deleted or not.  The row that it get's is expected to match
     * the partial column list of the scan.  
     * <p>
     * On entering this routine the stream should be positioned to the
     * beginning of the row data, just after the row header.  On exit
     * the stream will also be positioned there.
     *
     * A two dimensional array is to be used to pass around a AND's and OR's in
     * conjunctive normal form.  The top slot of the 2 dimensional array is 
     * optimized for the more frequent where no OR's are present.  The first 
     * array slot is always a list of AND's to be treated as described above 
     * for single dimensional AND qualifier arrays.  The subsequent slots are 
     * to be treated as AND'd arrays or OR's.  Thus the 2 dimensional array 
     * qual[][] argument is to be treated as the following, note if 
     * qual.length = 1 then only the first array is valid and it is and an 
     * array of and clauses:
     *
     * (qual[0][0] and qual[0][0] ... and qual[0][qual[0].length - 1])
     * and
     * (qual[1][0] or  qual[1][1] ... or  qual[1][qual[1].length - 1])
     * and
     * (qual[2][0] or  qual[2][1] ... or  qual[2][qual[2].length - 1])
     * ...
     * and
     * (qual[qual.length - 1][0] or  qual[1][1] ... or  qual[1][2])
     *
	 * @return Whether or not the row input qualifies.
     *
     * @param row               restore row into this object array.
     * @param validColumns      If not null, bit map indicates valid cols.
     * @param materializedCols  Which columns have already been read into row?
     * @param lrdi              restore row from this stream.
     * @param recordHeader      The record header of the row, it was read in 
     *                          from stream and dataIn is positioned after it.
     * @param inUserCode        see comments above about jit bug. 
     * @param qualifier_list    An array of qualifiers to apply to the row, only
     *                          return row if qualifiers are all true, if array
     *                          is null always return the row.
     * @param recordToLock      The head row to use for locking, used to lock 
     *                          head row of overflow columns/rows.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private final boolean qualifyRecordFromSlot(
    Object[]                row, 
    int                     offset_to_row_data,
    FetchDescriptor         fetchDesc,
    StoredRecordHeader      recordHeader,
    RecordHandle            recordToLock)
		 throws StandardException, IOException
	{
        boolean         row_qualifies    = true;
        Qualifier[][]   qual_list        = fetchDesc.getQualifierList();
        int[]           materializedCols = fetchDesc.getMaterializedColumns();

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(qual_list != null, "Not coded yet!");
        }

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(row != null);
        }

        // First process the initial list of AND's in the 1st array

        for (int i = 0; i < qual_list[0].length; i++)
        {
            // process each AND clause 

            row_qualifies = false;

            // Apply one qualifier to the row.
            Qualifier q      = qual_list[0][i];
            int       col_id = q.getColumnId();

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(
                    (col_id < row.length),
                    "Qualifier is referencing a column not in the row.");
            }

            // materialize the column object if we haven't done it yet.
            if (materializedCols[col_id] == 0)
            {
                // materialize just this column from the row, no qualifiers
                readOneColumnFromPage(
                    row, 
                    col_id,
                    offset_to_row_data,
                    recordHeader,
                    recordToLock);

                // mark offset, indicating the row has been read in.
                //
                // RESOLVE (mikem) - right now value of entry is useless, it
                // is an int so that in the future we could cache the offset
                // to fields to improve performance of getting to a column 
                // after qualifying.
                materializedCols[col_id] = offset_to_row_data;
            }

            // Get the column from the possibly partial row, of the 
            // q.getColumnId()'th column in the full row.

            if (SanityManager.DEBUG)
            {
                if (row[col_id] == null)
                    SanityManager.THROWASSERT(
                        "1:row = " + RowUtil.toString(row) +
                        "row.length = " + row.length +
                        ";q.getColumnId() = " + q.getColumnId());
            }

            // do the compare between the column value and value in the
            // qualifier.
            row_qualifies = 
                ((DataValueDescriptor) row[col_id]).compare(
                        q.getOperator(),
                        q.getOrderable(),
                        q.getOrderedNulls(),
                        q.getUnknownRV());

            if (q.negateCompareResult())
                row_qualifies = !row_qualifies;

            // Once an AND fails the whole Qualification fails - do a return!
            if (!row_qualifies)
                return(false);
        }

        // Now process the Subsequent OR clause's, beginning with qual_list[1]

        for (int and_idx = 1; and_idx < qual_list.length; and_idx++)
        {
            // loop through each of the "and" clause.

            row_qualifies = false;

            for (int or_idx = 0; or_idx < qual_list[and_idx].length; or_idx++)
            {
                // Apply one qualifier to the row.
                Qualifier q      = qual_list[and_idx][or_idx];
                int       col_id = q.getColumnId();

                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT(
                        (col_id < row.length),
                        "Qualifier is referencing a column not in the row.");
                }

                // materialize the column object if we haven't done it yet.
                if (materializedCols[col_id] == 0)
                {
                    // materialize just this column from the row, no qualifiers
                    readOneColumnFromPage(
                        row, 
                        col_id,
                        offset_to_row_data,
                        recordHeader,
                        recordToLock);

                    // mark offset, indicating the row has been read in.
                    //
                    // RESOLVE (mikem) - right now value of entry is useless, it
                    // is an int so that in the future we could cache the offset
                    // to fields to improve performance of getting to a column 
                    // after qualifying.
                    materializedCols[col_id] = offset_to_row_data;
                }

                // Get the column from the possibly partial row, of the 
                // q.getColumnId()'th column in the full row.

                if (SanityManager.DEBUG)
                {
                    if (row[col_id] == null)
                        SanityManager.THROWASSERT(
                            "1:row = " + RowUtil.toString(row) +
                            "row.length = " + row.length +
                            ";q.getColumnId() = " + q.getColumnId());
                }

                // do the compare between the column value and value in the
                // qualifier.
                row_qualifies = 
                    ((DataValueDescriptor) row[col_id]).compare(
                            q.getOperator(),
                            q.getOrderable(),
                            q.getOrderedNulls(),
                            q.getUnknownRV());


                if (q.negateCompareResult())
                    row_qualifies = !row_qualifies;

                // SanityManager.DEBUG_PRINT("StoredPage.qual", "processing qual[" + and_idx + "][" + or_idx + "] = " + qual_list[and_idx][or_idx] );

                // SanityManager.DEBUG_PRINT("StoredPage.qual", "value = " + row_qualifies);

                // processing "OR" clauses, so as soon as one is true, break
                // to go and process next AND clause.
                if (row_qualifies)
                    break;

            }

            // The qualifier list represented a set of "AND'd" 
            // qualifications so as soon as one is false processing is done.
            if (!row_qualifies)
                break;
        }

        return(row_qualifies);
    }

    /**
     * restore a record from a stream.
     * <p>
     * The rawDataIn stream is expected to be positioned after the record 
     * header.  The inUserCode parameter here is only to get around a 
     * JDK 1.1.x (at least 1.1.7) JIT bug. If inUserCode was a local variable 
     * then it is not correctly set on an exception, the only time we care 
     * about its value. It seems to work when its a parameter. Null should 
     * always be passed in.  This bug is fixed in the JDK 1.2 JIT.
     * <p>
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
     * @param row               restore row into this object array.
     * @param max_colid         The maximum numbered column id that will be 
     *                          requested by caller.  It should be:
     *                            min(row.length - 1, maximum bit set in vCols)
     *                          It is used to stop the inner most loop from 
     *                          looking at more columns in the row.
     * @param vCols             If not null, bit map indicates valid cols.
     * @param mCols             If not null, int array indicates columns already
     *                          read in from the stream.  A non-zero entry 
     *                          means the column has already been read in.
     * @param dataIn            restore row from this stream.
     * @param recordHeader      The record header of the row, it was read in 
     *                          from stream and dataIn is positioned after it.
     * @param inUserCode        see comments above about jit bug. 
     * @param recordToLock      The head row to use for locking, used to lock 
     *                          head row of overflow columns/rows.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private final boolean readRecordFromStream(
    Object[]   row, 
    int                     max_colid,
    int[]                   vCols,
    int[]                   mCols,
	LimitObjectInput        dataIn, 
    StoredRecordHeader      recordHeader,
    ErrorObjectInput        inUserCode, 
    RecordHandle            recordToLock)
		 throws StandardException, IOException
	{
		try
		{
			// Get the number of columns in the row.
			int numberFields = recordHeader.getNumberFields();

			int startColumn = recordHeader.getFirstField();

			if (startColumn > max_colid)
            {
                // done if the startColumn is higher than highest column.
				return true;
            }

			// For each column in the row, restore the column from
			// the corresponding field in the record.  If the field
			// is missing or not set, set the column to null.

			int highestColumnOnPage = numberFields + startColumn;

			int vColsSize           = (vCols == null ) ? 0 : vCols.length;

 			for (int columnId = startColumn; columnId <= max_colid; columnId++) 
            {
				// skip any "existing" columns not requested, or requested cols
                // that have already been read.
				if (((vCols != null) && 
                     (!(vColsSize > columnId && (vCols[columnId] != 0)))) ||
				    ((mCols != null) && (mCols[columnId] != 0)))
                {
                    if (columnId < highestColumnOnPage)
                    {
                        // If the field exists in the row on the page, but the
                        // partial row being returned does not include it,
                        // skip the field ...
                        
                        skipField(dataIn);
                    }

					continue;
				}

				// See if the column identifier is beyond the number of fields
                // that this record has
				if (columnId >= highestColumnOnPage) 
                {
					// field is non-existent
					Object column = row[columnId];

                    if (column instanceof DataValueDescriptor) 
                    {
						// RESOLVE - This is in place for 1.2. In the future
						// we may want to return this column as non-existent
						// even if it is a storable column, or maybe use a
                        // supplied default.

						((DataValueDescriptor) column).restoreToNull();
					} 
                    else 
                    {
						row[columnId] = null;
					}
					continue;
				}

				// read the field header
				int fieldStatus     = 
                    StoredFieldHeader.readStatus(dataIn);

				int fieldDataLength = 
                    StoredFieldHeader.readFieldDataLength(
                        dataIn, fieldStatus, slotFieldSize);

				if (SanityManager.DEBUG) 
                {
					SanityManager.ASSERT(
                        !StoredFieldHeader.isExtensible(fieldStatus), 
                        "extensible fields not supported yet");
				}

                Object column     = row[columnId];

				OverflowInputStream overflowIn = null;

				// SRW-DJD code assumes non-extensible case ...

				// field is non-existent, return null
				if (StoredFieldHeader.isNonexistent(fieldStatus)) 
                {

					if (column instanceof DataValueDescriptor) 
                    {
						// RESOLVE - This is in place for 1.2. In the future
						// we may want to return this column as non-existent
						// even if it is a storable column, or maybe use a 
                        // supplied default.
						((DataValueDescriptor) column).restoreToNull();
					} 
                    else 
                    {
						row[columnId] = null;
					}
					continue;
				}

				boolean isOverflow = StoredFieldHeader.isOverflow(fieldStatus);

				if (isOverflow) 
                {

					// A fetched long column needs to be returned as a stream
                    //
					long overflowPage   = 
                        CompressedNumber.readLong((InputStream) dataIn);

					int overflowId      = 
                        CompressedNumber.readInt((InputStream) dataIn);

					// Prepare the stream for results...
					// create the byteHolder the size of a page, so, that it 
                    // will fit the field Data that would fit on a page.
					MemByteHolder byteHolder = 
                        new MemByteHolder(pageData.length);

					overflowIn = new OverflowInputStream(
                        byteHolder, owner, overflowPage, 
                        overflowId, recordToLock);
				}

				// Deal with Object columns
                if (column instanceof DataValueDescriptor) 
                {
					DataValueDescriptor sColumn = (DataValueDescriptor) column;

					// is the column null ?
					if (StoredFieldHeader.isNull(fieldStatus)) 
                    {
						sColumn.restoreToNull();
						continue;
					}

					// set the limit for the user read
					if (!isOverflow) 
                    {
                        // normal, non-overflow column case.

						dataIn.setLimit(fieldDataLength);
						inUserCode = dataIn;
						sColumn.readExternal(dataIn);
						inUserCode = null;
						int unread = dataIn.clearLimit();
						if (unread != 0)
							dataIn.skipBytes(unread);
					}
                    else
                    {
                        // column being fetched is a Object long column.

						FormatIdInputStream newIn = 
                            new FormatIdInputStream(overflowIn);

						// if a column is a long column, store recommends user 
                        // fetch it as a stream.
						boolean fetchStream = true;

						if (!(sColumn instanceof StreamStorable)) 
                        {
							fetchStream = false;
						}

						if (fetchStream) 
                        {
							((StreamStorable)sColumn).setStream(newIn);
						} 
                        else 
                        {
							inUserCode = newIn;
							sColumn.readExternal(newIn);
							inUserCode = null;
						}

					} 

					continue;
				}

                // At this point only non-Storable columns.

				if (StoredFieldHeader.isNull(fieldStatus))
                {
                    // Only Storables can be null ...

                    throw StandardException.newException(
                            SQLState.DATA_NULL_STORABLE_COLUMN,
                            Integer.toString(columnId));
                }

				// This is a non-extensible field, which means the caller must 
                // know the correct type and thus the element in row is the 
                // correct type or null. It must be Serializable.
                //
                // We do not support Externalizable here.

				dataIn.setLimit(fieldDataLength);
				inUserCode = dataIn;
				row[columnId] = (Object) dataIn.readObject();
				inUserCode = null;
				int unread = dataIn.clearLimit();
				if (unread != 0)
					dataIn.skipBytes(unread);

				continue;
			}

			// if the last column on this page is bigger than the highest 
            // column we are looking for, then we are done restoring the record.

			if ((numberFields + startColumn) > max_colid)
				return true;
			else
				return false;

		} 
        catch (IOException ioe) 
        {
			// an exception during the restore of a user column, this doesn't
			// make the database corrupt, just that this field is inaccessable

			if (inUserCode != null) 
            {
				dataIn.clearLimit();

				if (ioe instanceof EOFException) 
                {
                    if (SanityManager.DEBUG)
                    {
                        SanityManager.DEBUG_PRINT("DEBUG_TRACE",
                            "StoredPage - EOF while restoring record: " +
                                recordHeader +
                            "Page dump = " + this);
                    }

					// going beyond the limit in a DataInput class results in
                    // an EOFException when it sees the -1 from a read
					throw StandardException.newException(
                            SQLState.DATA_STORABLE_READ_MISMATCH,
                            ioe, inUserCode.getErrorInfo());
				}

                // some SQLData error reporting
                Exception ne = inUserCode.getNestedException();
                if (ne != null)
                {
                    if (ne instanceof InstantiationException)
                    {
                        throw StandardException.newException(
                            SQLState.DATA_SQLDATA_READ_INSTANTIATION_EXCEPTION,
                            ne, inUserCode.getErrorInfo());
                    }

                    if (ne instanceof IllegalAccessException)
                    {
                        throw StandardException.newException(
                            SQLState.DATA_SQLDATA_READ_ILLEGAL_ACCESS_EXCEPTION,
                            ne, inUserCode.getErrorInfo());
                    }

                    if (ne instanceof StandardException)
                    {
                        throw (StandardException) ne;
                    }
                }

				throw StandardException.newException(
                        SQLState.DATA_STORABLE_READ_EXCEPTION,
                        ioe, inUserCode.getErrorInfo());
			}

			// re-throw to higher levels so they can put it in correct context.
			throw ioe;

		} 
        catch (ClassNotFoundException cnfe) 
        {
			dataIn.clearLimit();

			// an exception during the restore of a user column, this doesn't
			// make the database corrupt, just that this field is inaccessable
			throw StandardException.newException(
                    SQLState.DATA_STORABLE_READ_MISSING_CLASS,
                    cnfe, inUserCode.getErrorInfo());

		} 
        catch (LinkageError le)
        {
			// Some error during the link of a user class
			if (inUserCode != null)
            {
				dataIn.clearLimit();

                throw StandardException.newException(
                        SQLState.DATA_STORABLE_READ_EXCEPTION,
                        le, inUserCode.getErrorInfo());
			}
			throw le;
		}

	}

	private final boolean readRecordFromArray(
    Object[]   row, 
    int                     max_colid,
    int[]                   vCols,
    int[]                   mCols,
	ArrayInputStream        dataIn, 
    StoredRecordHeader      recordHeader,
    ErrorObjectInput        inUserCode, 
    RecordHandle            recordToLock)
		 throws StandardException, IOException
	{
		try
		{
			// Get the number of columns in the row.
			int numberFields = recordHeader.getNumberFields();

			int startColumn = recordHeader.getFirstField();

			if (startColumn > max_colid)
            {
                // done if the startColumn is higher than highest column.
				return true;
            }

			// For each column in the row, restore the column from
			// the corresponding field in the record.  If the field
			// is missing or not set, set the column to null.

			int highestColumnOnPage = numberFields + startColumn;

			int vColsSize           = (vCols == null ) ? 0 : vCols.length;

            int offset_to_field_data = dataIn.getPosition();

 			for (int columnId = startColumn; columnId <= max_colid; columnId++) 
            {
				// skip any "existing" columns not requested, or requested cols
                // that have already been read.
				if (((vCols != null) && 
                     (!(vColsSize > columnId && (vCols[columnId] != 0)))) ||
				    ((mCols != null) && (mCols[columnId] != 0)))
                {
                    if (columnId < highestColumnOnPage)
                    {
                        // If the field exists in the row on the page, but the
                        // partial row being returned does not include it,
                        // skip the field ...
                        
                        offset_to_field_data += 
                            StoredFieldHeader.readTotalFieldLength(
                                pageData, offset_to_field_data);
                    }

					continue;
				}
                else if (columnId < highestColumnOnPage) 
                {
                    // the column is on this page.

                    // read the field header

                    // read the status byte.
                    int fieldStatus     = 
                        StoredFieldHeader.readStatus(
                            pageData, offset_to_field_data);

                    // read the field data length, position on 1st byte of data
                    int fieldDataLength = 
                        StoredFieldHeader.readFieldLengthAndSetStreamPosition(
                            pageData, 
                            offset_to_field_data + 
                              StoredFieldHeader.STORED_FIELD_HEADER_STATUS_SIZE,
                            fieldStatus,
                            slotFieldSize,
                            dataIn);


                    if (SanityManager.DEBUG) 
                    {
                        SanityManager.ASSERT(
                            !StoredFieldHeader.isExtensible(fieldStatus), 
                            "extensible fields not supported yet");
                    }

                    Object              column     = row[columnId];

                    OverflowInputStream overflowIn = null;

                    // SRW-DJD code assumes non-extensible case ...

                    if ((fieldStatus & StoredFieldHeader.FIELD_NONEXISTENT) != 
                                            StoredFieldHeader.FIELD_NONEXISTENT)
                    {
                        // normal path - field exists.

                        boolean isOverflow = 
                            ((fieldStatus & 
                                  StoredFieldHeader.FIELD_OVERFLOW) != 0);

                        if (isOverflow) 
                        {

                            // A fetched long column is returned as a stream

                            long overflowPage   = 
                                CompressedNumber.readLong((InputStream) dataIn);

                            int overflowId      = 
                                CompressedNumber.readInt((InputStream) dataIn);

                            // Prepare the stream for results...
                            // create the byteHolder the size of a page, so, 
                            // that it will fit the field Data that would fit 
                            // on a page.

                            MemByteHolder byteHolder = 
                                new MemByteHolder(pageData.length);

                            overflowIn = new OverflowInputStream(
                                byteHolder, owner, overflowPage, 
                                overflowId, recordToLock);
                        }

                        // Deal with Object columns
                        if (column instanceof DataValueDescriptor) 
                        {
                            DataValueDescriptor sColumn = 
                                (DataValueDescriptor) column;

                            // is the column null ?
                            if ((fieldStatus & 
                                        StoredFieldHeader.FIELD_NULL) == 0)
                            {
                                // the field is not null.

                                // set the limit for the user read
                                if (!isOverflow) 
                                {
                                    // normal, non-overflow column case.

                                    dataIn.setLimit(fieldDataLength);
                                    inUserCode = dataIn;
                                    sColumn.readExternalFromArray(dataIn);
                                    inUserCode = null;
                                    int unread = dataIn.clearLimit();
                                    if (unread != 0)
                                        dataIn.skipBytes(unread);
                                }
                                else
                                {
                                    // column being fetched is a long column.

                                    FormatIdInputStream newIn = 
                                        new FormatIdInputStream(overflowIn);

                                    // long columns are fetched as a stream.

                                    boolean fetchStream = true;

                                    if (!(sColumn instanceof StreamStorable)) 
                                    {
                                        fetchStream = false;
                                    }

                                    if (fetchStream) 
                                    {
                                        ((StreamStorable) sColumn).setStream(
                                                                         newIn);
                                    } 
                                    else 
                                    {
                                        inUserCode = newIn;
                                        sColumn.readExternal(newIn);
                                        inUserCode = null;
                                    }
                                } 
                            }
                            else
                            {
                                sColumn.restoreToNull();
                            }

                        }
                        else
                        {

                            // At this point only non-Storable columns.

                            if (StoredFieldHeader.isNull(fieldStatus))
                            {
                                // Only Storables can be null ...

                                throw StandardException.newException(
                                        SQLState.DATA_NULL_STORABLE_COLUMN,
                                        Integer.toString(columnId));
                            }

                            // This is a non-extensible field, which means the 
                            // caller must know the correct type and thus the 
                            // element in row is the correct type or null. It 
                            // must be Serializable.
                            //
                            // We do not support Externalizable here.

                            dataIn.setLimit(fieldDataLength);
                            inUserCode = dataIn;
                                    // RESOLVE (no non-storables?)
                            row[columnId] = (Object) dataIn.readObject();
                            inUserCode = null;
                            int unread = dataIn.clearLimit();
                            if (unread != 0)
                                dataIn.skipBytes(unread);
                        }
                    }
                    else
                    {
                        // column is non-existent.

                        if (column instanceof DataValueDescriptor) 
                        {
                            // RESOLVE - This is in place for 1.2. In the future
                            // we may want to return this column as non-existent
                            // even if it is a storable column, or maybe use a 
                            // supplied default.
                            ((DataValueDescriptor) column).restoreToNull();
                        } 
                        else 
                        {
                            row[columnId] = null;
                        }
                    }

                    // move the counter to point to beginning of next field.
                    offset_to_field_data = dataIn.getPosition();
                }
                else
                {
					// field is non-existent
					Object column = row[columnId];

                    if (column instanceof DataValueDescriptor) 
                    {
						// RESOLVE - This is in place for 1.2. In the future
						// we may want to return this column as non-existent
						// even if it is a storable column, or maybe use a
                        // supplied default.

						((DataValueDescriptor) column).restoreToNull();
					} 
                    else 
                    {
						row[columnId] = null;
					}
				}
			}

			// if the last column on this page is bigger than the highest 
            // column we are looking for, then we are done restoring the record.

			if ((numberFields + startColumn) > max_colid)
				return true;
			else
				return false;

		} 
        catch (IOException ioe) 
        {
			// an exception during the restore of a user column, this doesn't
			// make the database corrupt, just that this field is inaccessable

			if (inUserCode != null) 
            {
				dataIn.clearLimit();

				if (ioe instanceof EOFException) 
                {
                    if (SanityManager.DEBUG)
                    {
                        SanityManager.DEBUG_PRINT("DEBUG_TRACE",
                            "StoredPage - EOF while restoring record: " +
                                recordHeader +
                            "Page dump = " + this);
                    }

					// going beyond the limit in a DataInput class results in
                    // an EOFException when it sees the -1 from a read
					throw StandardException.newException(
                            SQLState.DATA_STORABLE_READ_MISMATCH,
                            ioe, inUserCode.getErrorInfo());
				}

                // some SQLData error reporting
                Exception ne = inUserCode.getNestedException();
                if (ne != null)
                {
                    if (ne instanceof InstantiationException)
                    {
                        throw StandardException.newException(
                            SQLState.DATA_SQLDATA_READ_INSTANTIATION_EXCEPTION,
                            ne, inUserCode.getErrorInfo());
                    }

                    if (ne instanceof IllegalAccessException)
                    {
                        throw StandardException.newException(
                            SQLState.DATA_SQLDATA_READ_ILLEGAL_ACCESS_EXCEPTION,
                            ne, inUserCode.getErrorInfo());
                    }

                    if (ne instanceof StandardException)
                    {
                        throw (StandardException) ne;
                    }
                }

				throw StandardException.newException(
                        SQLState.DATA_STORABLE_READ_EXCEPTION,
                        ioe, inUserCode.getErrorInfo());
			}

			// re-throw to higher levels so they can put it in correct context.
			throw ioe;

		} 
        catch (ClassNotFoundException cnfe) 
        {
			dataIn.clearLimit();

			// an exception during the restore of a user column, this doesn't
			// make the database corrupt, just that this field is inaccessable
			throw StandardException.newException(
                    SQLState.DATA_STORABLE_READ_MISSING_CLASS,
                    cnfe, inUserCode.getErrorInfo());

		} 
        catch (LinkageError le)
        {
			// Some error during the link of a user class
			if (inUserCode != null)
            {
				dataIn.clearLimit();

                throw StandardException.newException(
                        SQLState.DATA_STORABLE_READ_EXCEPTION,
                        le, inUserCode.getErrorInfo());
			}
			throw le;
		}

	}

    /**
     * Restore a portion of a long column.
     * <p>
     * Restore a portion of a long column - user must supply two streams on top
     * of the same data, one implements ObjectInput interface that knows how to
     * restore the object, the other one implements LimitInputStream.
     *
     * @param fetchStream  the stream to read the next portion of long col from
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public void restorePortionLongColumn(
    OverflowInputStream fetchStream)
		throws StandardException, IOException
	{
		int                 slot       = 
            findRecordById(fetchStream.getOverflowId(), FIRST_SLOT_NUMBER);

		StoredRecordHeader recordHeader = getHeaderAtSlot(slot);

		int offset       = getRecordOffset(slot);
		int numberFields = recordHeader.getNumberFields();

		if (SanityManager.DEBUG) 
        {
			if ((numberFields > 2) || (numberFields < 1))
            {
				SanityManager.THROWASSERT(
					"longColumn record header must have 1 or 2 fields." +
                    "numberFields = " + numberFields);
            }
		}

		rawDataIn.setPosition(offset + recordHeader.size());

		int fieldStatus     = 
            StoredFieldHeader.readStatus(rawDataIn);
		int fieldDataLength = 
            StoredFieldHeader.readFieldDataLength(
                rawDataIn, fieldStatus, slotFieldSize);

        // read the data portion of this segment from the stream.

		ByteHolder bh = fetchStream.getByteHolder();
		bh.write(rawDataIn, fieldDataLength);
		fetchStream.setByteHolder(bh);

		// set the next overflow pointer in the stream...
		if (numberFields == 1) 
        {
			// this is the last bit of the long column
			fetchStream.setOverflowPage(-1);
			fetchStream.setOverflowId(-1);
		} 
        else 
        {
			int firstFieldStatus = fieldStatus;	// for DEBUG check

            // get the field status and data length of the overflow pointer.
			fieldStatus     = 
                StoredFieldHeader.readStatus(rawDataIn);
			fieldDataLength = 
                StoredFieldHeader.readFieldDataLength(
                    rawDataIn, fieldStatus, slotFieldSize);

			if (SanityManager.DEBUG)
			{
				if (!StoredFieldHeader.isOverflow(fieldStatus))
				{
					// In version 1.5, the first field is overflow and the
					// second is not.   In version 2.0 onwards, the first
					// field is not overflow and the second is overflow
					// (the overflow bit goes with the overflow pointer).
					// Check first field to make sure its overflow bit is
					// set on.
                    SanityManager.ASSERT(
                        StoredFieldHeader.isOverflow(firstFieldStatus));
				}
			}

			long overflowPage = 
                CompressedNumber.readLong((InputStream) rawDataIn);
			int  overflowId   = 
                CompressedNumber.readInt((InputStream) rawDataIn);

			// there is more after this chunk.
			fetchStream.setOverflowPage(overflowPage);
			fetchStream.setOverflowId(overflowId);
		}
	}


    /**
     * Log a Storable to a stream.
     * <p>
     * Log a Storable into a stream.  This is used by update field operations
     * <P>
     * Write the column in its field format to the stream. Field format is a 
     * field header followed the data of the column as defined by the data 
     * itself.  See this class's description for the specifics of the header.
     *
     * @exception StandardException	    Standard Cloudscape error policy
     * @exception IOException			RESOLVE
     **/
	public void logColumn(
    int                     slot, 
    int                     fieldId, 
    Object                  column, 
    DynamicByteArrayOutputStream  out, 
    int                     overflowThreshold)
		throws StandardException, IOException
	{
		// calculate the space available on the page, it includes
		//	the free space
		//  the space the record has reserved but not used
		//  the length of the old field itself

		// free space
		int bytesAvailable  = freeSpace;
		int beginPosition   = -1;

		// space reserved, but not used by the record
		bytesAvailable      += getReservedCount(slot);

		// The size of the old field is also available for the new field
		rawDataIn.setPosition(getFieldOffset(slot, fieldId));

		int fieldStatus     = 
            StoredFieldHeader.readStatus(rawDataIn);
		int fieldDataLength = 
            StoredFieldHeader.readFieldDataLength(
                rawDataIn, fieldStatus, slotFieldSize);

		bytesAvailable += 
            StoredFieldHeader.size(fieldStatus, fieldDataLength, slotFieldSize) 
                + fieldDataLength;

		try 
        {
			setOutputStream(out);
			beginPosition = rawDataOut.getPosition();

			Object[] row = new Object[1];
			row[0]       = column;
			if (bytesAvailable == logColumn(
                                        row, 0, out, bytesAvailable, 
                                        COLUMN_NONE, overflowThreshold)) 
            {
				throw new NoSpaceOnPage(isOverflowPage());
			}

		} 
        finally 
        {
			rawDataOut.setPosition(beginPosition);
			resetOutputStream();
		}
	}

    /**
     * Log a long column into a DataOuput.
     * <p>
     * Log a long column into a DataOuput.  This is used by insert operations
     * <P>
     * Write the column in its field format to the stream. Field format is a 
     * field header followed the data of the column as defined by the data 
     * itself.  See this class's description for the specifics of the header.
     *
     * @param slot      slot of the row with the column
     * @param recordId  record id of the
     * @param column    the object form of the column to log 
     * @param out       where to log to the column to.
     *
     * @exception StandardException	Standard Cloudscape error policy
     * @exception IOException	    I/O exception from writing to an array.
     *
     * @see BasePage#logColumn
     **/
	public int logLongColumn(
    int                     slot, 
    int                     recordId, 
    Object                  column, 
    DynamicByteArrayOutputStream  out)
		throws StandardException, IOException
	{
		int spaceAvailable = freeSpace;

		// need to account for the slot table using extra space...
		spaceAvailable -= slotEntrySize;

		// <= is ok here as we know we want to write at least one more byte
		if (spaceAvailable <= 0)
			throw new NoSpaceOnPage(isOverflowPage());

		setOutputStream(out);
		int beginPosition = out.getPosition();

		try 
        {
			// in the long column portion on the new page there will be 1 field
            // if the portion fits on the page (2 if it needs another pointer
            // to continue to yet another page).
			int numberFields = 1;

			StoredRecordHeader recordHeader = 
                new StoredRecordHeader(recordId, numberFields);

			int recordHeaderLength = recordHeader.write(logicalDataOut);

			spaceAvailable -= recordHeaderLength;

			if (spaceAvailable < 0)
            {
                // this part of long column won't totally fit on page, it
                // needs to be linked to another page.  Throw exception and
                // caller will handle logging an overflow column portion 
                // with a forward pointer.

				throw new NoSpaceOnPage(isOverflowPage());
            }
            else
            {
                // the rest of the long column fits on the page!

                Object[] row = new Object[1];
                row[0] = column;
                return logColumn(row, 0, out, spaceAvailable, COLUMN_LONG, 100);
            }

		} 
        finally 
        {
			resetOutputStream();
		}
	}

    /**
     * Log column from input row to the given output stream.
     * <p>
     * Read data from row[arrayPosition], and write the column data in
     * raw store page format to the given column.  Along the way determine
     * if the column will fit on the current page.
     * <p>
     * Action taken in this routine is determined by the kind of column as
     * specified in the columnFlag:
     *     COLUMN_NONE	 - the column is insignificant
     *     COLUMN_FIRST  - this is the first column in a logRow() call
     *     COLUMN_LONG   - this is a known long column, therefore we will 
     *                     store part of the column on the current page and 
     *                     overflow the rest if necessary.
     * <p>
     * Upon entry to this routine logicalDataOut is tied to the 
     * DynamicByteArrayOutputStream out.
     * <BR>
     * If a column is a long column and it does not totally fit on the current
     * page, then a LongColumnException is thrown.  We package up info about
     * the current long column in the partially filled in exception so that
     * callers can take correct action.  The column will now be set a as a
     * stream.
     *
	 * @return The spaceAvailable after accounting for space for this column.
     *
     * @param row           array of column from which to read the column from.
     * @param arrayPosition The array position of column to be reading from row.
     * @param out           The stream to write the raw store page format of the
     *                      the column to.
     * @param spaceAvailable    The number of bytes available on the page for
     *                          this column, this may differ from current page
     *                          as it may include bytes used by previous 
     *                          columns.
     * @param columnFlag    one of: COLUMN_NONE, COLUMN_FIRST, or COLUMN_LONG.
     *
	 * @exception  StandardException    Standard exception policy.
     * @exception  LongColumnException  Thrown if column will not fit on a 
     *                                  single page. See notes above
     **/
	private int logColumn(
    Object[]                row, 
    int                     arrayPosition,
    DynamicByteArrayOutputStream  out, 
    int                     spaceAvailable,
    int                     columnFlag, 
    int                     overflowThreshold)
		throws StandardException, IOException
	{
        // RESOLVE (mikem) - why will row be null?
		Object column = (row != null ? row[arrayPosition] : null);

		// Check to see if the data comes from a page, if it is, then the field
		// header is already formatted.
		if (column instanceof RawField)
        {
			// field data is raw, no need to set up a field header etc.

			byte[] data = ((RawField) column).getData();

			if (data.length <= spaceAvailable) 
            {
				out.write(data);
				spaceAvailable -= data.length;
			}
			return spaceAvailable;
		}

        // If this is a long column, it may fit in this page or it may not.
		boolean longColumnDone = true;


        // default field status.
		int fieldStatus =
            StoredFieldHeader.setFixed(StoredFieldHeader.setInitial(), true);

		int beginPosition       = out.getPosition();
		int columnBeginPosition = 0;
		int headerLength;
		int fieldDataLength     = 0;

		if (column instanceof StreamStorable)
        {
            StreamStorable  stream_storable_column = (StreamStorable) column;

            if (stream_storable_column.returnStream() != null)
            {
				column = 
                    (Object) stream_storable_column.returnStream();
            }
		}

		if (column == null)
        {
			fieldStatus  = StoredFieldHeader.setNonexistent(fieldStatus);
			headerLength =
                StoredFieldHeader.write(
                    logicalDataOut, fieldStatus, 
                    fieldDataLength, slotFieldSize);
		}
		else if (column instanceof InputStream)
        {
			RememberBytesInputStream bufferedIn = null;
			int                      bufferLen = 0;

			int estimatedMaxDataSize =
                getMaxDataLength(spaceAvailable, overflowThreshold);

			// if column is already instanceof RememberBytesInputStream, then we
            // need to find out how many bytes have already been stored in the 
            // buffer.
			if (column instanceof RememberBytesInputStream)
            {
				// data is already RememberBytesInputStream

				bufferedIn = (RememberBytesInputStream) column;
				bufferLen  = bufferedIn.numBytesSaved();

			} 
            else 
            {
				// data comes in as an inputstream
				bufferedIn = new RememberBytesInputStream(
                    (InputStream) column, new MemByteHolder(maxFieldSize + 1));

                // always set stream of InputStream to RememberBytesInputStream
                // so that all future access to this column will be able to
                // get at the bytes drained from the InputStream, and copied 
                // into the RememberBytesInputStream.
                if (row[arrayPosition] instanceof StreamStorable)
                    ((StreamStorable)row[arrayPosition]).setStream(bufferedIn);
			}

			// read the buffer by reading the max we can read.
			if (bufferLen < (estimatedMaxDataSize + 1))
            {
				bufferLen +=
                    bufferedIn.fillBuf(estimatedMaxDataSize + 1 - bufferLen);
            }

			if ((bufferLen <= estimatedMaxDataSize))
            {
				// we will be able to fit this into the page
                
				fieldDataLength = bufferLen;
				fieldStatus     = StoredFieldHeader.setFixed(fieldStatus, true);
				headerLength    = StoredFieldHeader.write(
                                        logicalDataOut, fieldStatus, 
                                        fieldDataLength, slotFieldSize);
	
				// if the field is extensible, then we write the serializable 
                // formatId.  if the field is non-extensible, we don't need to 
                // write the formatId.  but at this point, how do we know 
                // whether the field is extensible or not???  For Plato release,
                // we do not support InputStream on extensible types, 
				// therefore, we ignore the formatId for now.
				bufferedIn.putBuf(logicalDataOut, fieldDataLength);
			} 
            else
            {
                // current column will not fit into the current page.

				if (columnFlag == COLUMN_LONG)
                {
                    // column is a long column and the remaining portion does
                    // not fit on the current page.
					longColumnDone = false;
                   
					// it's a portion of a long column, and there is more to 
                    // write reserve enough room for overflow pointer, then 
                    // write as much data as we can leaving an extra 2 bytes
                    // for overflow field header.
					fieldDataLength =
                        estimatedMaxDataSize - OVERFLOW_POINTER_SIZE - 2;
					fieldStatus     =
                        StoredFieldHeader.setFixed(fieldStatus, true);

					headerLength    =
                        StoredFieldHeader.write(
                            logicalDataOut, fieldStatus, 
                            fieldDataLength, slotFieldSize);
					bufferedIn.putBuf(logicalDataOut, fieldDataLength);

					// now, we need to adjust the buffer, move the unread 
                    // bytes to the beginning position the cursor correctly,
					// so, next time around, we can read more into the buffer.
					int remainingBytes = bufferedIn.available();

					// move the unread bytes to the beginning of the byteHolder.
					int bytesShifted = bufferedIn.shiftToFront();

				} 
                else
                {
                    // column not a long column and does not fit on page.
					int delta = maxFieldSize - bufferLen + 1;

					if (delta > 0)
						bufferLen += bufferedIn.fillBuf(delta);
					fieldDataLength = bufferLen;

					// the data will not fit on this page make sure the new 
                    // input stream is passed back to the upper layer...
					column = (Object) bufferedIn;
				}
			}
		
		} 
        else if (column instanceof DataValueDescriptor)
        {
			DataValueDescriptor sColumn = (DataValueDescriptor) column;

			boolean isNull = sColumn.isNull();
			if (isNull) 
            {
				fieldStatus = StoredFieldHeader.setNull(fieldStatus, true);
			}

			// header is written with 0 length here.
			headerLength = 
                StoredFieldHeader.write(
                    logicalDataOut, fieldStatus, 
                    fieldDataLength, slotFieldSize);

			if (!isNull) 
            {
				// write the field data to the log 
				try 
                {
					columnBeginPosition = out.getPosition();
					sColumn.writeExternal(logicalDataOut);
				}
                catch (IOException ioe)
                {
                    // SQLData error reporting
                    if (logicalDataOut != null)
                    {
                        Exception ne = logicalDataOut.getNestedException();
                        if (ne != null)
                        {
                            if (ne instanceof StandardException)
                            {
                                throw (StandardException) ne;
                            }
                        }
                    }


					throw StandardException.newException(
                            SQLState.DATA_STORABLE_WRITE_EXCEPTION, ioe);
				}

				fieldDataLength =
                    (out.getPosition() - beginPosition) - headerLength;
			}
		}
        else if (column instanceof RecordHandle)
        {
			// we are inserting an overflow pointer for a long column

            // casted reference to column to avoid repeated casting.
			RecordHandle overflowHandle = (RecordHandle) column;

			fieldStatus     = StoredFieldHeader.setOverflow(fieldStatus, true);
			headerLength    = 
                StoredFieldHeader.write(
                    logicalDataOut, fieldStatus, 
                    fieldDataLength, slotFieldSize);

			fieldDataLength += 
                CompressedNumber.writeLong(out, overflowHandle.getPageNumber());
			fieldDataLength += 
                CompressedNumber.writeInt(out, overflowHandle.getId());

		} 
        else
        {
			// Serializable/Externalizable/Formattable
			// all look the same at this point.

			// header is written with 0 length here.
			headerLength = 
                StoredFieldHeader.write(
                    logicalDataOut, fieldStatus, 
                    fieldDataLength, slotFieldSize);

			logicalDataOut.writeObject(column);
			fieldDataLength = 
                (out.getPosition() - beginPosition) - headerLength;
		}

		// calculate the size of the field on page with compresed field header

		fieldStatus = StoredFieldHeader.setFixed(fieldStatus, false);
		int fieldSizeOnPage =
            StoredFieldHeader.size(fieldStatus, fieldDataLength, slotFieldSize)
            + fieldDataLength;

		userRowSize += fieldDataLength;

		boolean fieldIsLong = isLong(fieldSizeOnPage, overflowThreshold);
       
		// Do we have enough space on the page for this field?
		if (((spaceAvailable < fieldSizeOnPage) || (fieldIsLong)) &&
            (columnFlag != COLUMN_LONG)) 
        {
            // Column was not long before getting here and does not fit.

			if (fieldIsLong) 
            {
				// long column, and this first time we have figured out this
                // column is long.

				if (!(column instanceof InputStream)) 
                {
					// Convert already written object to an InputStream.
					ByteArray fieldData =
						new ByteArray(
                            ((DynamicByteArrayOutputStream)out).getByteArray(),
							(columnBeginPosition), fieldDataLength);

					ByteArrayInputStream columnIn =
						new ByteArrayInputStream(
                            fieldData.getArray(), columnBeginPosition, 
                            fieldDataLength);

					MemByteHolder byteHolder = 
                        new MemByteHolder(fieldDataLength + 1);

					RememberBytesInputStream bufferedIn = 
                        new RememberBytesInputStream(columnIn, byteHolder);
 
					// the data will not fit on this page make sure the new 
                    // input stream is passed back to the upper layer...
					column = bufferedIn;
				}

				out.setPosition(beginPosition);

                // This exception carries the information for the client
                // routine to continue inserting the long row on multiple
                // pages.
				LongColumnException lce = new LongColumnException();
				lce.setColumn(column);
				throw lce;

			} 
            else 
            {
				// Column does not fit on this page, but it isn't a long column.

				out.setPosition(beginPosition);
				return(spaceAvailable);
			}
		}
 
		// Now we go back to update the fieldDataLength in the field header
		out.setPosition(beginPosition);

		// slotFieldSize is set based on the pageSize.
		// We are borrowing this to set the size of our fieldDataLength.
		fieldStatus  = StoredFieldHeader.setFixed(fieldStatus, true);
		headerLength = StoredFieldHeader.write(
                            out, fieldStatus, fieldDataLength, slotFieldSize);

		// set position to the end of the field
		out.setPosition(beginPosition + fieldDataLength + headerLength);

		spaceAvailable -= fieldSizeOnPage;

		// YYZ: revisit
		if (columnFlag == COLUMN_LONG)
        {
			// if we are logging a long column, we don't care how much space 
            // is left on the page, instead, we care whether we are done with 
            // the column or not.  So, here, we want to return 1. if we are 
            // not done, and return -1 if we are done.
			// If logColumn returns -1, that flag is returned all the way to
			// BasePage.insertLongColumn to signal end of loop.
			if (longColumnDone)
				return -1;
			else
				return 1;
		} else
        {
			return (spaceAvailable);
        }
	}

    /**
     * Create and write a long row header to the log stream.
     * <p>
     * Called to log a new overflow record, will check for space available
     * and throw an exception if the record header will not fit on the page.
     * <p>
     *
	 * @return -1
     *
     * @param slot           slot of record to log.
     * @param spaceAvailable spaceAvaliable on page.
     * @param out            stream to log the record to.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private int logOverflowRecord(
    int                     slot, 
    int                     spaceAvailable, 
    DynamicByteArrayOutputStream  out)
		throws StandardException, IOException
	{
		setOutputStream(out);
		
		StoredRecordHeader pageRecordHeader = getHeaderAtSlot(slot);
				
        StoredRecordHeader  overflow_rh = getOverFlowRecordHeader();
        overflow_rh.setOverflowFields(pageRecordHeader);

		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(overflow_rh.getOverflowPage() != 0);
		}

		/*
		// #1 situation,
		// we want to update the header to just an overflow pointer with no data
		// so, update the recordHeader, and we are done...
		if (!overflow_rh.isPartialOverflow()) {
			// this recordHeader becomes just a overflow pointer,
			// we need to make sure that the number of fields is set to 0.
			overflow_rh.setNumberFields(0);
			
			spaceAvailable -= overflow_rh.write(logicalDataOut);

			if (spaceAvailable < 0) {
				throw new NoSpaceOnPage(isOverflowPage());
			}

			resetOutputStream();

			return (-1);
		}
		*/

		// #2 situation,
		// we want to only update the recordheader of the page, while leaving
        // the data of the record on the page.  Just update the header part and
        // then arrange for the data part to move to after the new header.

		int oldSize = pageRecordHeader.size();
		int newSize = overflow_rh.size();

		if (oldSize < newSize) 
        {
			// need extra room...
			int delta = newSize - oldSize;
			if (spaceAvailable < delta) 
            {
				throw new NoSpaceOnPage(isOverflowPage());
			}
		}

		// write the new overflow_rh for the record.
		overflow_rh.write(logicalDataOut);

		// now, log the data
		logRecordDataPortion(
            slot, LOG_RECORD_DEFAULT, pageRecordHeader, 
            (FormatableBitSet) null, logicalDataOut, (RecordHandle)null);

		return (-1);
	}

	private int logOverflowField(
    DynamicByteArrayOutputStream  out, 
    int                     spaceAvailable,
    long                    overflowPage, 
    int                     overflowId)
		throws StandardException, IOException
	{
		int fieldStatus = 
            StoredFieldHeader.setOverflow(
                StoredFieldHeader.setInitial(), true);

		int fieldSizeOnPage = 
            CompressedNumber.sizeLong(overflowPage) + 
            CompressedNumber.sizeInt(overflowId);

		int fieldDataLength = fieldSizeOnPage;

		fieldSizeOnPage += 
            StoredFieldHeader.size(fieldStatus, fieldDataLength, slotFieldSize);

		// need to check that we have room on the page for this.
		spaceAvailable -= fieldSizeOnPage;

		// what if there is not enough room for the overflow pointer?
		if (spaceAvailable < 0)
			throw new NoSpaceOnPage(isOverflowPage());

		// write the field to the page:
		StoredFieldHeader.write(
            logicalDataOut, fieldStatus, fieldDataLength, slotFieldSize);
		CompressedNumber.writeLong(out, overflowPage);
		CompressedNumber.writeInt(out, overflowId);

		// return the available bytes
		return(spaceAvailable);
	}

    /**
     * Log a record to the ObjectOutput stream.
     * <p>
     * Write out the complete on-page record to the store stream.  Data is 
     * preceeded by a  compressed int that gives the length of the following 
     * data.
     *
     * @exception StandardException	Standard Cloudscape error policy
     * @exception IOException	    on error writing to log stream.
     *
     * @see BasePage#logRecord
     **/
	public void logRecord(
    int             slot, 
    int             flag, 
    int             recordId, 
    FormatableBitSet         validColumns, 
    OutputStream    out, 
    RecordHandle    headRowHandle) 
		throws StandardException, IOException
	{
		StoredRecordHeader recordHeader = getHeaderAtSlot(slot);

		if (recordId != recordHeader.getId()) 
        {
			// the record is being logged under a different identifier,
			// write it out with the correct identifier
			StoredRecordHeader newRecordHeader = 
                new StoredRecordHeader(recordHeader);

			newRecordHeader.setId(recordId);

			newRecordHeader.write(out);
			newRecordHeader = null;
		} 
        else 
        {
			// write the original record header
			recordHeader.write(out);
		}

		logRecordDataPortion(
            slot, flag, recordHeader, validColumns, out, headRowHandle);

	}

	private void logRecordDataPortion(
    int                 slot, 
    int                 flag,
    StoredRecordHeader  recordHeader,
    FormatableBitSet             validColumns, 
    OutputStream        out,
    RecordHandle        headRowHandle) 
		throws StandardException, IOException
	{
		int offset = getRecordOffset(slot);

		// now skip over the original header before writing the data
		int oldHeaderLength = recordHeader.size();
		offset += oldHeaderLength;

		// write out the record data (FH+data+...) from the page data
		int startField = recordHeader.getFirstField();
		int endField = startField + recordHeader.getNumberFields();
		int validColumnsSize = (validColumns == null) ? 0 : validColumns.getLength();

		for (int fieldId = startField; fieldId < endField; fieldId++) {

			rawDataIn.setPosition(offset);

			// get the field header information from the page
			int fieldStatus = StoredFieldHeader.readStatus(rawDataIn);
			int fieldDataLength = StoredFieldHeader.readFieldDataLength(rawDataIn, fieldStatus, slotFieldSize);

			// see if this field needs to be logged
			// no need to write the data portion if the log is getting written
			// for purges unless the field is  overflow pointer for a long column.
			if (((validColumns != null) && !(validColumnsSize > fieldId && validColumns.isSet(fieldId))) || 
				((flag & BasePage.LOG_RECORD_FOR_PURGE)!=0 && !StoredFieldHeader.isOverflow(fieldStatus)))
			{
				// nope, move page offset along
				offset += StoredFieldHeader.size(fieldStatus, fieldDataLength, slotFieldSize);
				offset += fieldDataLength;

				// write a non-existent field
				fieldStatus = StoredFieldHeader.setInitial();
				fieldStatus = StoredFieldHeader.setNonexistent(fieldStatus);
				StoredFieldHeader.write(out, fieldStatus, 0, slotFieldSize);
				continue;
			}

			// If this field is to be updated, and it points to a long column
			// chain, the entire long column chain will be orphaned after the
			// update operation.  Therefore, need to queue up a post commit
			// work to reclaim the long column chain.  We cannot do any clean
			// up in this transaction now because we are underneath a log
			// action and cannot interrupt the transaction log buffer.
			// HeadRowHandle may be null if updateAtSlot is called to update a
			// non-head row piece.  In that case, don't do anything.
			// If temp container, don't do anything.
			if (((flag & BasePage.LOG_RECORD_FOR_UPDATE) != 0) && 
				headRowHandle != null &&
				StoredFieldHeader.isOverflow(fieldStatus) &&
				owner.isTemporaryContainer() == false)
			{

				int saveOffset = rawDataIn.getPosition(); // remember the page offset
				long overflowPage = CompressedNumber.readLong((InputStream) rawDataIn);
				int overflowId = CompressedNumber.readInt((InputStream) rawDataIn);

				// Remember the time stamp on the first page of the column
				// chain.  This is to prevent the case where the post commit
				// work gets fired twice, in that case, the second time it is
				// fired, this overflow page may not part of this row chain
				// that is being updated.
				Page firstPageOnColumnChain = getOverflowPage(overflowPage);
				PageTimeStamp ts = firstPageOnColumnChain.currentTimeStamp();
				firstPageOnColumnChain.unlatch();

				RawTransaction rxact = (RawTransaction)owner.getTransaction();

				ReclaimSpace work = 
					new ReclaimSpace(ReclaimSpace.COLUMN_CHAIN,
								headRowHandle,
								fieldId, // long column about to be orphaned by update 
								overflowPage, // page where the long column starts
								overflowId, // record Id of the beginning of the long column
								ts,
								rxact.getDataFactory(), true);

				rxact.addPostCommitWork(work);

				rawDataIn.setPosition(saveOffset); // Just to be safe, reset data stream
			}


			// write the field header for the log
			offset += StoredFieldHeader.write(out, fieldStatus, fieldDataLength, slotFieldSize);

			if (fieldDataLength != 0) {

				// write the actual data
				out.write(pageData, offset, fieldDataLength);

				offset += fieldDataLength;
			}
		}
	}

	/**
		Log a field to the ObjectOutput stream.
		<P>
		Find the field in the record and then write out the complete
		field, i.e. header and data.

		@exception StandardException	Standard Cloudscape error policy
		@exception IOException			RESOLVE

		@see BasePage#logField
	*/

	public void logField(int slot, int fieldNumber, OutputStream out)
		throws StandardException, IOException
	{
		int offset = getFieldOffset(slot, fieldNumber);

		// these reads are always against the page array
		ArrayInputStream lrdi = rawDataIn;

		// now write out the field we are interested in ...
		lrdi.setPosition(offset);
		int fieldStatus = StoredFieldHeader.readStatus(lrdi);
		int fieldDataLength = StoredFieldHeader.readFieldDataLength(lrdi, fieldStatus, slotFieldSize);

		StoredFieldHeader.write(out, fieldStatus, fieldDataLength, slotFieldSize);
		
		if (fieldDataLength != 0) {
			// and then the data
			out.write(pageData, lrdi.getPosition(), fieldDataLength);
		}
	}

	/*
	** Overidden methods of BasePage
	*/

	/**
		Override insertAtSlot to provide long row support.
		@exception StandardException Standard Cloudscape error policy
	*/
	public RecordHandle insertAtSlot(
    int                   slot, 
    Object[] row, 
    FormatableBitSet               validColumns,
    LogicalUndo           undo, 
    byte                  insertFlag, 
    int                   overflowThreshold)
		throws StandardException
	{
		try {

			return super.insertAtSlot(slot, row, validColumns, undo, insertFlag, overflowThreshold);

		} catch (NoSpaceOnPage nsop) {

			// Super class already handle the case of insert that allows overflow.
			// If we get here, we know that the insert should not allow overflow.
			// Possibles causes:
			// 1.	insert to an empty page, row will never fit (ie long row)
			// 2.	insert to original page
			// we will do:
			// return a null to indicate the insert cannot be accepted ..
			return null;

		}
	}
	

	/**
		Update field at specified slot
		@exception StandardException Standard Cloudscape error policy
	*/
	public RecordHandle updateFieldAtSlot(
    int                 slot, 
    int                 fieldId, 
    Object newValue, 
    LogicalUndo         undo)
		throws StandardException
	{
		try {

			return super.updateFieldAtSlot(slot, fieldId, newValue, undo);

		} catch (NoSpaceOnPage nsop) {


			// empty page apart from the record
			if (slotsInUse == 1) 
            {
				throw StandardException.newException(
                    SQLState.DATA_NO_SPACE_FOR_RECORD);
			}
            throw StandardException.newException(
                    SQLState.DATA_NO_SPACE_FOR_RECORD);

/*
// djd			if (isOverflowPage()) {
			}

			return XXX;
*/
		}

	}

	/**
		Get the number of fields on the row at slot
		@exception StandardException Standard Cloudscape error policy
	*/
	public int fetchNumFieldsAtSlot(int slot) throws StandardException
	{

		StoredRecordHeader recordHeader = getHeaderAtSlot(slot);

		if (!recordHeader.hasOverflow())
			return super.fetchNumFieldsAtSlot(slot);

		BasePage overflowPage = getOverflowPage(recordHeader.getOverflowPage());
		int count = overflowPage.fetchNumFieldsAtSlot(getOverflowSlot(overflowPage, recordHeader));
		overflowPage.unlatch();
		return count;
	}

	/*
	 * methods that is called underneath a page action
	 */

	/*
	 * update page version and instance due to actions by a log record
	 */
	public void logAction(LogInstant instant) throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		if (rawDataOut == null)
			createOutStreams();

		if (!isActuallyDirty()) {
			// if this is not an overflow page and the page is valid, set the
			// initial row count.
			if (!isOverflowPage() && ((getPageStatus() & VALID_PAGE) != 0)) {
				initialRowCount = internalNonDeletedRecordCount();
			} else
				initialRowCount = 0;
		}

		setDirty();

		bumpPageVersion();
		updateLastLogInstant(instant);
	}


	/* clean the page for first use or reuse */
	private void cleanPage()
	{
		setDirty();

		// set pageData to all nulls
		clearSection(0, getPageSize());

		slotsInUse = 0;
		deletedRowCount = 0;
		headerOutOfDate = true;	// headerOutOfDate must be set after setDirty
								// because isDirty maybe called unlatched

		clearAllSpace();

	}

	/**
		Initialize the page.  

		If reuse, then 
		Clean up any in memory or on disk structure to ready the page for reuse.
		This is not only reusing the page buffer, but reusing a free page 
		which may or may not be cleaned up the the client of raw store when it 
        was deallocated.

		@exception StandardException Cloudscape Standard Error Policy
	 */
	public void initPage(LogInstant instant, byte status, int recordId, 
						 boolean overflow, boolean reuse)
		 throws StandardException
	{
		// log action at the end after the page is updated with all the
		// pertinent information
		logAction(instant);

		if (reuse)
		{
			cleanPage();
			super.cleanPageForReuse();
		}
		// if not reuse, createPage already called cleanpage

		headerOutOfDate = true;	// headerOutOfDate must be set after setDirty
								// because isDirty maybe called unlatched
		setPageStatus(status);
		isOverflowPage = overflow;
		nextId = recordId;

	}

	/**
		Set page status
		@exception StandardException Cloudscape Standard Error Policy
	*/
	public void setPageStatus(LogInstant instant, byte status)
		 throws StandardException
	{
		logAction(instant);
		headerOutOfDate = true;	// headerOutOfDate must be set after setDirty
								// because isDirty maybe called unlatched

		setPageStatus(status);
	}

	/**
		Set the row reserved space.
		@exception StandardException Cloudscape Standard Error Policy
	 */
	public void setReservedSpace(LogInstant instant, int slot, int value)
		 throws StandardException, IOException
	{
		logAction(instant);
		headerOutOfDate = true;	// headerOutOfDate must be set after setDirty
								// because isDirty maybe called unlatched

		int delta = value - getReservedCount(slot);

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(delta <= freeSpace, 
				"Cannot grow reserved space because there is not enough free space on the page");
			SanityManager.ASSERT(delta != 0,
				"Set Reserved Space called to set identical value");

            if (value < 0)
                SanityManager.THROWASSERT(
                    "Cannot set reserved space to value " + value);
		}

		// Find the end of the record that we are about to add or subtract from
		// the reserved space.
		int nextRecordOffset = getRecordOffset(slot) + getTotalSpace(slot);

		if (delta > 0) {
			// Growing - hopefully during a RRR restore
			expandPage(nextRecordOffset, delta);
		} else	{
			// shrinking, delta is < 0
			shrinkPage(nextRecordOffset, -delta);
		}

		// Lastly, update the reserved space count in the slot.
		rawDataOut.setPosition(getSlotOffset(slot) + (2*slotFieldSize));
		if (slotFieldSize == SMALL_SLOT_SIZE)
			logicalDataOut.writeShort(value);
		else
			logicalDataOut.writeInt(value);

	}


	/**
		Store a record at the given slot.

		@exception StandardException	Standard Cloudscape error policy
		@exception IOException			RESOLVE
	*/
	public void storeRecord(LogInstant instant, int slot, boolean insert, ObjectInput in)
		throws StandardException, IOException
	{
		logAction(instant);

		if (insert)
			storeRecordForInsert(slot, in);
		else
			storeRecordForUpdate(slot, in);
	}

	private void storeRecordForInsert(int slot, ObjectInput in)
		throws StandardException, IOException
	{

		StoredRecordHeader recordHeader = shiftUp(slot);
		if (recordHeader == null) {
			recordHeader = new StoredRecordHeader();
			setHeaderAtSlot(slot, recordHeader);
		}

		bumpRecordCount(1);

		// recordHeader represents the new version of the record header.
		recordHeader.read(in);

		// the record is already marked delete, we need to bump the deletedRowCount
		if (recordHeader.isDeleted()) {
			deletedRowCount++;
			headerOutOfDate = true;
		}

		// during a rollforward insert, recordId == nextId
		// during a rollback of purge, recordId < nextId
		if (nextId <= recordHeader.getId())
			nextId = recordHeader.getId()+1;

		int recordOffset = firstFreeByte;
		int offset = recordOffset;

		// write each field out to the page
		int numberFields = recordHeader.getNumberFields();

		rawDataOut.setPosition(offset);
		offset += recordHeader.write(rawDataOut);

		int userData = 0;
		for (int i = 0; i < numberFields; i++) {

			// get the field header information, the input stream came from the log 
			int newFieldStatus = StoredFieldHeader.readStatus(in);
			int newFieldDataLength = StoredFieldHeader.readFieldDataLength(in, newFieldStatus, slotFieldSize);
			newFieldStatus = StoredFieldHeader.setFixed(newFieldStatus, false);

			rawDataOut.setPosition(offset);
			offset += StoredFieldHeader.write(rawDataOut, newFieldStatus, newFieldDataLength, slotFieldSize);

			if (newFieldDataLength != 0) {
				in.readFully(pageData, offset, newFieldDataLength);
				offset += newFieldDataLength;
				userData += newFieldDataLength;
			}
		}

		int dataWritten = offset - firstFreeByte;

		freeSpace -= dataWritten;
		firstFreeByte += dataWritten;

		int reservedSpace = 0;
		if (minimumRecordSize > 0) {

			// make sure we reserve the minimumRecordSize for the user data 
			// portion of the record excluding the space we took on recordHeader 
			// and fieldHeaders.
			if (userData < minimumRecordSize) {
				reservedSpace = minimumRecordSize - userData;
				freeSpace -= reservedSpace;
				firstFreeByte += reservedSpace;
			}
		}

		// update the slot table
		addSlotEntry(slot, recordOffset, dataWritten, reservedSpace);

        if (SanityManager.DEBUG)
        {
            if ((firstFreeByte > getSlotOffset(slot)) ||
                (freeSpace < 0))
            {
                SanityManager.THROWASSERT(
                        " firstFreeByte = " + firstFreeByte + 
                        " dataWritten = "        + dataWritten        +
                        " getSlotOffset(slot) = "   + getSlotOffset(slot)   + 
						" slot = " + slot +
                        " firstFreeByte = "      + firstFreeByte + 
                        " freeSpace = "          + freeSpace  + 
                        " page = "               + this);
            }
        }

		if ((firstFreeByte > getSlotOffset(slot)) || (freeSpace < 0))
        {
			throw dataFactory.markCorrupt(
                StandardException.newException(
                    SQLState.DATA_CORRUPT_PAGE, getPageId()));
        }

	}


	private void storeRecordForUpdate(int slot, ObjectInput in)
		throws StandardException, IOException
	{
		// set up to read the in-memory record header back from the record
		StoredRecordHeader recordHeader = getHeaderAtSlot(slot);
		StoredRecordHeader newRecorderHeader = new StoredRecordHeader();

		// recordHeader represents the new version of the record header.
		newRecorderHeader.read(in);

		int oldFieldCount = recordHeader.getNumberFields();
		int newFieldCount = newRecorderHeader.getNumberFields();

		int startField = recordHeader.getFirstField();
		if (SanityManager.DEBUG) {
			if (startField != newRecorderHeader.getFirstField())
				SanityManager.THROWASSERT("First field changed from " + startField + " to " + newRecorderHeader.getFirstField());
		}

		// See if the number of fields shrunk, if so clear out the old data
		// we do this first to stop shuffling about the fields that are going to
		// be deleted during the update of the earlier fields. This case occurs
		// on an update that changes the row to be overflowed.
		if (newFieldCount < oldFieldCount) {

			int oldDataStartingOffset = getFieldOffset(slot, startField + newFieldCount);

			// calculate the length of the to be deleted fields
			int deleteLength = getRecordOffset(slot) + getRecordPortionLength(slot) - oldDataStartingOffset;

			// we are updateing to zero bytes!
			updateRecordPortionLength(slot, -(deleteLength), deleteLength);
		}

		// write each field out to the page

		int startingOffset = getRecordOffset(slot);
		int newOffset = startingOffset;
		int oldOffset = startingOffset;

		// see which field we get to use the reserve space
		int reservedSpaceFieldId = newFieldCount < oldFieldCount ?
			newFieldCount - 1 : oldFieldCount - 1;
		reservedSpaceFieldId += startField;


		// the new data the needs to be written at newOffset but can't until
		// unsedSpace >= newDataToWrite.length (allowing for the header)
		DynamicByteArrayOutputStream newDataToWrite = null;

		rawDataOut.setPosition(newOffset);

		// write the record header, which may change in size
		int oldLength = recordHeader.size();
		int newLength = newRecorderHeader.size();

		int unusedSpace = oldLength; // the unused space at newOffset

		// no fields, so we can eat into the reserve space
		if (reservedSpaceFieldId < startField) // no fields
			unusedSpace += getReservedCount(slot);

		if (unusedSpace >= newLength) {
			newRecorderHeader.write(rawDataOut);
			newOffset += newLength;
			unusedSpace -= newLength;
			
		} else {			

			newDataToWrite = new DynamicByteArrayOutputStream(getPageSize());
			newRecorderHeader.write(newDataToWrite);
		}
		oldOffset += oldLength;
		int recordDelta = (newLength - oldLength);

		int oldFieldStatus = 0;
		int oldFieldDataLength = 0;
		int newFieldStatus = 0;
		int newFieldDataLength = 0;

		int oldEndFieldExclusive = startField + oldFieldCount;
		int newEndFieldExclusive = startField + newFieldCount;

		for (int fieldId = startField; fieldId < newEndFieldExclusive; fieldId++) {

			int oldFieldLength = 0;
			if (fieldId < oldEndFieldExclusive) {
				rawDataIn.setPosition(oldOffset);
				oldFieldStatus = StoredFieldHeader.readStatus(rawDataIn);
				oldFieldDataLength = StoredFieldHeader.readFieldDataLength(rawDataIn, oldFieldStatus, slotFieldSize);
				oldFieldLength = StoredFieldHeader.size(oldFieldStatus, oldFieldDataLength, slotFieldSize)
					+ oldFieldDataLength;
			}

			newFieldStatus = StoredFieldHeader.readStatus(in);
			newFieldDataLength = StoredFieldHeader.readFieldDataLength(in, newFieldStatus, slotFieldSize);

			// if no value was provided on an update of a field then use the old value,
			// unless the old field didn't exist.
			if (StoredFieldHeader.isNonexistent(newFieldStatus) && (fieldId < oldEndFieldExclusive)) {

				// may need to move this old field ...
				if ((newDataToWrite == null) || (newDataToWrite.getUsed() == 0)) {
					// the is no old data to catch up on, is the data at
					// the correct position already?
					if (newOffset == oldOffset) {
						// yes, nothing to do!!
						if (SanityManager.DEBUG) {
							if (unusedSpace != 0)
							SanityManager.THROWASSERT("Unused space is out of sync, expect 0 got " + unusedSpace);
						}
					} else {
						// need to shift the field left
						if (SanityManager.DEBUG) {
							if (unusedSpace != (oldOffset - newOffset))
							SanityManager.THROWASSERT(
								"Unused space is out of sync expected " + (oldOffset - newOffset) + " got " + unusedSpace);
						}

						System.arraycopy(pageData, oldOffset, pageData, newOffset, oldFieldLength);
					}
					newOffset += oldFieldLength;

					// last field to be updated can eat into the reserve space
					if (fieldId == reservedSpaceFieldId)
						unusedSpace += getReservedCount(slot);

				} else {
					// there is data still to be written, just append this field to the
					// saved data
					int position = newDataToWrite.getPosition();
					newDataToWrite.setPosition(position + oldFieldLength);
					System.arraycopy(pageData, oldOffset,
						newDataToWrite.getByteArray(), position, oldFieldLength);

					unusedSpace += oldFieldLength;

					// last field to be updated can eat into the reserve space
					if (fieldId == reservedSpaceFieldId)
						unusedSpace += getReservedCount(slot);

					// attempt to write out some of what we have in the side buffer now.
					int copyLength = moveSavedDataToPage(newDataToWrite, unusedSpace, newOffset);
					newOffset += copyLength;
					unusedSpace -= copyLength;

				}
				oldOffset += oldFieldLength;
				continue;
			}

			newFieldStatus = StoredFieldHeader.setFixed(newFieldStatus, false);

			int newFieldHeaderLength = StoredFieldHeader.size(newFieldStatus, newFieldDataLength, slotFieldSize);
			int newFieldLength = newFieldHeaderLength + newFieldDataLength;

			recordDelta += (newFieldLength - oldFieldLength);

			// See if we can write this field now

			// space available increases by the amount of the old field
			unusedSpace += oldFieldLength;
			oldOffset += oldFieldLength;

			// last field to be updated can eat into the reserve space
			if (fieldId == reservedSpaceFieldId)
				unusedSpace += getReservedCount(slot);

			if ((newDataToWrite != null) && (newDataToWrite.getUsed() != 0)) {

				// catch up on the old data if possible
				int copyLength = moveSavedDataToPage(newDataToWrite, unusedSpace, newOffset);
				newOffset += copyLength;
				unusedSpace -= copyLength;
			}

			if (((newDataToWrite == null) || (newDataToWrite.getUsed() == 0))
				&& (unusedSpace >= newFieldHeaderLength)) {

				// can fit the header in
				rawDataOut.setPosition(newOffset);
				newOffset += StoredFieldHeader.write(rawDataOut, newFieldStatus, newFieldDataLength, slotFieldSize);
				unusedSpace -= newFieldHeaderLength;

				if (newFieldDataLength != 0) {

					// read as much as the field as possible
					int fieldCopy = unusedSpace >= newFieldDataLength ?
							newFieldDataLength : unusedSpace;

					if (fieldCopy != 0) {
						in.readFully(pageData, newOffset, fieldCopy);

						newOffset += fieldCopy;
						unusedSpace -= fieldCopy;
					}


					fieldCopy = newFieldDataLength - fieldCopy;
					if (fieldCopy != 0) {
						if (newDataToWrite == null)
							newDataToWrite = new DynamicByteArrayOutputStream(newFieldLength * 2);

						// append the remaining portion of the field to the saved data
						int position = newDataToWrite.getPosition();
						newDataToWrite.setPosition(position + fieldCopy);
						in.readFully(newDataToWrite.getByteArray(),
								position, fieldCopy);

					}
				}
			} else {
				// can't fit these header, or therefore the field, append it
				// to the buffer.

				if (newDataToWrite == null)
					newDataToWrite = new DynamicByteArrayOutputStream(newFieldLength * 2);

				StoredFieldHeader.write(newDataToWrite, newFieldStatus, newFieldDataLength, slotFieldSize);

				// save the new field data
				if (newFieldDataLength != 0) {
					int position = newDataToWrite.getPosition();
					newDataToWrite.setPosition(position + newFieldDataLength);
					in.readFully(newDataToWrite.getByteArray(),
								position, newFieldDataLength);
				}
			}
		}

		// at this point there may still be data left in the saved buffer
		// but presumably we can't fit it in

		int reservedDelta;

		if ((newDataToWrite != null) && (newDataToWrite.getUsed() != 0)) {

			// need to shift the later records down ...
			int nextRecordOffset = startingOffset + getTotalSpace(slot);

			int spaceRequiredFromFreeSpace = newDataToWrite.getUsed() - (nextRecordOffset - newOffset);

			if (SanityManager.DEBUG) {
				if (newOffset > nextRecordOffset)
					SanityManager.THROWASSERT("data has overwritten next record - offset " + newOffset
							+ " next record " + nextRecordOffset);

				if ((spaceRequiredFromFreeSpace <= 0) || (spaceRequiredFromFreeSpace > freeSpace))
					SanityManager.THROWASSERT("invalid space required " + spaceRequiredFromFreeSpace
					+ " newDataToWrite.getUsed() " + newDataToWrite.getUsed()
					+ " nextRecordOffset " + nextRecordOffset
					+ " newOffset " + newOffset
					+ " reservedSpaceFieldId " + reservedSpaceFieldId
					+ " startField " + startField
					+ " newEndFieldExclusive " + newEndFieldExclusive
					+ " newFieldCount " + newFieldCount
					+ " oldFieldCount " + oldFieldCount
					+ " slot " + slot
					+ " freeSpace " + freeSpace
					+ " unusedSpace " + unusedSpace
					+ " page " + getPageId());


				if ((getReservedCount(slot) + spaceRequiredFromFreeSpace) != recordDelta)
					SanityManager.THROWASSERT("mismatch on count: reserved " + getReservedCount(slot) +
						"free space take " + spaceRequiredFromFreeSpace +
						"record delta " + recordDelta);

			}

			if (spaceRequiredFromFreeSpace > freeSpace) {
				throw dataFactory.markCorrupt(
                    StandardException.newException(
                        SQLState.DATA_CORRUPT_PAGE, getPageId()));
			}

			// see if this is the last record on the page, if so a simple
			// shift of the remaining fields will sufice...
			expandPage(nextRecordOffset, spaceRequiredFromFreeSpace);

			unusedSpace += spaceRequiredFromFreeSpace;

			moveSavedDataToPage(newDataToWrite, unusedSpace, newOffset);

			reservedDelta = -1 * getReservedCount(slot);

			if (SanityManager.DEBUG) {
				if (newDataToWrite.getUsed() != 0)
					SanityManager.THROWASSERT("data is left in save buffer ... " + newDataToWrite.getUsed());
			}
		} else {
			reservedDelta = -1 * recordDelta;
		}

		// now reset the length in the slot entry
		updateRecordPortionLength(slot, recordDelta, reservedDelta);

		setHeaderAtSlot(slot, newRecorderHeader);
	}

	private int moveSavedDataToPage(DynamicByteArrayOutputStream savedData, int unusedSpace, int pageOffset) {
		// catch up on the old data if possible
		if (unusedSpace > (savedData.getUsed() / 2)) {
			// copy onto the page
			int copyLength = unusedSpace <= savedData.getUsed() ?
							unusedSpace : savedData.getUsed();
			System.arraycopy(savedData.getByteArray(), 0,
				pageData, pageOffset, copyLength);

			// fix up the saved buffer
			savedData.discardLeft(copyLength);

			return copyLength;
		}

		return 0;
	}


	/**
		Create the space to update a portion of a record.
		This method ensures there is enough room to replace the
		old data of length oldLength at the given offset, with the new data of length
		newLength. This method does put any new data on the page, it moves old data around
		and zeros out any old data when newLength < oldLength. This method does
		update the information in the slot table.

		The passed in offset is the correct place to put the data
		when this method returns, ie. it only moves data that
		has an offset greater then this.

		@exception StandardException	Standard Cloudscape error policy
		@exception IOException			RESOLVE
	*/
	private void createSpaceForUpdate(int slot, int offset, int oldLength, int newLength)
		throws StandardException, IOException
	{

		// now replace the old data with the new data
		if (newLength <= oldLength) {

			// now shift the remaining data down ...
			int diffLength = oldLength - newLength;

			// real easy
			if (diffLength == 0)
				return;

			// shift the remaing fields down
			int remainingLength = 
                shiftRemainingData(slot, offset, oldLength, newLength);

			// clear the now unused data on the page
			clearSection(offset + newLength + remainingLength, diffLength);

			if (SanityManager.DEBUG) {

                if ((getRecordPortionLength(slot) - diffLength) != 
					((offset - getRecordOffset(slot)) + newLength + 
                      remainingLength))
                {
                    SanityManager.THROWASSERT(
                        " Slot table trying to update record length " + 
                        (getRecordPortionLength(slot) - diffLength) +
                        " that is not the same as what it actully is");
                }
			}

			// now reset the length in the slot entry, increase the reserved space
			updateRecordPortionLength(slot, -(diffLength), diffLength);
			return;
		}

		// tough case, the new field is bigger than the old field ... 
		// first attempt, see how much space is in row private reserved space

		int extraLength = newLength - oldLength; 

        // extraLength is always greater than 0.
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(extraLength > 0);

		int recordReservedSpace = getReservedCount(slot);
		int reservedDelta = 0;

		int spaceRequiredFromFreeSpace = extraLength - recordReservedSpace;

		if (SanityManager.DEBUG) {
			if (spaceRequiredFromFreeSpace > freeSpace)
				SanityManager.THROWASSERT(
                	"spaceRequiredFromFreeSpace = " +
						spaceRequiredFromFreeSpace +
                	";freeSpace = "                 + freeSpace     +
                	";newLength = "                 + newLength     +
                	";oldLength = "                 + oldLength     +
                	";\npage= "                     + this);
		}

		if (spaceRequiredFromFreeSpace > 0) {
            // The update requires all the reserved space + some from free space
            
			int nextRecordOffset = getRecordOffset(slot) + getTotalSpace(slot);

			// see if this is the last record on the page, if so a simple
			// shift of the remaining fields will sufice...
			expandPage(nextRecordOffset, spaceRequiredFromFreeSpace);

			// we used all the reserved space we have, set it to 0
			reservedDelta = -(recordReservedSpace);
		} else {
            // the update uses some amount of space from the rows reserved space

			// set reserved Delta to account for amount of reserved space used.
			reservedDelta = -(extraLength);
		}
		
		// just shift all remaining fields up
		int remainingLength = shiftRemainingData(slot, offset, oldLength, newLength);
	
		if (SanityManager.DEBUG) {
			if ((extraLength + reservedDelta) < 0)
				SanityManager.THROWASSERT(
					"total space the record occupies cannot shrink, extraLength = "
					+ extraLength + " reservedDelta = " + reservedDelta
					+ " spacerequired = " + spaceRequiredFromFreeSpace
					+ " recordReservedSpace = " + recordReservedSpace);
		}

		// now reset the length in the slot entry
		updateRecordPortionLength(slot, extraLength, reservedDelta);
	}

	/**
		storeField

		@exception StandardException	Standard Cloudscape error policy
		@exception IOException			RESOLVE
	*/
	public void storeField(LogInstant instant, int slot, int fieldNumber, ObjectInput in)
		throws StandardException, IOException
	{
		logAction(instant);

		int offset = getFieldOffset(slot, fieldNumber);

		// get the field header information, the input stream came from the log
		ArrayInputStream lrdi = rawDataIn;
		lrdi.setPosition(offset);
		int oldFieldStatus = StoredFieldHeader.readStatus(lrdi);
		int oldFieldDataLength = StoredFieldHeader.readFieldDataLength(lrdi, oldFieldStatus, slotFieldSize);

		int newFieldStatus = StoredFieldHeader.readStatus(in);
		int newFieldDataLength = StoredFieldHeader.readFieldDataLength(in, newFieldStatus, slotFieldSize);
		newFieldStatus = StoredFieldHeader.setFixed(newFieldStatus, false);

		int oldFieldLength = StoredFieldHeader.size(oldFieldStatus, oldFieldDataLength, slotFieldSize) + oldFieldDataLength;
		int newFieldLength = StoredFieldHeader.size(newFieldStatus, newFieldDataLength, slotFieldSize) + newFieldDataLength;

		createSpaceForUpdate(slot, offset, oldFieldLength, newFieldLength);
		
		rawDataOut.setPosition(offset);
		offset += StoredFieldHeader.write(rawDataOut, newFieldStatus, newFieldDataLength, slotFieldSize);

		if (newFieldDataLength != 0)
			in.readFully(pageData, offset, newFieldDataLength);
	}

	/**
		reserveSpaceForSlot
		This method will reserve at least specified "spaceToReserve" bytes for the record
		in the slot.

		@exception StandardException	Standard Cloudscape error policy
		@exception IOException			RESOLVE
	*/
	public void reserveSpaceForSlot(LogInstant instant, int slot, int spaceToReserve)
		throws StandardException, IOException
	{
		logAction(instant);

		int extraSpace = spaceToReserve - getReservedCount(slot);
		if (extraSpace <= 0)
			return;

		if (freeSpace < extraSpace)
			throw new NoSpaceOnPage(isOverflowPage());

		// need to shift the later records down ...
		int startingOffset = getRecordOffset(slot);
		int nextRecordOffset = startingOffset + getTotalSpace(slot);

		// see if this is the last record on the page, if so a simple
		// shift of the remaining fields will sufice...
		expandPage(nextRecordOffset, extraSpace);

		setSlotEntry(slot, startingOffset, getRecordPortionLength(slot), spaceToReserve);
	}

	/**
		Skip a field header and its data on the given stream.
		
		@exception IOException corrupt stream
	*/
	public void skipField(ObjectInput in) throws IOException {


		int fieldStatus = StoredFieldHeader.readStatus(in);
		int fieldDataLength = StoredFieldHeader.readFieldDataLength(in, fieldStatus, slotFieldSize);

		if (fieldDataLength != 0) {
			in.skipBytes(fieldDataLength);
		}
	}

	public void skipRecord(ObjectInput in) throws IOException
	{

		StoredRecordHeader recordHeader = new StoredRecordHeader();
		recordHeader.read(in);

		for (int i = recordHeader.getNumberFields(); i > 0; i--) {
			skipField(in);		
		}
	}

	/**
		Shift data within a record to account for an update.

		@param offset  Offset where the update starts, need not be on a field boundry.
		@param oldLenght length of the data being replaced
		@param newLength length of the data replacing the old data

		@return the length of the data in the record after the replaced data.
	*/
	private int shiftRemainingData(int slot, int offset, int oldLength, int newLength) 
		throws IOException
	{

		// length of valid data remaining in the record after the portion that
		// is being replaced.
		int remainingLength = (getRecordOffset(slot) + getRecordPortionLength(slot)) - 
											(offset + oldLength);

		if (SanityManager.DEBUG) {

            if (!(((remainingLength >= 0) && 
                   (getRecordPortionLength(slot) >= oldLength))))
            {
                SanityManager.THROWASSERT(
                    "oldLength = " + oldLength + " newLength = " + newLength + 
                    "remainingLength = " + remainingLength + 
                    " offset = " + offset + 
                    " getRecordOffset(" + slot + ") = " + getRecordOffset(slot)+
                    " getRecordPortionLength(" + slot + ") = " + 
                        getRecordPortionLength(slot));
            }
		}

		if (remainingLength != 0) {
			System.arraycopy(pageData, offset + oldLength,
							 pageData, offset + newLength, remainingLength);
		}

		return remainingLength;

	}

	/**
		Set the deleted status

		@exception StandardException	Standard Cloudscape error policy
		@exception IOException			RESOLVE
		@see BasePage#setDeleteStatus
	*/
	public void setDeleteStatus(LogInstant instant, int slot, boolean delete)
		throws StandardException, IOException 
	{

		logAction(instant);

		deletedRowCount += super.setDeleteStatus(slot, delete);
		headerOutOfDate = true;

		int offset = getRecordOffset(slot);
		StoredRecordHeader recordHeader = getHeaderAtSlot(slot);

		rawDataOut.setPosition(offset);
		recordHeader.write(logicalDataOut);
	}

	/**
		get record count without checking for latch
	*/
	protected int internalDeletedRecordCount()
	{
		return deletedRowCount;
	}

	/**
		purgeRecord from page.  Move following slots up by one.

		@exception StandardException	Standard Cloudscape error policy
		@exception IOException			RESOLVE
	*/
	public void purgeRecord(LogInstant instant, int slot, int recordId)
		throws StandardException, IOException 
	{

		logAction(instant);

		// if record is marked deleted, reduce deletedRowCount
		if (getHeaderAtSlot(slot).isDeleted())
			deletedRowCount--;

		int startByte = getRecordOffset(slot);
		int endByte = startByte + getTotalSpace(slot) - 1;

		compressPage(startByte, endByte);
		
		// fix up the on-page slot table
		removeSlotEntry(slot);

		// fix up the in-memory version
		removeAndShiftDown(slot);
	}

	/*
	**
	*/

	/**
		Get the offset of the field header of the given field for
		the record in the given slot.

		Field number is the absolute number for the complete record, not just this portion.
		E.g. if this is a record portion that starts at field 3 and has 6 fields
		then the second field on this *page* has field number 4.
	*/
	private int getFieldOffset(int slot, int fieldNumber) throws IOException
	{
		// RESOLVE - overflow, needs to be changed
		int offset = getRecordOffset(slot);

		StoredRecordHeader recordHeader = getHeaderAtSlot(slot);

		// get the number of fields
		int startField = recordHeader.getFirstField();

		if (SanityManager.DEBUG) {
			int numberFields = recordHeader.getNumberFields();

			if ((fieldNumber < startField) || (fieldNumber >= (startField + numberFields)))
				SanityManager.THROWASSERT(
					"fieldNumber: " + fieldNumber +
					" start field: " + startField +
					" number of fields " + numberFields);
		}

		ArrayInputStream lrdi = rawDataIn;

		// skip the record header
		lrdi.setPosition(offset + recordHeader.size());

		// skip any earlier fields ...
		for (int i = startField; i < fieldNumber; i++) {
			skipField(lrdi);
		}

		return rawDataIn.getPosition();
	}


	/*
	 * Time stamp support - this page supports time stamp
	 */

	/**
		Get a time stamp for this page
		@return page time stamp
	*/		
	public PageTimeStamp currentTimeStamp()
	{
		// saving the whole key would be an overkill
		return new PageVersion(getPageNumber(), getPageVersion());
	}

	/**
		Set given pageVersion to be the as what is on this page
	  
		@exception StandardException given time stamp is null or is not a time
		stamp implementation this page knows how to deal with
	*/
	public void setTimeStamp(PageTimeStamp ts) throws StandardException
	{
		if (ts == null)
        {
			throw StandardException.newException(SQLState.DATA_TIME_STAMP_NULL);
        }

		if (!(ts instanceof PageVersion))
        {
			throw StandardException.newException(
                SQLState.DATA_TIME_STAMP_ILLEGAL, ts);
        }

		PageVersion pv = (PageVersion)ts;

		pv.setPageNumber(getPageNumber());
		pv.setPageVersion(getPageVersion());
	}

	/**
		compare given PageVersion with pageVersion on page

		@param ts the page version gotton from this page via a currentTimeStamp
				or setTimeStamp call earlier

		@return true if the same
		@exception StandardException given time stamp not gotton from this page
	*/
	public boolean equalTimeStamp(PageTimeStamp ts) throws StandardException
	{
		if (ts == null)
			return false;

		if (!(ts instanceof PageVersion))
        {
			throw StandardException.newException(
                SQLState.DATA_TIME_STAMP_ILLEGAL, ts);
        }

		PageVersion pv = (PageVersion)ts;

		if (pv.getPageNumber() != getPageNumber())
        {
			throw StandardException.newException(
                SQLState.DATA_TIME_STAMP_ILLEGAL, ts);
        }

		return (pv.getPageVersion() == getPageVersion());
	}

	/** debugging, print this page */
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON("DeadlockTrace") || SanityManager.DEBUG_ON("userLockStackTrace"))
				return "page = " + getIdentity();


			String str = "---------------------------------------------------\n";
			str += pageHeaderToString();
			// str += slotTableToString();	// print in memory slot table

			// now print each row
			for (int s = 0; s < slotsInUse; s++)
				str += recordToString(s);
		
			//if (SanityManager.DEBUG_ON("dumpPageImage"))
			{
				str += "---------------------------------------------------\n";
				str += pagedataToHexDump(pageData);
				str += "---------------------------------------------------\n";
			}
			return str;
		}
		else
			return null;
	}

    /**
     * Provide a hex dump of the data in the in memory version of the page.
     * <p>
     * The output looks like:
     *
     * 00000000: 4d5a 9000 0300 0000 0400 0000 ffff 0000  MZ..............
     * 00000010: b800 0000 0000 0000 4000 0000 0000 0000  ........@.......
     * 00000020: 0000 0000 0000 0000 0000 0000 0000 0000  ................
     * 00000030: 0000 0000 0000 0000 0000 0000 8000 0000  ................
     * 00000040: 0e1f ba0e 00b4 09cd 21b8 014c cd21 5468  ........!..L.!Th
     * 00000050: 6973 2070 726f 6772 616d 2063 616e 6e6f  is program canno
     * 00000060: 7420 6265 2072 756e 2069 6e20 444f 5320  t be run in DOS 
     * 00000070: 6d6f 6465 2e0d 0a24 0000 0000 0000 0050  mode...$.......P
     * 00000080: 4500 004c 0109 008b abfd 3000 0000 0000  E..L......0.....
     * 00000090: 0000 00e0 000e 210b 0102 3700 3405 0000  ......!...7.4...
     * 000000a0: 8401 0000 6400 0000 6004 0000 1000 0000  ....d...`.......
     * 000000b0: 5005 0000 0008 6000 1000 0000 0200 0001  P.....`.........
     * 000000c0: 0000 0000 0000 0004 0000 0000 0000 0000  ................
     * 000000d0: 9007 0000 0400 0009 a207 0002 0000 0000  ................
     * 000000e0: 0010 0000 1000 0000 0010 0000 1000 0000  ................
     * 000000f0: 0000 0010 0000 0000 6006 00ef 8100 0000  ........`.......
     * 00000100: 5006 00e6 0c00 0000 0007 00d0 0400 0000  P...............
     * 00000110: 0000 0000 0000 0000 0000 0000 0000 0000  ................
     * 00000120: 1007 00c8 7100 0000 0000 0000 0000 0000  ....q...........
     * 00000130: 0000 0000 0000 0000 0000 0000 0000 0000  ................
     *
     * <p>
     * RESOLVE - this has been hacked together and is not efficient.  There
     * are probably some java utilities to use.
     *
	 * @return The string with the hex dump in it.
     *
     * @param data   array of bytes to dump.
     **/
    private static String pagedataToHexDump(byte[] data)
    {
		return org.apache.derby.iapi.util.StringUtil.hexDump(data);
    }

	private String pageHeaderToString()
	{
		if (SanityManager.DEBUG) {
			return "page id " + getIdentity() + 
				" Overflow: " + isOverflowPage +
				" PageVersion: " + getPageVersion() +
				" SlotsInUse: " + slotsInUse +
				" DeletedRowCount: " + deletedRowCount +
				" PageStatus: " + getPageStatus() + 
				" NextId: " + nextId + 
				" firstFreeByte: " + firstFreeByte + 
				" freeSpace: " + freeSpace + 
				" totalSpace: " + totalSpace + 
				" spareSpace: " + spareSpace + 
                " PageSize: " + getPageSize() +
                "\n";
		}
		else
			return null;
	}

	private String recordToString(int slot)
	{
		if (SanityManager.DEBUG)
		{
			String str = new String();
			try 
			{
				StoredRecordHeader recordHeader = getHeaderAtSlot(slot);
				int offset = getRecordOffset(slot);
				int numberFields = recordHeader.getNumberFields();
				str = "\nslot " + slot + " offset " + offset + " " +
                         " recordlen " + getTotalSpace(slot) +
                         " (" + getRecordPortionLength(slot) +
                         "," + getReservedCount(slot) + ")"+
                         recordHeader.toString();

				rawDataIn.setPosition(offset + recordHeader.size());

				for (int i = 0; i < numberFields; i++)
				{
					int fieldStatus = StoredFieldHeader.readStatus(rawDataIn);
					int fieldDataLength = StoredFieldHeader.readFieldDataLength(rawDataIn, fieldStatus, slotFieldSize);
					if (fieldDataLength < 0)
					{
						str += "\n\tField " + i + ": offset=" + offset + " null " + 
							StoredFieldHeader.toDebugString(fieldStatus);
					}
					else 
					{
						str += "\n\tField " + i + ": offset=" + offset + 
							" len=" + fieldDataLength + " " + 
                            StoredFieldHeader.toDebugString(fieldStatus);

						if (StoredFieldHeader.isOverflow(fieldStatus))
						{
							// not likely to be a real pointer, this is most
							// likely an old column chain where the first field
							// is set to overflow even though the second field
							// is the overflow pointer
							if (i == 0 && fieldDataLength != 3) 
							{
								// figure out where we should go next
								offset = rawDataIn.getPosition() + fieldDataLength;
								long overflowPage = CompressedNumber.readLong((InputStream) rawDataIn);
								int overflowId = CompressedNumber.readInt((InputStream) rawDataIn);

								str += "Questionable long column at (" +
									overflowPage + "," + overflowId + ")";
								rawDataIn.setPosition(offset);
							}
							else
							{
								// print the overflow pointer
								long overflowPage = CompressedNumber.readLong((InputStream) rawDataIn);
								int overflowId = CompressedNumber.readInt((InputStream) rawDataIn);
								str += "long column at (" + overflowPage + "," + overflowId + ")";
							}
						}
						else
						{
							// go to next field
							offset = rawDataIn.getPosition() + fieldDataLength;
							rawDataIn.setPosition(offset);
						}
					}
				}
				str += "\n";

			}
			catch (IOException ioe)
			{
				str += "\n =======      ERROR IOException  =============\n";
				str += ioe.toString();
			}
			catch (StandardException se)
			{
				str += "\n =======      ERROR StandardException  =============\n";
				str += se.toString();
			}

			return str;
		}
		else
			return null;
	}

	/*
	**	Overflow related methods
	*/

	/**
		Get the overflow page for a record that has already overflowed.
		@exception StandardException Standard Cloudscape error policy
	*/
	protected StoredPage getOverflowPage(long pageNumber) throws StandardException
	{

		StoredPage overflowPage = (StoredPage) owner.getPage(pageNumber);
		if (overflowPage == null) {
		}

		// RESOLVE-LR
		//if (!overflowPage.isOverflow()) {
		//	overflowPage.unlatch();
		//}

		return overflowPage;
	}

	/**
		Get an empty overflow page.
		@exception StandardException Standard Cloudscape error policy
	*/
	protected BasePage getNewOverflowPage() throws StandardException
	{

		FileContainer myContainer = (FileContainer) containerCache.find(identity.getContainerId());

		try {
			// add an overflow page
			return (BasePage) myContainer.addPage(owner, true);
		} finally {
			containerCache.release(myContainer);
		}
	}

	/**
		Get the overflow slot for a record that has already overflowed.
		@exception StandardException Standard Cloudscape error policy
	*/
	protected static int getOverflowSlot(BasePage overflowPage, StoredRecordHeader recordHeader)
		throws StandardException
	{

		int slot = overflowPage.findRecordById(
                        recordHeader.getOverflowId(), Page.FIRST_SLOT_NUMBER);

		if (slot < 0)
        {
			throw StandardException.newException(
                    SQLState.DATA_SLOT_NOT_ON_PAGE);
        }

		return slot;
	}

	/**
		Get a overflow page that potentially can handle a new overflowed record.
		@exception StandardException Standard Cloudscape error policy
	*/
	public BasePage getOverflowPageForInsert(
    int                     currentSlot, 
    Object[]   row, 
    FormatableBitSet                 validColumns)
		throws StandardException
	{
		return getOverflowPageForInsert(currentSlot, row, validColumns, 0);
	}

	/**
		@exception StandardException Standard Cloudscape error policy
	*/
	public BasePage getOverflowPageForInsert(
    int                     currentSlot, 
    Object[]   row, 
    FormatableBitSet                 validColumns, 
    int                     startColumn)
		throws StandardException
	{
        // System.out.println("Top of getOverflowPageForInsert");

		// look at all the overflow pages that are in use on this page, up
		// to a maximum of 5.
		long[] pageList = new long[5];
		int    pageCount = 0;

		long   currentOverflowPageNumber = 0;

slotScan:
		for (int slot = 0; (slot < slotsInUse) && (pageCount < pageList.length); slot++) {

			StoredRecordHeader recordHeader = getHeaderAtSlot(slot);
			if (!recordHeader.hasOverflow())
				continue;

			long overflowPageNumber = recordHeader.getOverflowPage();

			if (slot == currentSlot) {
				currentOverflowPageNumber = overflowPageNumber;
				continue;
			}

			for (int i = 0; i < pageCount; i++) {
				if (pageList[i] == overflowPageNumber)
					continue slotScan;
			}

			pageList[pageCount++] = overflowPageNumber;	
		}


		for (int i = 0; i < pageCount; i++) {

			long pageNumber = pageList[i];

			// don't look at the current overflow page
			// used by this slot, because it the record is already
			// overflowed then we reached here because the overflow
			// page is full.
			if (pageNumber == currentOverflowPageNumber)
				continue;
			StoredPage overflowPage = null;
			int spaceNeeded = 0;
			try {
				overflowPage = getOverflowPage(pageNumber);
				if ( overflowPage.spaceForInsert(row, validColumns,
					spaceNeeded, startColumn, 100))
                {
                    // System.out.println("returning used page: " + pageNumber);
					return overflowPage;
                }

				spaceNeeded = ((StoredPage) overflowPage).getCurrentFreeSpace();
				overflowPage.unlatch();
				overflowPage = null;
				
			} catch (StandardException se) {
				if (overflowPage != null) {
					overflowPage.unlatch();
					overflowPage = null;
				}

			}
		}

		// if we get here then we have to allocate a new overflow page
        // System.out.println("returning new page: ");
		return getNewOverflowPage();
	}
	
	/**
		Update an already overflowed record.

		@param slot Slot of the original record on its original page
		@param row new version of the data

		@exception StandardException Standard Cloudscape error policy
	*/
	protected void updateOverflowed(
    RawTransaction          t, 
    int                     slot,
    Object[]   row, 
    FormatableBitSet                 validColumns,
    StoredRecordHeader      recordHeader)
		throws StandardException
	{

		BasePage overflowPage = getOverflowPage(recordHeader.getOverflowPage());

		try {

			int overflowSlot = getOverflowSlot(overflowPage, recordHeader);

			overflowPage.doUpdateAtSlot(t, overflowSlot, recordHeader.getOverflowId(), row, validColumns);
			overflowPage.unlatch();
			overflowPage = null;

			return;

		} finally {
			if (overflowPage != null) {
				overflowPage.unlatch();
				overflowPage = null;
			}
		}
	}


	/**
		Update a record handle to point to an overflowed record portion.
		Note that the record handle need not be the current page.
		@exception StandardException Standard Cloudscape error policy
	*/
	public void updateOverflowDetails(RecordHandle handle, RecordHandle overflowHandle)
		throws StandardException
	{
		long handlePageNumber = handle.getPageNumber();
		if (handlePageNumber == getPageNumber()) {
			updateOverflowDetails(this, handle, overflowHandle);
			return;
		}
		
		StoredPage handlePage = (StoredPage) owner.getPage(handlePageNumber);

		updateOverflowDetails(handlePage, handle, overflowHandle);		
		handlePage.unlatch();
	}

	private void updateOverflowDetails(StoredPage handlePage, RecordHandle handle, RecordHandle overflowHandle)
		throws StandardException {
		// update the temp record header, this will be used in the log row ..
		handlePage.getOverFlowRecordHeader().setOverflowDetails(overflowHandle);

		// Use the slot interface as we don't need a lock since
		// the initial insert/update holds the lock on the first
		// portion of the record.
		int slot = handlePage.getSlotNumber(handle);

		// use doUpdateAtSlot as it avoids unnecessary work in updateAtSlot the
        // null indicates to this page that the record should become an 
        // overflow record
		handlePage.doUpdateAtSlot(
            owner.getTransaction(), slot, handle.getId(), 
            (Object[]) null, (FormatableBitSet) null);
	}

	/**
		@exception StandardException Standard Cloudscape error policy
	*/
	public void updateFieldOverflowDetails(RecordHandle handle, RecordHandle overflowHandle)
		throws StandardException
	{
		// add an overflow field at the end of the previous record
        // uses sparse rows
		Object[] row = new Object[2];
		row[1] = overflowHandle;

		// we are expanding the record to have 2 fields, the second field is the overflow pointer.
		FormatableBitSet validColumns = new FormatableBitSet(2);
		validColumns.set(1);

		// Use the slot interface as we don't need a lock since
		// the initial insert/update holds the lock on the first
		// portion of the record.
		int slot = getSlotNumber(handle);

		// use doUpdateAtSlot as it avoids unnecessary work in updateAtSlot
		doUpdateAtSlot(owner.getTransaction(), slot, handle.getId(), row, validColumns);
	}

	/**
		@exception StandardException Standard Cloudscape error policy
	*/
	public int appendOverflowFieldHeader(DynamicByteArrayOutputStream logBuffer, RecordHandle overflowHandle)
		throws StandardException, IOException
	{
		int fieldStatus = StoredFieldHeader.setInitial();
		fieldStatus = StoredFieldHeader.setOverflow(fieldStatus, true);

		long overflowPage = overflowHandle.getPageNumber();
		int overflowId = overflowHandle.getId();
		int fieldDataLength = CompressedNumber.sizeLong(overflowPage)
			+ CompressedNumber.sizeInt(overflowId);

		// write the field header to the log buffer
		int lenWritten = StoredFieldHeader.write(logBuffer, fieldStatus, fieldDataLength, slotFieldSize);

		// write the overflow details to the log buffer
		lenWritten += CompressedNumber.writeLong(logBuffer, overflowPage);
		lenWritten += CompressedNumber.writeInt(logBuffer, overflowId);

		// this length is the same on page as in the log
		return (lenWritten);
	}

    protected int getSlotsInUse()
    {
        return(slotsInUse);
    }


	/**
		return the max datalength allowed with the space available
	*/
	private int getMaxDataLength(int spaceAvailable, int overflowThreshold) {

		if (SanityManager.DEBUG) {
			if (overflowThreshold == 0) 
				SanityManager.THROWASSERT("overflowThreshold cannot be 0");
		}

		// we need to take into considering of the overflowThreshold
		// the overflowThreshold limits the max data length,
		// whatever space we have left, we will not allow max data length
		// to exceed the overflow threshold.
		int maxThresholdSpace = totalSpace * overflowThreshold / 100;
		int maxAvailable = 0;

		if (spaceAvailable < (64 - 2))
			maxAvailable = spaceAvailable - 2;
		else if (spaceAvailable < (16383 - 3))
			maxAvailable = spaceAvailable - 3;
		else
			maxAvailable = spaceAvailable - 5;

		return (maxAvailable > maxThresholdSpace ? maxThresholdSpace : maxAvailable);

	}

	/**
		return whether the field has exceeded the max threshold for this page
		it compares the fieldSize with the largest possible field for this page
	*/
	private boolean isLong(int fieldSize, int overflowThreshold) {

		if (SanityManager.DEBUG) {
			if (overflowThreshold == 0) 
				SanityManager.THROWASSERT("overflowThreshold cannot be 0");
		}

		// if a field size is over the threshold, then it becomes a long column
		int maxThresholdSize = maxFieldSize * overflowThreshold / 100;
		return (fieldSize > maxThresholdSize);
	}

	/**
		Perform an update.

		@exception StandardException Standard cloudscape policy
	*/
	public void doUpdateAtSlot(
    RawTransaction          t, 
    int                     slot, 
    int                     id, 
    Object[]                row, 
    FormatableBitSet                 validColumns)
		throws StandardException
	{
		// If this is a head page, the recordHandle is the head row handle.
		// If this is not a head page, we are calling updateAtSlot inside some
		// convoluted loop that updates an overflow chain.  There is nothing we
		// can doing about it anyway.
		RecordHandle headRowHandle = 
            isOverflowPage() ? null : getRecordHandleAtSlot(slot); 
		
		// RESOLVE: djd/yyz what does a null row means? (sku)
		if (row == null) 
        {
			owner.getActionSet().actionUpdate(
                t, this, slot, id, row, validColumns, -1, 
                (DynamicByteArrayOutputStream) null, -1, headRowHandle);

			return;
		}

		// startColumn is the first column to be updated.
		int startColumn = RowUtil.nextColumn(row, validColumns, 0);
		if (startColumn == -1)
			return;

		if (SanityManager.DEBUG)
		{
			// make sure that if N bits are set in the validColumns that
			// exactly N columns are passed in via the row array.
			if (!isOverflowPage() && validColumns != null)
			{
				if (RowUtil.getNumberOfColumns(-1, validColumns) > row.length)
					SanityManager.THROWASSERT("updating slot " + slot + 
						 " on page " + getIdentity() + " " +
						  RowUtil.getNumberOfColumns(-1, validColumns) + 
						  " bits are set in validColumns but only " +
						  row.length + " columns in row[]");
			}
		}


		// Keep track of row shrinkage in the head row piece.  If any row piece
		// shrinks, file a post commit work to clear all reserved space for the
		// entire row chain.
		boolean rowHasReservedSpace = false; 

		StoredPage curPage = this;
		for (;;) 
        {
			StoredRecordHeader rh = curPage.getHeaderAtSlot(slot);

			int startField          = rh.getFirstField(); 
			int endFieldExclusive   = startField + rh.getNumberFields();

			// curPage contains column[startField] to column[endFieldExclusive-1]

			// Need to cope with an update that is increasing the number of 
            // columns.  If this occurs we want to make sure that we perform a 
            // single update to the last portion of a record, and not an update
            // of the current columns and then an update to append a column.

			long nextPage        = -1;
			int  realStartColumn = -1;
			int  realSpaceOnPage = -1;

			if (!rh.hasOverflow() || 
                ((startColumn >= startField) && 
                 (startColumn <  endFieldExclusive))) 
			{
				boolean                 hitLongColumn;
				int                     nextColumn      = -1;
				Object[]   savedFields     = null;
				DynamicByteArrayOutputStream  logBuffer       = null;

				do 
                {
					try 
                    {
						// Update this portion of the record.
						// Pass in headRowHandle in case we are to update any
						// long column and they need to be cleaned up by post
						// commit processing.  We don't want to purge the
						// columns right now because in order to reclaim the
						// page, we need to remove them.  But it would be bad
						// to remove them now because the transaction may not
						// commit for a long time.  We can do both purging of
						// the long column and page removal together in the
						// post commit.
						nextColumn = 
                            owner.getActionSet().actionUpdate(
                                t, curPage, slot, id, row, validColumns, 
							    realStartColumn, logBuffer, 
                                realSpaceOnPage, headRowHandle);

						hitLongColumn = false;

					} 
                    catch (LongColumnException lce) 
                    {
	
						if (lce.getRealSpaceOnPage() == -1) 
                        {
							// an update that has caused the row to increase 
                            // in size *and* push some fields off the page 
                            // that need to be inserted in an overflow page

							// no need to make a copy as we are going to use 
                            // this buffer right away
							logBuffer = lce.getLogBuffer();

							savedFields     = 
                                (Object[]) lce.getColumn();
                            
							realStartColumn = lce.getNextColumn();
							realSpaceOnPage = -1;

							hitLongColumn   = true;

							continue;
						}

						
						// we caught a real long column exception
						// three things should happen here:
						// 1. insert the long column into overflow pages.
						// 2. append the overflow field header in the main chain.
						// 3. continue the update in the main data chain.
						logBuffer = 
                            new DynamicByteArrayOutputStream(lce.getLogBuffer());

						// step 1: insert the long column ... if this update 
                        // operation rolls back, purge the after image column 
                        // chain and reclaim the overflow page because the 
                        // whole chain will be orphaned anyway. 
						RecordHandle longColumnHandle =
							insertLongColumn(
                                curPage, lce, Page.INSERT_UNDO_WITH_PURGE);

						// step 2: append overflow field header to log buffer
						int overflowFieldLen = 0;
						try 
                        {
							overflowFieldLen +=
								appendOverflowFieldHeader(
                                    logBuffer, longColumnHandle);

						} 
                        catch (IOException ioe) 
                        {
							throw StandardException.newException(
                                SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
						}

						// step 3: continue the insert in the main data chain
						// need to pass the log buffer, and start column to the
                        // next insert.
						realStartColumn = lce.getNextColumn() + 1;
						realSpaceOnPage = lce.getRealSpaceOnPage() - overflowFieldLen;
						hitLongColumn = true;

					}

				} while (hitLongColumn);


				// See if we completed all the columns that are on this page.
				int validColumnsSize = 
                    (validColumns == null) ? 0 : validColumns.getLength();

				if (nextColumn != -1) 
                {

					if (SanityManager.DEBUG) 
                    {
						// note nextColumn might be less than the the first 
                        // column we started updating. This is because the 
                        // update might force the record header to grow and 
                        // push fields before the one we are updating off the 
                        // page and into this insert.

						if ((nextColumn < startField) || 
                            (rh.hasOverflow() && (nextColumn >= endFieldExclusive)))
                        {
							SanityManager.THROWASSERT(
                                "nextColumn out of range = " + nextColumn +
								" expected between " + 
                                startField + " and " + endFieldExclusive);
                        }
					}

					// Need to insert rows from nextColumn to endFieldExclusive 
                    // onto a new overflow page.
					// If the column is not being updated we
					// pick it up from the current page. If it is being updated
					// we take it from the new value.
					int possibleLastFieldExclusive = endFieldExclusive;
                    
					if (!rh.hasOverflow()) 
                    {
						// we might be adding a field here
						if (validColumns == null) 
                        {
							if (row.length > possibleLastFieldExclusive)
								possibleLastFieldExclusive = row.length;
						} 
                        else 
                        {
							if (validColumnsSize > possibleLastFieldExclusive)
								possibleLastFieldExclusive = validColumnsSize;
						}
					}


                    // use a sparse row
					Object[] newRow = 
                        new Object[possibleLastFieldExclusive];

					FormatableBitSet  newColumnList = 
                        new FormatableBitSet(possibleLastFieldExclusive);

					ByteArrayOutputStream fieldStream = null;

					for (int i = nextColumn; i < possibleLastFieldExclusive; i++) 
                    {
						if ((validColumns == null) || 
                            (validColumnsSize > i && validColumns.isSet(i))) 
                        {
							newColumnList.set(i);
							// use the new value
							newRow[i] = RowUtil.getColumn(row, validColumns, i);

						}
                        else if (i < endFieldExclusive) 
                        {
							newColumnList.set(i);

							// use the old value
							newRow[i] = savedFields[i - nextColumn];
						}
					}

					RecordHandle handle = curPage.getRecordHandleAtSlot(slot);

					// If the portion we just updated is the last portion then 
                    // there cannot be any updates to do.
					if (rh.hasOverflow()) 
                    {
						// We have to carry across the overflow information
						// from the current record, if any.
						nextPage = rh.getOverflowPage();
						id = rh.getOverflowId();

						// find the next starting column before unlatching page
						startColumn = 
                            RowUtil.nextColumn(
                                row, validColumns, endFieldExclusive);
					} 
                    else 
                    {
						startColumn = -1;
						nextPage = 0;
					}


					// After the update is done, see if this row piece has
					// shrunk in curPage if no other row pieces have shrunk so
					// far.  In head page, need to respect minimumRecordSize.
					// In overflow page, only need to respect
					// RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT
					// Don't bother with temp container.
					if (!rowHasReservedSpace && headRowHandle != null &&
						curPage != null && !owner.isTemporaryContainer())
					{
						rowHasReservedSpace = 
                            curPage.checkRowReservedSpace(slot);
					}


					// insert the record portion on a new overflow page at slot
                    // 0 this will automatically handle any overflows in
					// this new portion

					// BasePage op = getNewOverflowPage();

                    BasePage op = 
                        curPage.getOverflowPageForInsert(
                            slot,
                            newRow,
                            newColumnList,
                            nextColumn);

					// We have all the information from this page so unlatch it
					if (curPage != this) 
                    {
						curPage.unlatch();
						curPage = null;
					}

					byte mode = Page.INSERT_OVERFLOW;
					if (nextPage != 0)
						mode |= Page.INSERT_FOR_SPLIT;

					RecordHandle nextPortionHandle =
						nextPage == 0 ? null :
						owner.makeRecordHandle(nextPage, id);

					// RESOLVED (sku):  even though we would like to roll back 
                    // these inserts with PURGE rather than with delete, 
                    // we have to delete because if we purge the last row
					// from an overflow page, the purge will queue a post 
                    // commit to remove the page.
					// While this is OK with long columns, we cannot do this 
                    // for long rows because long row overflow pages can be 
                    // shared by more than one long rows, and thus it is unsafe
					// to remove the page without first latching the head page.
                    // However, the insert log record do not have the head 
                    // row's page number so the rollback cannot put that
					// information into the post commit work.
					RecordHandle portionHandle =
						op.insertAllowOverflow(
                            0, newRow, newColumnList, nextColumn, mode, 100, 
                            nextPortionHandle);

					// Update the previous record header to point to new portion
					if (curPage == this)
						updateOverflowDetails(this, handle, portionHandle);
					else
						updateOverflowDetails(handle, portionHandle);
					op.unlatch();
				} 
                else 
                {

					// See earlier comments on checking row reserved space.
					if (!rowHasReservedSpace    && 
                        headRowHandle != null   &&
						curPage != null         && 
                        !owner.isTemporaryContainer()) 
                    {
						rowHasReservedSpace = 
                            curPage.checkRowReservedSpace(slot);
					}


					// find the next starting column before we unlatch the page
					startColumn = 
                        rh.hasOverflow() ? 
                            RowUtil.nextColumn(
                                row, validColumns, endFieldExclusive) : -1;
				}

				// have we completed this update?
				if (startColumn == -1) {

					if ((curPage != this) && (curPage != null))
						curPage.unlatch();
					break;		// break out of the for loop
				}
			}

			if (nextPage == -1) 
            {
				if (SanityManager.DEBUG) 
                {
					SanityManager.ASSERT(
                        curPage != null, 
                        "Current page is null be no overflow information has been obtained");
				}

				// Get the next page info while we still have the page
				// latched.
				nextPage = rh.getOverflowPage();
				id = rh.getOverflowId();
			}
			
			if ((curPage != this) && (curPage != null))
				curPage.unlatch();

			// get the next portion page and find the correct slot
			curPage = (StoredPage) owner.getPage(nextPage);

			if (SanityManager.DEBUG)
            {
				SanityManager.ASSERT(
                    curPage.isOverflowPage(), 
                    "following row chain gets a non-overflow page");
            }

			slot = curPage.findRecordById(id, FIRST_SLOT_NUMBER);
		}

		// Back to the head page.  Get rid of all reserved space in the entire
		// row post commit.
		if (rowHasReservedSpace)
		{
			RawTransaction rxact = (RawTransaction)owner.getTransaction();

			ReclaimSpace work = 
				new ReclaimSpace(ReclaimSpace.ROW_RESERVE,
								 headRowHandle, 
								 rxact.getDataFactory(), true);
			rxact.addPostCommitWork(work);
		}
	}

	/**
		See if the row on this page has reserved space that can be shrunk once
		the update commits.
	 */
	private boolean checkRowReservedSpace(int slot) throws StandardException
	{
		boolean rowHasReservedSpace = false;
		try {
			int shrinkage = getReservedCount(slot);

			// Only reclaim reserved space if it is
			// "reasonably" sized, i.e., we can reclaim at
			// least MININUM_RECORD_SIZE_DEFAULT
			int reclaimThreshold = RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT;
			
			if (shrinkage > reclaimThreshold) {
				int totalSpace = getRecordPortionLength(slot) + shrinkage; 

				if (isOverflowPage()) {
					if (totalSpace >
						RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT+reclaimThreshold)
						rowHasReservedSpace = true;

					// Otherwise, I can at most reclaim less than
					// MINIMUM_RECORD_SIZE_DEFAULT, forget about that.
				} else {
					// this is a head page
					if (totalSpace > (minimumRecordSize +
									  RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT)) 
						rowHasReservedSpace = true;

					// Otherwise, I can at most reclaim less than
					// MINIMUM_RECORD_SIZE_DEFAULT, forget about that.
				}
			}
		} catch (IOException ioe) {
			throw StandardException.newException(
                SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}

		return rowHasReservedSpace;
	}

	/**
		@see BasePage#compactRecord
		@exception StandardException Standard Cloudscape error policy
	 */
	protected void compactRecord(RawTransaction t, int slot, int id) 
		 throws StandardException 
	{
		// If this is a head row piece, first take care of the entire overflow
		// row chain.  Don't need to worry about long column because they are
		// not in place updatable.
		if (isOverflowPage() == false) {
			StoredRecordHeader recordHeader = getHeaderAtSlot(slot);

			while (recordHeader.hasOverflow()) {
				StoredPage nextPageInRowChain =
					getOverflowPage(recordHeader.getOverflowPage());

				if (SanityManager.DEBUG)
					SanityManager.ASSERT(nextPageInRowChain != null);

				try {
					int nextId = recordHeader.getOverflowId();
					int nextSlot = getOverflowSlot(nextPageInRowChain, recordHeader);

					nextPageInRowChain.compactRecord(t, nextSlot, nextId);

					// Follow the next long row pointer.
					recordHeader = nextPageInRowChain.getHeaderAtSlot(nextSlot);
				} finally {
					nextPageInRowChain.unlatch();
				}
			}
		}

		// Lastly, see if this row has anything sizable that can be freed.
		// Try to only reclaim space larger than MINIMUM_RECORD_SIZE_DEFAULT
		// because otherwise it is probably not worth the effort.
		int reclaimThreshold = RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT;
		try
		{
			int reserve = getReservedCount(slot);
			if (reserve > reclaimThreshold) {
				int recordLength = getRecordPortionLength(slot);
				int correctReservedSpace = reserve;

				if (isOverflowPage()) {
					if ((reserve + recordLength) > 
						(RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT+reclaimThreshold))
					{ 
						// calculate what the correct reserved space is
						if (recordLength >= RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT)
							correctReservedSpace = 0;
						else	// make sure record takes up minimum_record_size 
							correctReservedSpace = 
								RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT - recordLength; 
					}
				} else {
					// this is a head page
					if ((reserve + recordLength) > 
						(minimumRecordSize+reclaimThreshold)) {
						// calculate what the correct reserved space is
						if (recordLength >= minimumRecordSize)
							correctReservedSpace = 0;
						else
							correctReservedSpace = minimumRecordSize - recordLength;
					}
				}

				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(correctReservedSpace <= reserve,
										 "correct reserve > reserve");
				}

				// A shrinkage has occured.
				if (correctReservedSpace < reserve) {
					owner.getActionSet().
						actionShrinkReservedSpace(t, this, slot, id,
										correctReservedSpace, reserve);
				}
			}
		} catch (IOException ioe) {
			throw StandardException.newException(
                SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}
	}
}


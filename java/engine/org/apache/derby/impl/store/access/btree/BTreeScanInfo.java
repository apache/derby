/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.access.btree
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.access.btree;

import org.apache.derby.iapi.store.access.ScanInfo;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.i18n.MessageService;
import java.util.Properties;

/**

  This object provides performance information related to an open scan.
  The information is accumulated during operations on a ScanController() and
  then copied into this object and returned by a call to 
  ScanController.getStatistic().


**/
class BTreeScanInfo implements ScanInfo
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
    /**
     * Performance counters ...
     */
    private int     stat_numpages_visited           = 0;
    private int     stat_numrows_visited            = 0;
    private int     stat_numrows_qualified          = 0;
    private int     stat_numdeleted_rows_visited    = 0;
    private int     stat_numColumnsFetched          = 0;
    private int     stat_treeHeight                 = 0;
    private FormatableBitSet  stat_validColumns               = null;

    /* Constructors for This class: */
    BTreeScanInfo(BTreeScan scan)
    {
        // copy perfomance state out of scan, to get a fixed set of stats
        stat_numpages_visited           = scan.stat_numpages_visited;
        stat_numrows_visited            = scan.stat_numrows_visited;
        stat_numrows_qualified          = scan.stat_numrows_qualified;
        stat_numdeleted_rows_visited    = scan.stat_numdeleted_rows_visited;

        stat_validColumns = 
            (scan.init_scanColumnList == null ? 
                null : ((FormatableBitSet) scan.init_scanColumnList.clone()));

        if (stat_validColumns == null)
        {
            stat_numColumnsFetched = scan.init_template.length;
        }
        else
        {
            for (int i = 0; i < stat_validColumns.size(); i++)
            {
                if (stat_validColumns.get(i))
                    stat_numColumnsFetched++;
            }
        }

        try
        {
            stat_treeHeight = scan.getHeight();
        }
        catch (Throwable t)
        {
            stat_treeHeight = -1;
        }
    }

    /**
     * Return all information gathered about the scan.
     * <p>
     * This routine returns a list of properties which contains all information
     * gathered about the scan.  If a Property is passed in, then that property
     * list is appeneded to, otherwise a new property object is created and
     * returned.
     * <p>
     * Not all scans may support all properties, if the property is not 
     * supported then it will not be returned.  The following is a list of
     * properties that may be returned:
     *
     *     numPagesVisited
     *         - the number of pages visited during the scan.  For btree scans
     *           this number only includes the leaf pages visited.  
     *     numRowsVisited
     *         - the number of rows visited during the scan.  This number 
     *           includes all rows, including: those marked deleted, those
     *           that don't meet qualification, ...
     *     numRowsQualified
     *         - the number of undeleted rows, which met the qualification.
     *     treeHeight (btree's only)
     *         - for btree's the height of the tree.  A tree with one page
     *           has a height of 1.  Total number of pages visited in a btree
     *           scan is (treeHeight - 1 + numPagesVisited).
     *     numColumnsFetched
     *         - the number of columns Fetched - partial scans will result
     *           in fetching less columns than the total number in the scan.
     *     columnsFetched
     *         - The FormatableBitSet.toString() method called on the validColumns arg.
     *           to the scan, unless validColumns was set to null, and in that
     *           case we will return "all".
     *     NOTE - this list will be expanded as more information about the scan
     *            is gathered and returned.
     *
     * @param prop   Property list to fill in.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public Properties getAllScanInfo(Properties prop)
		throws StandardException
    {
        if (prop == null)
            prop = new Properties();

        prop.put(
			MessageService.getTextMessage(SQLState.STORE_RTS_SCAN_TYPE),
			MessageService.getTextMessage(SQLState.STORE_RTS_BTREE));
        prop.put(
			MessageService.getTextMessage(SQLState.STORE_RTS_NUM_PAGES_VISITED),
            Integer.toString(stat_numpages_visited));
        prop.put(
			MessageService.getTextMessage(SQLState.STORE_RTS_NUM_ROWS_VISITED),
            Integer.toString(stat_numrows_visited));
        prop.put(
			MessageService.getTextMessage(
								SQLState.STORE_RTS_NUM_DELETED_ROWS_VISITED),
            Integer.toString(stat_numdeleted_rows_visited));
        prop.put(
			MessageService.getTextMessage(
								SQLState.STORE_RTS_NUM_ROWS_QUALIFIED),
            Integer.toString(stat_numrows_qualified));
        prop.put(
			MessageService.getTextMessage(
								SQLState.STORE_RTS_TREE_HEIGHT),
            Integer.toString(stat_treeHeight));
        prop.put(
			MessageService.getTextMessage(
								SQLState.STORE_RTS_NUM_COLUMNS_FETCHED),
            Integer.toString(stat_numColumnsFetched));
        prop.put(
			MessageService.getTextMessage(
								SQLState.STORE_RTS_COLUMNS_FETCHED_BIT_SET),
			(stat_validColumns == null ?
				MessageService.getTextMessage(
								SQLState.STORE_RTS_ALL) :
                stat_validColumns.toString()));

        return(prop);
    }
}

/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql;

/**
 * This is a holder of language properties that are
 * exposed users.  Consolodate all properties here.
 */
public interface LanguageProperties
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	/*
	** BulkFetch
	**
	** The default size needs some explaining.  As
	** of 7/14/98, the most efficient way for access
	** to return rows from a table is basically by
	** reading/qualifying/returning all the rows in
	** one page.  If you are read in many many rows
	** at a time the performance gain is only marginally
	** better.  Anyway, since even a small number of
	** rows per read helps, and since there is no good
	** way to get access to retrieve the rows page
	** by page, we use 16 totally arbitrarily.  Ultimately,
	** this should be dynamically sized -- in which
	** case we wouldn't need this default.
	*/
    static final String BULK_FETCH_PROP = "derby.language.bulkFetchDefault";
    static final String BULK_FETCH_DEFAULT = "16";
    static final int BULK_FETCH_DEFAULT_INT = 16;
}

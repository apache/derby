/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.access
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.access;

/**

Global constants provided by the Access Interface.

**/

public interface AccessFactoryGlobals
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;
    /**************************************************************************
     * Static constants.
     **************************************************************************
     */
    /**
     * The name for user transactions. This name will be displayed by the
     * transactiontable VTI.
     */
    public static final String USER_TRANS_NAME = "UserTransaction";

    /**
     * The name for system transactions. This name will be displayed by the
     * transactiontable VTI.
     */
    public static final String SYS_TRANS_NAME = "SystemTransaction";

	/**
	 *	Overflow Threshold
	 *
	 *  This defined how large the row can be before it becomes a long row,
	 *  during an insert.
	 *
	 *  @see org.apache.derby.iapi.store.raw.Page
	 */
	public static final int BTREE_OVERFLOW_THRESHOLD = 50;
	public static final int HEAP_OVERFLOW_THRESHOLD  = 100;
	public static final int SORT_OVERFLOW_THRESHOLD  = 100;

    public static final String CFG_CONGLOMDIR_CACHE = "ConglomerateDirectoryCache";

    public static final String HEAP = "heap";

	public static final String DEFAULT_PROPERTY_NAME = "derby.defaultPropertyName";

	public static final String PAGE_RESERVED_SPACE_PROP = "0";

	public static final String CONGLOM_PROP = "derby.access.Conglomerate.type";

	public static final String IMPL_TYPE = "implType";

	public static final String SORT_EXTERNAL = "sort external";
	public static final String SORT_INTERNAL = "sort internal";

	public static final String NESTED_READONLY_USER_TRANS = "nestedReadOnlyUserTransaction";
	public static final String NESTED_UPDATE_USER_TRANS = "nestedUpdateUserTransaction";

    public static final String RAMXACT_CONTEXT_ID = "RAMTransactionContext";

    public static final String RAMXACT_CHILD_CONTEXT_ID = "RAMChildContext";

    public static final String RAMXACT_INTERNAL_CONTEXT_ID = "RAMInternalContext";

}


/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.access
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.access;

import org.apache.derby.iapi.error.StandardException; 

import java.util.Properties;

/**

ConglomPropertyable provides the interfaces to read properties from a 
conglomerate.
<p>
RESOLVE - If language ever wants these interfaces on a ScanController it 
          should not be too difficult to add them.

@see ConglomerateController

**/

public interface ConglomPropertyQueryable
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
    /**
     * Request the system properties associated with a table. 
     * <p>
     * Request the value of properties that are associated with a table.  The
     * following properties can be requested:
     *     derby.storage.pageSize 
     *     derby.storage.pageReservedSpace
     *     derby.storage.minimumRecordSize
     *     derby.storage.initialPages
     * <p>
     * To get the value of a particular property add it to the property list,
     * and on return the value of the property will be set to it's current 
     * value.  For example:
     *
     * get_prop(ConglomerateController cc)
     * {
     *     Properties prop = new Properties();
     *     prop.put("derby.storage.pageSize", "");
     *     cc.getTableProperties(prop);
     *
     *     System.out.println(
     *         "table's page size = " + 
     *         prop.getProperty("derby.storage.pageSize");
     * }
     *
     * @param prop   Property list to fill in.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    void getTableProperties(Properties prop)
		throws StandardException;

    /**
     * Request set of properties associated with a table. 
     * <p>
     * Returns a property object containing all properties that the store
     * knows about, which are stored persistently by the store.  This set
     * of properties may vary from implementation to implementation of the
     * store.
     * <p>
     * This call is meant to be used only for internal query of the properties
     * by jbms, for instance by language during bulk insert so that it can
     * create a new conglomerate which exactly matches the properties that
     * the original container was created with.  This call should not be used
     * by the user interface to present properties to users as it may contain
     * properties that are meant to be internal to jbms.  Some properties are 
     * meant only to be specified by jbms code and not by users on the command
     * line.
     * <p>
     * Note that not all properties passed into createConglomerate() are stored
     * persistently, and that set may vary by store implementation.
     *
     * @param prop   Property list to add properties to.  If null, routine will
     *               create a new Properties object, fill it in and return it.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    Properties getInternalTablePropertySet(Properties prop)
		throws StandardException;
}

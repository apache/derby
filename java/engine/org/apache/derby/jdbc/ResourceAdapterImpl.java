/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.jdbc
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.jdbc;

import org.apache.derby.iapi.services.info.JVMInfo;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.jdbc.ResourceAdapter;
//import org.apache.derby.iapi.jdbc.XATransactionResource;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.xa.XAResourceManager;
import org.apache.derby.iapi.store.access.xa.XAXactId;


import java.util.Properties;
import java.util.Hashtable;
import java.util.Enumeration;


public class ResourceAdapterImpl
		implements ResourceAdapter, ModuleControl
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	private boolean active;

	// the real resource manager 
	private XAResourceManager rm;	

	// maps Xid to XATransationResource for run time transactions
	private Hashtable connectionTable;

	/*
	 * Module control
	 */

	public void boot(boolean create, Properties properties)
		throws StandardException
	{
		// we can only run on jdk1.2 or beyond with JTA and JAVA 20 extension
		// loaded.
		if (SanityManager.DEBUG) {
			if (JVMInfo.JDK_ID < 2)
				SanityManager.THROWASSERT("expected JDK ID to be 2 - is " + JVMInfo.JDK_ID);
		}

		connectionTable = new Hashtable();

		AccessFactory af = 
			(AccessFactory)Monitor.findServiceModule(this, AccessFactory.MODULE);

		rm = (XAResourceManager) af.getXAResourceManager();

		active = true;
	}

	public void stop()
	{
		active = false;

		for (Enumeration e = connectionTable.elements(); e.hasMoreElements(); ) {

			XATransactionState tranState = (XATransactionState) e.nextElement();

			try {
				tranState.conn.close();
			} catch (java.sql.SQLException sqle) {
			}
		}

		active = false;
	}

	public boolean isActive()
	{
		return active;
	}

	/*
	 * Resource Adapter methods 
	 */

	public synchronized Object findConnection(XAXactId xid) {

		return connectionTable.get(xid);
	}

	public synchronized boolean addConnection(XAXactId xid, Object conn) {
		if (connectionTable.get(xid) != null)
			return false;

		// put this into the transaction table, if the xid is already
		// present as an in-doubt transaction, we need to remove it from
		// the run time list
		connectionTable.put(xid, conn);
		return true;
	}

	public synchronized Object removeConnection(XAXactId xid) {

		return connectionTable.remove(xid);

	}


	/**
		Return the XA Resource manager to the XA Connection
	 */
	public XAResourceManager getXAResourceManager()
	{
		return rm;
	}
}

/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.conn
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.conn;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.impl.sql.GenericPreparedStatement;
import org.apache.derby.impl.sql.GenericStatement;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.PreparedStatement;

import org.apache.derby.iapi.services.cache.Cacheable;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.monitor.Monitor;

/**
	@author ames
*/
public class CachedStatement implements Cacheable {

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	private GenericPreparedStatement ps;
	private Object identity;

	public CachedStatement() {
	}

	/**
	 * Get the PreparedStatement that is associated with this Cacheable
	 */
	public GenericPreparedStatement getPreparedStatement() {
		return ps;
	}

	/* Cacheable interface */

	/**

	    @see Cacheable#clean
	*/
	public void clean(boolean forRemove) {
	}

	/**
	*/
	public Cacheable setIdentity(Object key) {

		identity = key;
		ps = new GenericPreparedStatement((GenericStatement) key);
		ps.setCacheHolder(this);

		return this;
	}

	/** @see Cacheable#createIdentity */
	public Cacheable createIdentity(Object key, Object createParameter) {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("Not expecting any create() calls");

		return null;

	}

	/** @see Cacheable#clearIdentity */
	public void clearIdentity() {

		if (SanityManager.DEBUG)
			SanityManager.DEBUG("StatementCacheInfo","CLEARING IDENTITY: "+ps.getSource());
		ps.setCacheHolder(null);

		identity = null;
		ps = null;
	}

	/** @see Cacheable#getIdentity */
	public Object getIdentity() {
		return identity;
	}

	/** @see Cacheable#isDirty */
	public boolean isDirty() {
		return false;
	}

	/* Cacheable interface */
}

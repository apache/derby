/*

   Derby - Class org.apache.derby.impl.sql.conn.CachedStatement

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

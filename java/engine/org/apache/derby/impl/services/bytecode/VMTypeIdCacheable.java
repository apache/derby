/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.bytecode
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.bytecode;

import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.cache.CacheManager;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.classfile.ClassHolder;

/**
 * This class implements a Cacheable for a Byte code generator cache of
 * VMTypeIds.  It maps a Java class or type name to a VM type ID.
 */
class VMTypeIdCacheable implements Cacheable {
	/* The VM name of the Java class name */
	// either a Type (java type) or a String (method descriptor)
	private Object descriptor;

	/* This is the identity */
	private Object key;

	/* Cacheable interface */

	/** @see Cacheable#clearIdentity */
	public void clearIdentity() {
	}

	/** @see Cacheable#getIdentity */
	public Object getIdentity() {
		return key;
	}

	/** @see Cacheable#createIdentity */
	public Cacheable createIdentity(Object key, Object createParameter) {
		if (SanityManager.DEBUG) {
			SanityManager.THROWASSERT("VMTypeIdCacheable.create() called!");
		}
		return this;
	}

	/** @see Cacheable#setIdentity */
	public Cacheable setIdentity(Object key) {

		this.key = key;
		if (key instanceof String) {
			/* The identity is the Java class name */
			String javaName = (String) key;

			/* Get the VM type name associated with the Java class name */
			String vmName = ClassHolder.convertToInternalDescriptor(javaName);
			descriptor = new Type(javaName, vmName);
		}
		else
		{
			descriptor = ((BCMethodDescriptor) key).buildMethodDescriptor();
		}

		return this;
	}

	/** @see Cacheable#clean */
	public void clean(boolean remove) {
		/* No such thing as a dirty cache entry */
		return;
	}

	/** @see Cacheable#isDirty */
	public boolean isDirty() {
		/* No such thing as a dirty cache entry */
		return false;
	}

	/*
	** Class specific methods.
	*/

	/**
	 * Get the VM Type name (java/lang/Object) that is associated with this Cacheable
	 */

	Object descriptor() {
		return descriptor;
	}
}

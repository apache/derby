/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql;

import org.apache.derby.iapi.services.property.PropertySetCallback;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.access.TransactionController;
import java.io.Serializable;
import java.util.Dictionary;

/**
 * A class to handle setting language database properties
 */
public class LanguageDbPropertySetter implements PropertySetCallback
{
	public void init(boolean dbOnly, Dictionary p) {
		// not called yet ...
	}
	/** @exception StandardException Thrown on error. */
	public boolean validate
	(
		String			key,
		Serializable	value,
		Dictionary		p
	) throws StandardException 
	{
		if (key.equals(Property.LANGUAGE_STALE_PLAN_CHECK_INTERVAL)) {
			PropertyUtil.intPropertyValue(
						Property.LANGUAGE_STALE_PLAN_CHECK_INTERVAL,
						value,
						Property.MIN_LANGUAGE_STALE_PLAN_CHECK_INTERVAL,
						Integer.MAX_VALUE,
						Property.DEFAULT_LANGUAGE_STALE_PLAN_CHECK_INTERVAL
						);
			return true;
		}

		return false;
	}

	public Serviceable apply
	(
		String			key,
		Serializable	value,
		Dictionary		p
	) 
	{
		return null;
	}

 	public Serializable map
	(
		String			key,
		Serializable	value,
		Dictionary		p
	) 
	{
		return null;
	}
}

/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.catalog
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.services.context.ContextImpl;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.error.ExceptionSeverity;
/*
 * DataDictionaryContextImpl
 *
 * This class supports both nested and outer data dictionaries. The
 * outer data dictionary defines the name space for a database.
 * A nested data dictionary defines a name space for a publication
 * (or schema in the future).
 *
 * During error processing, a nested data dictionary context is popped
 * for statement errors or higher. The outer data dictionary context
 * is not popped by cleanupOnError.
 */
public class DataDictionaryContextImpl 
	extends ContextImpl
	implements DataDictionaryContext
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	//
	// True means this is a nested data dictionary. We pop a nested data dictionary
	// when we clean up an error.
	private final boolean nested;

	//
	// DataDictionaryContext interface
	//
	// we might want these to refuse to return
	// anything if they are in-use -- would require
	// the interface provide a 'done' call, and
	// we would mark them in-use whenever a get happened.
	public DataDictionary getDataDictionary()
	{
		return dataDictionary;
	}

	public void cleanupOnError(Throwable error)
	{
		if (!nested) return;
		if (error instanceof StandardException)
		{
			StandardException se = (StandardException)error;
		 	if (se.getSeverity() >= ExceptionSeverity.STATEMENT_SEVERITY)
				popMe();
		}
		else
		{
			popMe();
		}
	}

	//
	// class interface
	//
	// this constructor is called with the data dictionary
	// to be saved when the context
	// is created
	
	public DataDictionaryContextImpl(ContextManager cm, DataDictionary dataDictionary,
									 boolean nested)
	{
		super(cm, DataDictionaryContext.CONTEXT_ID);

		this.dataDictionary = dataDictionary;
		this.nested = nested;
	}

	DataDictionary			dataDictionary;
}

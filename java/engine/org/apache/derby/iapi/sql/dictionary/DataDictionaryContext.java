/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.dictionary
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.services.context.Context;

/**
 *
 * DataDictionaryContext stores the data dictionary to be used by
 * the language module.  Stack compiler contexts when a new, local 
 * data dictionary is needed.
 *
 */

public interface DataDictionaryContext extends Context
{
	// this is the ID we expect data dictionary contexts
	// to be stored into a context manager under.
	public static final String CONTEXT_ID = "DataDictionaryContext";

	/**
	 * Get the DataDictionaty from this DataDictionaryContext.
	 *
	 * @return	The data dictionary associated with this
	 *		DataDictionaryContext
	 *
	 */

	public DataDictionary getDataDictionary();
}

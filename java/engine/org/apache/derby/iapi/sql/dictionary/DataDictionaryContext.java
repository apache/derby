/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.DataDictionaryContext

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

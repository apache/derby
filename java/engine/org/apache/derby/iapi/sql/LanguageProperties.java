/*

   Derby - Class org.apache.derby.iapi.sql.LanguageProperties

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.sql;

/**
 * This is a holder of language properties that are
 * exposed users.  Consolodate all properties here.
 */
public interface LanguageProperties
{
	/*
	** BulkFetch
	**
	** The default size needs some explaining.  As
	** of 7/14/98, the most efficient way for access
	** to return rows from a table is basically by
	** reading/qualifying/returning all the rows in
	** one page.  If you are read in many many rows
	** at a time the performance gain is only marginally
	** better.  Anyway, since even a small number of
	** rows per read helps, and since there is no good
	** way to get access to retrieve the rows page
	** by page, we use 16 totally arbitrarily.  Ultimately,
	** this should be dynamically sized -- in which
	** case we wouldn't need this default.
	*/
    static final String BULK_FETCH_PROP = "derby.language.bulkFetchDefault";
    static final String BULK_FETCH_DEFAULT = "16";
    static final int BULK_FETCH_DEFAULT_INT = 16;
}

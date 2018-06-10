/*

   Derby - Class org.apache.derby.optional.api.LuceneIndexDescriptor

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.optional.api;

import java.sql.SQLException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.QueryParser;

/**
 * <p>
 * A descriptor for how a Lucene index is created and queried.
 * </p>
 */
public interface LuceneIndexDescriptor
{
    /**
     * Get the names of the fields which are created when text is indexed.
     * These fields can be mentioned later on when querying the index.
     *
     * @return an array of field names
     */
    public  String[]    getFieldNames();
    
    /**
     * Get the Analyzer used to create index terms
     *
     * @return the Analyzer
     *
     * @throws SQLException on error
     */
    public Analyzer getAnalyzer()   throws SQLException;

    /**
     * Get the QueryParser used to parse query text
     *
     * @return the QueryParser
     * @throws SQLException on error
     */
    public  QueryParser getQueryParser()    throws SQLException;
}

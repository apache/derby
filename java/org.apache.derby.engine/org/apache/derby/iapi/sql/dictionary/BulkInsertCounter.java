/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.BulkInsertCounter

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.types.NumberDataValue;

/**
 * Thin wrapper around a sequence generator to support the bulk-insert
 * optimization used by InsertResultSet.
 */

public interface BulkInsertCounter
{
    /**
     * <p>
     * Get the next sequence number for bulk-insert.
     * </p>
     *
     * @param returnValue This value is stuffed with the new sequence number.
     */
    public void getCurrentValueAndAdvance
        ( NumberDataValue returnValue ) throws StandardException;

    /**
     * <p>
     * Get the current value of the sequence generator without advancing it.
     * May return null if the generator is exhausted.
     * </p>
     */
    public Long peekAtCurrentValue() throws StandardException;
    
}


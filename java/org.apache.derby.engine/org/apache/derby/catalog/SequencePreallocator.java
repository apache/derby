/*

   Derby - Class org.apache.derby.catalog.SequencePreallocator

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

package org.apache.derby.catalog;

import java.sql.SQLException;

/**
 * <p>
 * Logic to determine how many values to pre-allocate for a sequence.
 * By default, Derby boosts concurrency by pre-allocating ranges of numbers for sequences.
 * During orderly database shutdown, the unused numbers are reclaimed so that shutdown will
 * not create holes in the sequences.  However, holes may appear if the application fails to shut
 * down its databases before the JVM exits.
 * </p>
 *
 * <p>
 * Logic in this class is called every time Derby needs to pre-allocate a new range of sequence
 * values. Users can override Derby's default behavior by writing their own implementation of this
 * interface and then setting the following Derby property:
 * </p>
 *
 * <pre>
 *  -Dderby.language.sequence.preallocator=com.acme.MySequencePreallocator
 * </pre>
 *
 * <p>
 * Classes which implement this interface must also provide a public 0-arg constructor so
 * that Derby can instantiate them. Derby will instantiate a SequencePreallocator for every sequence.
 * </p>
 *
 */
public  interface   SequencePreallocator
{
    /**
     * <p>
     * This method returns the size of the next pre-allocated range for the specified
     * sequence. Names are case-sensitive, as specified in CREATE SEQUENCE
     * and CREATE TABLE statements.
     * </p>
     *
     * @param schemaName Name of schema holding the sequence.
     * @param sequenceName Specific name of the sequence.
     *
     * @return the size of the next pre-allocated range
     */
    public  int nextRangeSize
        (
         String schemaName,
         String sequenceName
         );
    
}




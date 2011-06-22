/*

   Derby - Class org.apache.derby.impl.sql.catalog.SequenceRange

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

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.catalog.SequencePreallocator;

/**
 * <p>
 * Default Derby logic for determining how many values to pre-allocate for an
 * identity column or sequence.
 * </p>
 */
public  class   SequenceRange   implements  SequencePreallocator
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Default number of values to pre-allocate. In the future, we may want to provide
     * something more sophisticated. For instance, we might want to make Derby tune
     * this number per sequence generator or give the user the power to override Derby's
     * decision.
     */
    private static final int DEFAULT_PREALLOCATION_COUNT = 5;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** <p>0-arg constructore needed to satisfy the SequencePreallocator contract.</p> */
    public  SequenceRange() {}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // SequencePreallocator BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  int nextRangeSize
        (
         String schemaName,
         String sequenceName
         )
    {
        return DEFAULT_PREALLOCATION_COUNT;
    }


}

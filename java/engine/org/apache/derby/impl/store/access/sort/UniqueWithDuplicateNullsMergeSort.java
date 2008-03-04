/*
 
   Derby - Class org.apache.derby.impl.store.access.sort.UniqueWithDuplicateNullsMergeSort
 
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

package org.apache.derby.impl.store.access.sort;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * This class extends and customizes MergeSort to support almost unique index.
 * It overrides compare method to consider keypart - 1 parts of the keys while
 * comparing (only for non null keys).
 */
final class UniqueWithDuplicateNullsMergeSort extends MergeSort {
    
    /**
     * Compares two sets of keys. If all the parts of the keys are not null
     * keys.length - 1 part is compared other wise all the parts are compared.
     * This methods assumes that last part is location.
     * @param r1 keys 
     * @param r2 keys
     * @return 0 for duplicates non zero for distinct keys 
     */
    protected int compare(DataValueDescriptor[] r1, DataValueDescriptor[] r2)
    throws StandardException {
        // Get the number of columns we have to compare.
        int colsToCompare = columnOrdering.length;
        int r;

        // Compare the columns specified in the column
        // ordering array.
        boolean nonull = true;
        for (int i = 0; i < colsToCompare; i++) {
            if (i == colsToCompare - 1 && nonull)
                return 0;
            // Get columns to compare.
            int colid = columnOrderingMap[i];
            boolean nullsLow = columnOrderingNullsLowMap[i];
            
            // If the columns don't compare equal, we're done.
            // Return the sense of the comparison.
            if ((r = r1[colid].compare(r2[colid], nullsLow))
            != 0) {
                if (this.columnOrderingAscendingMap[i])
                    return r;
                else
                    return -r;
            } else {
                if (r1[colid].isNull())
                    nonull = false;
            }
        }
        
        // We made it through all the columns, and they must have
        // all compared equal.  So return that the rows compare equal.
        return 0;
    }
    
}

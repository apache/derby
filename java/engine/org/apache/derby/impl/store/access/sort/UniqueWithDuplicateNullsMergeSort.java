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
 * This class extends and customizes MergeSort to support unique indexes with
 * duplicate nulls.
 * It overrides compare method to consider keypart - 1 parts of the keys while
 * comparing (only for non null keys).
 */
final class UniqueWithDuplicateNullsMergeSort extends MergeSort {
    
    /**
     * Compares two keys. 
     *
     * If all the parts of the keys are not null then the leading 
     * (keys.length - 1) parts are compared, else if a part of the key
     * is null then all parts of the key are compared (keys.length).
     *
     * This behavior is useful for implementing unique constraints where
     * multiple null values are allowed, but uniqueness must still be 
     * guaranteed for keys with no null values.   In this case the leading
     * parts of the key are the user key columns, while the last column
     * is a system provided column which is guaranteed unique per base row.
     *
     * @param r1 keys 
     * @param r2 keys
     *
     * @return 0 for duplicates non zero for distinct keys 
     */
    @Override
    protected int compare(DataValueDescriptor[] r1, DataValueDescriptor[] r2)
    throws StandardException {
        // Get the number of columns we have to compare.
        int colsToCompare = columnOrdering.length;
        int r;

        // Compare the columns specified in the column ordering array.
        boolean nonull = true;
        for (int i = 0; i < colsToCompare; i++) {
            //if there are any nulls in the row nonull will be false
            //
            //if there was no nulls in the row and we are about to 
            //compare the last field (all fields except for the location
            //are same), treat them as duplicate.   This is used by caller
            //to implement unique key while ignoring case of keys with
            //null values.
            //
            //if at least one field was null, go ahead and compare the 
            //location too.  This is used to provide proper sorting of
            //duplicate keys with nulls, they must be ordered properly 
            //according to the last field also.
            if (i == colsToCompare - 1 && nonull) {
                if (sortObserver.deferred()) {
                    sortObserver.rememberDuplicate(r1);
                } else {
                    return 0;
                }
            }

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
                //set nonull to false if the fields are equal and null
                if (r1[colid].isNull())
                    nonull = false;
            }
        }
        
        // We made it through all the columns, and they must have
        // all compared equal.  So return that the rows compare equal.
        return 0;
    }
}

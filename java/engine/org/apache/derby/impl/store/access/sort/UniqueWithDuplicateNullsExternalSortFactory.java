/*

   Derby - Class org.apache.derby.impl.store.access.sort.UniqueWithDuplicateNullsExternalSortFactory

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

// for javadoc
import org.apache.derby.iapi.store.access.conglomerate.MethodFactory;

/**
 * Method factory to support sorting of Almost unique index. This class 
 * overrides getMergeSort of ExternalSortFactory to return UniqueWithDuplicateNullsMergeSort.
 */
public class UniqueWithDuplicateNullsExternalSortFactory 
    extends ExternalSortFactory 
{
    private static final String IMPLEMENTATIONID = 
        "sort almost unique external";
    
    protected MergeSort getMergeSort() 
    {
        return new UniqueWithDuplicateNullsMergeSort ();
    }

    /**
     * @see MethodFactory#primaryImplementationType
     */
    public String primaryImplementationType() 
    {
        return IMPLEMENTATIONID;
    }

    /**
     * @see MethodFactory#supportsImplementation
     */
    public boolean supportsImplementation(String implementationId) 
    {
        return IMPLEMENTATIONID.equals(implementationId);
    }
}

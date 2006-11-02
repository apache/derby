/*

   Derby - Class org.apache.derby.impl.store.raw.data.BaseDataFileFactoryJ4

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

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.services.cache.Cacheable;

/**
 * This class overloads BaseDataFileFactory to produce RAFContainer4 objects
 * instead of RAFContainer objects. It makes no other change to its superclass'
 * behavior.
 */
public class BaseDataFileFactoryJ4 extends BaseDataFileFactory {

    /**
     * Do-nothing constructor (as in the superclass) - real initialization
     * is done by super's boot().
     */
    public BaseDataFileFactoryJ4() {
    }

    /**
     * Overrides newRAFContainer in BaseDataFileFactory to produce RAFContainer4
     * objects capable of exploiting the NIO API available in Java 1.4+
     */
    protected Cacheable newRAFContainer(BaseDataFileFactory factory) {
        return new RAFContainer4(factory);
    }
}

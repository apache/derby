/*

Derby - Class org.apache.derbyDemo.vtis.snapshot.SnapshotQuery

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

package org.apache.derbyDemo.vtis.snapshot;

import java.lang.annotation.*;

/**
 * <p>
 * This is an Annotation describing the query needed to materialize a ResultSet
 * from a foreign database. The driver name and connection url must still be
 * specified at run time.
 * </p>
 *
  */
@Retention( value=RetentionPolicy.RUNTIME )
public  @interface  SnapshotQuery
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** The names of the parameters passed to the query. These are parameter
     * names mentioned in the SubscriptionSignature of the enclosing
     * Subscription class */
    String[]    parameters();

    /** The query string that is passed to the foreign database */
    String      query();
    
    
}

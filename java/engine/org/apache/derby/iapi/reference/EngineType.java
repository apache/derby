/*

   Derby - Class org.apache.derby.iapi.reference.EngineType

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

package org.apache.derby.iapi.reference;

/**
 * Derby engine types. Enumerate different modes the
 * emmbedded engine (JDBC driver, SQL langauge layer and
 * store) can run in. A module can query the monitor to
 * see what type of service is being requested in terms
 * of its engine type and then use that in a decision
 * as to if it is suitable.
 * 
 * @see org.apache.derby.iapi.services.monitor.ModuleSupportable
 * @see org.apache.derby.iapi.services.monitor.Monitor#isDesiredType(Properties, int)
 * @see org.apache.derby.iapi.services.monitor.Monitor#getEngineType(Properties)
 *
 */
public interface EngineType {
    /**
     * Full database engine, the typical configuration.
     */
    int STANDALONE_DB = 0x00000002;
    
    /**
     * A JDBC engine with a query language layer but no
     * store layer executing. More used a a building block
     * for functionality built on top of a runtime SQL
     * engine, such as syntax checking.
     */
    int STORELESS_ENGINE = 0x00000080;
    
    /**
     * Property used to define the type of engine required.
     * If not set defaults to STANDALONE_DB.
     */
    String PROPERTY = "derby.engineType";
}
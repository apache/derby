/*

   Derby - Class org.apache.derby.client.net.ResultSetRequestInterface

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

package org.apache.derby.client.net;

import org.apache.derby.client.am.Section;
import org.apache.derby.client.am.SqlException;

// In general, required data is passed.
// In addition, ResultSet objects are passed for convenient access to any material result set caches.
// Implementations of this interface should not dereference common layer ResultSet state, as it is passed in,
// but may dereference material layer ResultSet state if necessary for performance.

interface ResultSetRequestInterface {
    public void writeFetch(NetResultSet resultSet,
                           Section section,
                           int fetchSize) throws SqlException;

    public void writeScrollableFetch(
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        NetResultSet resultSet,
        Section section,
        int fetchSize,
        int orientation,
        long rowToFetch,
        boolean resetQueryBlocks) throws SqlException;

    public void writePositioningFetch(NetResultSet resultSet,
                                      Section section,
                                      int orientation,
                                      long rowToFetch) throws SqlException;

    public void writeCursorClose(NetResultSet resultSet,
                                 Section section) throws SqlException;

}

/*

   Derby - Class org.apache.derby.iapi.jdbc.EmbeddedXADataSourceInterface

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

package org.apache.derby.iapi.jdbc;

import java.sql.ShardingKeyBuilder;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import org.apache.derby.iapi.jdbc.ResourceAdapter;


/**
 * Common interface of Derby embedded XA data sources.
 */
public interface EmbeddedXADataSourceInterface extends EmbeddedDataSourceInterface, javax.sql.XADataSource
{
    public ResourceAdapter getResourceAdapter();

    /** Added by Java 9 */
    public default ShardingKeyBuilder createShardingKeyBuilder()
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
//IC see: https://issues.apache.org/jira/browse/DERBY-6974
        throws SQLException
    {
        throw new java.sql.SQLFeatureNotSupportedException();
    }
}

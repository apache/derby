/*

   Derby - Class org.apache.derby.client.net.ConnectionRequestInterface

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

// In general, required data is passed.
// In addition, Connection objects are passed for convenient access to any material connection caches.
// Implementations of this interface should not dereference common layer Connection state, as it is passed in,
// but may dereference material layer Connection state if necessary for performance.

public interface ConnectionRequestInterface {
    public void writeCommitSubstitute(NetConnection connection) throws org.apache.derby.client.am.SqlException;

    public void writeLocalCommit(NetConnection connection) throws org.apache.derby.client.am.SqlException;

    public void writeLocalRollback(NetConnection connection) throws org.apache.derby.client.am.SqlException;

    public void writeLocalXAStart(NetConnection connection) throws org.apache.derby.client.am.SqlException;

    public void writeLocalXACommit(NetConnection connection) throws org.apache.derby.client.am.SqlException;

    public void writeLocalXARollback(NetConnection connection) throws org.apache.derby.client.am.SqlException;
}

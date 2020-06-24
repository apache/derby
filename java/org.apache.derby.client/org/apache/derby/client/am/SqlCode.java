/*

   Derby - Class org.apache.derby.client.am.SqlCode

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

package org.apache.derby.client.am;

// This class is for strong-typing.
//
// Dnc architected codes in the range +/- 4200 to 4299, plus one additional code for -4499.
//
// SQL codes are architected by the product that issues them.
//

public class SqlCode {
    private int code_;

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    SqlCode(int code) {
        code_ = code;
    }

    /**
     * Return the SQL code represented by this instance.
     *
     * @return an SQL code
     */
    public final int getCode() {
        return code_;
    }

    public final static SqlCode queuedXAError = new SqlCode(-4203);

    final static SqlCode disconnectError = new SqlCode(40000);
//IC see: https://issues.apache.org/jira/browse/DERBY-6125

    /** SQL code for SQL state 02000 (end of data). DRDA does not
     * specify the SQL code for this SQL state, but Derby uses 100. */
    public final static SqlCode END_OF_DATA = new SqlCode(100);
}

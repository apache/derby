/*

   Derby - Class org.apache.derby.shared.common.reference.JDBC40Translation

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

package org.apache.derby.shared.common.reference;

/**
        This class contains public statics that map directly to the
        new public statics in the jdbc 4.0 interfaces.  By providing
        an intermediary class with hard-coded copies of constants that
        will be available in jdbc 4.0, it becomes possible to refer to
        these constants when compiling against older jdk versions.

//IC see: https://issues.apache.org/jira/browse/DERBY-2438
        <p>
        This class also contains some constants shared by the network server and client.
        </p>

        <p>
        This class should not be shipped with the product.
        </p>

        <p>
        This class has no methods, all it contains are constants
        are public, static and final since they are declared in an interface.
        </p>
*/

public interface JDBC40Translation {
    // Constants shared by network client and server
    public static final int DEFAULT_COLUMN_DISPLAY_SIZE = 15;
    public static final int UNKNOWN_SCALE = 0;
    public static final int UNKNOWN_PRECISION = 0;

    // constants from java.sql.Types
    public static final int REF_CURSOR = 2012;
}

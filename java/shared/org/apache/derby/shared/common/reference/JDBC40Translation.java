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

        <P>The test <code>jdbc4/JDBC40TranslationTest.junit</code>,
        which is compiled against jdk16, contains tests that verifies
        that these hard coded constants are in fact equal to those
        found in jdk16.

        <P>
        This class should not be shipped with the product.

        <P>
        This class has no methods, all it contains are constants
        are public, static and final since they are declared in an interface.
*/

public interface JDBC40Translation {
    /*
    ** public statics from 4.0 version of java.sql.DatabaseMetaData
    */
    public static final int FUNCTION_PARAMETER_UNKNOWN = 0;
    public static final int FUNCTION_PARAMETER_IN      = 1;
    public static final int FUNCTION_PARAMETER_INOUT   = 2;
    public static final int FUNCTION_PARAMETER_OUT     = 3;
    public static final int FUNCTION_RETURN            = 4;
    
    public static final int FUNCTION_NO_NULLS          = 0;
    public static final int FUNCTION_NULLABLE          = 1;
    public static final int FUNCTION_NULLABLE_UNKNOWN  = 2;

    // constants from java.sql.Types
    public static final int NCHAR = -8;
    public static final int NVARCHAR = -9;
    public static final int LONGNVARCHAR = -10;
    public static final int NCLOB = 2007;
    public static final int ROWID = 2008;
    public static final int SQLXML = 2009;
}

/*

   Derby - Class org.apache.derby.iapi.reference.JDBC20Translation

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.reference;

import java.sql.ResultSet;
import javax.transaction.xa.XAResource;
import java.sql.Types;

/**
        This class contains public statics that map directly
        to the new public statics in the jdbc 2.0 classes.
        By providing an intermediary class, we can use the
        same statics without having to import the jdbc 2.0 classes
        into other classes.


        <P>
        This class should not be shipped with the product.

        <P>
        This class has no methods, all it contains are constants
        are public, static and final since they are declared in an interface.
*/

public interface JDBC20Translation {
        /*
        ** public statics from 2.0 version of java.sql.ResultSet
        */

        /**
         *      java.sql.ResultSet - result set concurrency
         */
        public static final int CONCUR_READ_ONLY = ResultSet.CONCUR_READ_ONLY;

        public static final int CONCUR_UPDATABLE = ResultSet.CONCUR_UPDATABLE;

        /**
         *      java.sql.ResultSet - result set type
         */
        public static final int TYPE_FORWARD_ONLY = ResultSet.TYPE_FORWARD_ONLY;
        public static final int TYPE_SCROLL_INSENSITIVE = ResultSet.TYPE_SCROLL_INSENSITIVE;
        public static final int TYPE_SCROLL_SENSITIVE = ResultSet.TYPE_SCROLL_SENSITIVE;

        /**
         *      java.sql.ResultSet - fetch direction
         */
        public static final int FETCH_FORWARD = ResultSet.FETCH_FORWARD;
        public static final int FETCH_REVERSE = ResultSet.FETCH_REVERSE;
        public static final int FETCH_UNKNOWN = ResultSet.FETCH_UNKNOWN;

        /*
        ** public statics from javax.transaction.xa.XAResource
        */
        public static final int XA_ENDRSCAN = XAResource.TMENDRSCAN;
        public static final int XA_FAIL = XAResource.TMFAIL;
        public static final int XA_JOIN = XAResource.TMJOIN;
        public static final int XA_NOFLAGS = XAResource.TMNOFLAGS;
        public static final int XA_RESUME = XAResource.TMRESUME;
        public static final int XA_STARTRSCAN = XAResource.TMSTARTRSCAN;
        public static final int XA_SUCCESS = XAResource.TMSUCCESS;
        public static final int XA_SUSPEND = XAResource.TMSUSPEND;


        /*
        ** New types in JDBC 2.0
        */
        public static final int SQL_TYPES_JAVA_OBJECT = Types.JAVA_OBJECT;
        public static final int SQL_TYPES_BLOB = Types.BLOB;
        public static final int SQL_TYPES_CLOB = Types.CLOB;
}

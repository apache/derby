/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.reference
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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

/*

   Derby - Class org.apache.derby.iapi.sql.compile.Node

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

package org.apache.derby.iapi.sql.compile;

import org.apache.derby.iapi.error.StandardException;

/**
 * Interface implemented by the nodes created by a {@code NodeFactory}. Callers
 * of the various {@code NodeFactory.getNode()} methods will typically cast the
 * returned node to a more specific sub-type, as this interface only contains
 * the methods needed by {@code NodeFactory} to initialize the node.
 */
public interface Node {

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1) throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2) throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3) throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3,
              Object arg4, Object arg5, Object arg6)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4,
              Object arg5, Object arg6, Object arg7)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4,
              Object arg5, Object arg6, Object arg7, Object arg8)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5,
              Object arg6, Object arg7, Object arg8, Object arg9)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5,
              Object arg6, Object arg7, Object arg8, Object arg9, Object arg10)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5,
              Object arg6, Object arg7, Object arg8, Object arg9, Object arg10,
              Object arg11)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5,
              Object arg6, Object arg7, Object arg8, Object arg9, Object arg10,
              Object arg11, Object arg12)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5,
              Object arg6, Object arg7, Object arg8, Object arg9, Object arg10,
              Object arg11, Object arg12, Object arg13)
            throws StandardException;

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException		Thrown on error
     */
    void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5,
              Object arg6, Object arg7, Object arg8, Object arg9, Object arg10,
              Object arg11, Object arg12, Object arg13, Object arg14)
            throws StandardException;
}

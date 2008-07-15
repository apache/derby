/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.RoleClosureIterator

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
package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.error.StandardException;

/**
 * Allows iterator over the role grant closure defined by the relation
 * GRANT role-a TO role-b, or its inverse.
 * @see DataDictionary#createRoleClosureIterator
 * @see org.apache.derby.impl.sql.catalog.RoleClosureIteratorImpl
 */
public interface RoleClosureIterator
{

    /**
     * Returns the next (as yet unreturned) role in the transitive closure of
     * the grant or grant<sup>-1</sup> relation.
     *
     * The grant relation forms a DAG (directed acyclic graph).
     * <pre>
     * Example:
     *      Assume a set of created roles forming nodes:
     *            {a1, a2, a3, b, c, d, e, f, h, j}
     *
     *      Assume a set of GRANT statements forming arcs:
     *
     *      GRANT a1 TO b;   GRANT b TO e;  GRANT e TO h;
     *      GRANT a1 TO c;                  GRANT e TO f;
     *      GRANT a2 TO c;   GRANT c TO f;  GRANT f TO h;
     *      GRANT a3 TO d;   GRANT d TO f;  GRANT a1 to j;
     *
     *
     *          a1            a2         a3
     *         / | \           |          |
     *        /  b  +--------> c          d
     *       j   |              \        /
     *           e---+           \      /
     *            \   \           \    /
     *             \   \---------+ \  /
     *              \             \_ f
     *               \             /
     *                \           /
     *                 \         /
     *                  \       /
     *                   \     /
     *                    \   /
     *                      h
     * </pre>
     * An iterator on the inverse relation starting at h for the above
     * grant graph will return:
     * <pre>
     *       closure(h, grant-inv) = {h, e, b, a1, f, c, a2, d, a3}
     * </pre>
     * <p>
     * An iterator on normal (not inverse) relation starting at a1 for
     * the above grant graph will return:
     * <pre>
     *       closure(a1, grant)    = {a1, b, j, e, h, f, c}
     * </pre>
     *
     * @return a role name identifying a yet unseen node, or null if the
     *         closure is exhausted.  The order in which the nodes are returned
     *         is not defined, except that the root is always returned first (h
     *         and a1 in the above examples).
     */
    public String next() throws StandardException;
}

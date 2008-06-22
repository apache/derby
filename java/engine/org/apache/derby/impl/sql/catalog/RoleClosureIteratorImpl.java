/*

   Derby - Class org.apache.derby.impl.sql.catalog.RoleClosureIteratorImpl

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

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.sql.dictionary.RoleGrantDescriptor;
import org.apache.derby.iapi.sql.dictionary.RoleClosureIterator;
import org.apache.derby.iapi.error.StandardException;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Allows iterator over the role grant closure defined by the relation
 * <code>GRANT</code> role-a <code>TO</code> role-b, or its inverse.
 * <p>
 * The graph is represented as a <code>HashMap</code> where the key is
 * the node and the value is a List grant descriptors representing
 * outgoing arcs. The set constructed depends on whether <code>inverse</code>
 * was specified in the constructor.
 * @see org.apache.derby.iapi.sql.dictionary.RoleClosureIterator
 */
public class RoleClosureIteratorImpl implements RoleClosureIterator
{
    /**
     * true if closure is inverse of GRANT role-a TO role-b.
     */
    private final boolean inverse;

    /**
     * Holds roles seen so far when computing the closure.
     * <ul>
     *   <li>Key: role name. Depending on value of {@code inverse}, the
     *       key represents and is compared against {@code roleName()}
     *       or {@code grantee()} of role descriptors visited.</li>
     *   <li>Value: none</li>
     * </ul>
     */
    private HashMap seenSoFar;

    /**
     * Holds the grant graph.
     * <ul>
     *   <li>key: role name</li>
     *   <li>value: list of {@code RoleGrantDescriptor}, making up outgoing arcs
     *        in graph</li>
     * </ul>
     */
    private HashMap graph;

    /**
     * Holds discovered, but not yet handed out, roles in the closure.
     */
    private List lifo;

    /**
     * Last node returned by next; a logical pointer into the arcs
     * list of a node we are currently processing.
     */
    private Iterator currNodeIter;

    /**
     * Constructor (package private).
     * Use {@code createRoleClosureIterator} to obtain an instance.
     * @see org.apache.derby.iapi.sql.dictionary.DataDictionary#createRoleClosureIterator
     *
     * @param root The role name for which to compute the closure
     * @param inverse If {@code true}, {@code graph} represents the
     *                grant<sup>-1</sup> relation.
     * @param graph The grant graph for which to construct a closure
     *              and iterator.
     *
     */
    RoleClosureIteratorImpl(String root, boolean inverse,
                            HashMap graph) {
        this.inverse = inverse;
        this.graph = graph;

        // we omit root from closure, so don't add it here.
        seenSoFar = new HashMap();
        lifo      = new ArrayList(); // remaining work stack
        // present iterator of outgoing arcs of the node we are
        // currently looking at
        List outgoingArcs = (List)graph.get(root);
        if (outgoingArcs != null) {
            this.currNodeIter = outgoingArcs.iterator();
        } else {
            // empty
            this.currNodeIter = new ArrayList().iterator();
        }


    }


    public String next() {
        RoleGrantDescriptor result = null;

        while (result == null) {
            while (currNodeIter.hasNext()) {
                RoleGrantDescriptor r =
                    (RoleGrantDescriptor)currNodeIter.next();

                if (seenSoFar.containsKey
                        (inverse ? r.getRoleName() : r.getGrantee())) {
                    continue;
                } else {
                    lifo.add(r);
                    result = r;
                    break;
                }
            }

            if (result == null) {
                // not more candidates located outgoing from the
                // latest found node, pick another and continue
                RoleGrantDescriptor newNode = null;

                currNodeIter = null;

                while (lifo.size() > 0 && currNodeIter == null) {

                    newNode = (RoleGrantDescriptor)lifo.remove(lifo.size() - 1);

                    // In the example (see interface doc), the
                    // iterator of outgoing arcs for f (grant inverse)
                    // would contain {e,c,d}.
                    List outArcs = (List)graph.get(
                        inverse? newNode.getRoleName(): newNode.getGrantee());

                    if (outArcs != null) {
                        currNodeIter = outArcs.iterator();
                    } // else: leaf node, pop next candidate, if any
                }

                if (currNodeIter == null) {
                    // candidate stack is empty, done
                    currNodeIter = null;
                    break;
                }
            }
        }

        if (result != null) {
            seenSoFar.put(inverse ? result.getRoleName(): result.getGrantee(),
                          null);
            return inverse ? result.getRoleName() : result.getGrantee();
        } else {
            return null;
        }
    }


    public void close() throws StandardException{
        seenSoFar = null;
        graph = null;
        lifo = null;
        currNodeIter = null;
    }
}

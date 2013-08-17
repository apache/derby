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

import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.derby.iapi.sql.dictionary.RoleGrantDescriptor;
import org.apache.derby.iapi.sql.dictionary.RoleClosureIterator;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.store.access.TransactionController;

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
    private HashMap<String,Object> seenSoFar;

    /**
     * Holds the grant graph.
     * <ul>
     *   <li>key: role name</li>
     *   <li>value: list of {@code RoleGrantDescriptor}, making up outgoing arcs
     *        in graph</li>
     * </ul>
     */
    private HashMap<String,List<RoleGrantDescriptor>> graph;

    /**
     * Holds discovered, but not yet handed out, roles in the closure.
     */
    private List<RoleGrantDescriptor> lifo;

    /**
     * Last node returned by next; a logical pointer into the arcs
     * list of a node we are currently processing.
     */
    private Iterator<RoleGrantDescriptor> currNodeIter;

    /**
     * DataDictionaryImpl used to get closure graph
     */
    private DataDictionaryImpl dd;

    /**
     * TransactionController used to get closure graph
     */
    private TransactionController tc;

    /**
     * The role for which we compute the closure.
     */
    private String root;

    /**
     * true before next is called the first time
     */
    private boolean initial;

    /**
     * Constructor (package private).
     * Use {@code createRoleClosureIterator} to obtain an instance.
     * @see org.apache.derby.iapi.sql.dictionary.DataDictionary#createRoleClosureIterator
     *
     * @param root The role name for which to compute the closure
     * @param inverse If {@code true}, {@code graph} represents the
     *                grant<sup>-1</sup> relation.
     * @param dd data dictionary
     * @param tc transaction controller
     *
     */
    RoleClosureIteratorImpl(String root, boolean inverse,
                            DataDictionaryImpl dd,
                            TransactionController tc) {
        this.inverse = inverse;
        this.graph = null;
        this.root = root;
        this.dd = dd;
        this.tc = tc;
        seenSoFar = new HashMap<String,Object>();
        lifo      = new ArrayList<RoleGrantDescriptor>(); // remaining work stack

        RoleGrantDescriptor dummy = new RoleGrantDescriptor
            (null,
             null,
             inverse ? root : null,
             inverse ? null : root,
             null,
             false,
             false);
        List<RoleGrantDescriptor> dummyList = new ArrayList<RoleGrantDescriptor>();
        dummyList.add(dummy);
        currNodeIter = dummyList.iterator();
        initial = true;
    }


    public String next() throws StandardException {
        if (initial) {
            // Optimization so we don't compute the closure for the current
            // role if unnecessary (when next is only called once).
            initial = false;
            seenSoFar.put(root, null);

            return root;

        } else if (graph == null) {
            // We get here the second time next is called.
            graph = dd.getRoleGrantGraph(tc, inverse);
            List<RoleGrantDescriptor> outArcs = graph.get(root);
            if (outArcs != null) {
                currNodeIter = outArcs.iterator();
            }
        }

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

                    newNode = lifo.remove(lifo.size() - 1);

                    // In the example (see interface doc), the
                    // iterator of outgoing arcs for f (grant inverse)
                    // would contain {e,c,d}.
                    List<RoleGrantDescriptor> outArcs = graph.get(
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
            String role = inverse ? result.getRoleName(): result.getGrantee();
            seenSoFar.put(role, null);
            return role;
        } else {
            return null;
        }
    }
}

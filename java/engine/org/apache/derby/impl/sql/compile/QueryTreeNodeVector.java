/*

   Derby - Class org.apache.derby.impl.sql.compile.QueryTreeNodeVector

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

package	org.apache.derby.impl.sql.compile;

import java.util.ArrayList;
import java.util.Iterator;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.compile.Visitable;

/**
 * QueryTreeNodeVector is the root class for all lists of query tree nodes.
 * It provides a wrapper for java.util.ArrayList. All
 * lists of query tree nodes inherit from QueryTreeNodeVector.
 *
 */

class QueryTreeNodeVector<E extends QueryTreeNode> extends QueryTreeNode
                                                   implements Iterable<E>
{
    private final ArrayList<E> v = new ArrayList<E>();
    final Class<E> eltClass; // needed for cast in #acceptChildren

    QueryTreeNodeVector(Class<E> eltClass, ContextManager cm) {
        super(cm);
        this.eltClass = eltClass;
    }

	public final int size()
	{
		return v.size();
	}

    final E elementAt(int index)
	{
        return v.get(index);
	}

    void addElement(E qt)
	{
		v.add(qt);
	}

    final E removeElementAt(int index)
	{
        return v.remove(index);
	}

    final void removeElement(E qt)
	{
		v.remove(qt);
	}

    final int indexOf(E qt)
	{
		return v.indexOf(qt);
	}

    final void setElementAt(E qt, int index)
	{
		v.set(index, qt);
	}

    final void destructiveAppend(QueryTreeNodeVector<E> qtnv)
	{
		nondestructiveAppend(qtnv);
		qtnv.removeAllElements();
	}

    final void nondestructiveAppend(QueryTreeNodeVector<E> qtnv)
	{
        v.addAll(qtnv.v);
	}

	final void removeAllElements()
	{
		v.clear();
	}

    final void insertElementAt(E qt, int index)
	{
		v.add(index, qt);
	}


	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 * @param depth		The depth to indent the sub-nodes
	 */
    @Override
    void printSubNodes(int depth) {
		if (SanityManager.DEBUG) {
			for (int index = 0; index < size(); index++) {
				debugPrint(formatNodeString("[" + index + "]:", depth));
                E elt = elementAt(index);
				elt.treePrint(depth);
			}
		}
	}


	/**
	 * Accept the visitor for all visitable children of this node.
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
    @Override
	void acceptChildren(Visitor v)
		throws StandardException
	{
		super.acceptChildren(v);

		int size = size();
		for (int index = 0; index < size; index++)
		{
            Visitable vbl = elementAt(index).accept(v);
            setElementAt(eltClass.cast(vbl), index);
		}
	}

    /* Iterable interface */
    public final Iterator<E> iterator() {
        return v.iterator();
    }
}

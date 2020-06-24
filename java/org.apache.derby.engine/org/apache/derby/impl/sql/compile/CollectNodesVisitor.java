/*

   Derby - Class org.apache.derby.impl.sql.compile.CollectNodesVisitor

//IC see: https://issues.apache.org/jira/browse/DERBY-1377
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
import java.util.List;
import org.apache.derby.iapi.sql.compile.Visitable; 
import org.apache.derby.iapi.sql.compile.Visitor;

/**
 * Collect all nodes of the designated type to be returned
 * in a list.
 * <p>
 * Can find any type of node -- the class or class name
 * of the target node is passed in as a constructor
 * parameter.
 *
 */
public class CollectNodesVisitor<T extends Visitable> implements Visitor
{
    private final List<T> nodeList;
    private final Class<T> nodeClass;
    private final Class<? extends Visitable> skipOverClass;

	/**
	 * Construct a visitor
	 *
	 * @param nodeClass the class of the node that 
	 * 	we are looking for.
	 */
    public CollectNodesVisitor(Class<T> nodeClass)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6075
        this(nodeClass, null);
	}

	/**
	 * Construct a visitor
	 *
	 * @param nodeClass the class of the node that 
	 * 	we are looking for.
	 * @param skipOverClass do not go below this
	 * node when searching for nodeClass.
	 */
    public CollectNodesVisitor(Class<T> nodeClass,
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
                               Class<? extends Visitable> skipOverClass)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        this.nodeList = new ArrayList<T>();
        this.nodeClass = nodeClass;
		this.skipOverClass = skipOverClass;
	}

	public boolean visitChildrenFirst(Visitable node)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-4421
		return false;
	}

	public boolean stopTraversal()
	{
		return false;
	}
	////////////////////////////////////////////////
	//
	// VISITOR INTERFACE
	//
	////////////////////////////////////////////////

	/**
	 * If we have found the target node, we are done.
	 *
	 * @param node 	the node to process
	 *
	 * @return me
	 */
	public Visitable visit(Visitable node)
	{
		if (nodeClass.isInstance(node))
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            nodeList.add(nodeClass.cast(node));
		}
		return node;
	}

	/**
	 * Don't visit children under the skipOverClass
	 * node, if it isn't null.
	 *
	 * @return true/false
	 */
	public boolean skipChildren(Visitable node)
	{
		return (skipOverClass == null) ?
				false:
				skipOverClass.isInstance(node);
	}

	////////////////////////////////////////////////
	//
	// CLASS INTERFACE
	//
	////////////////////////////////////////////////
	/**
	 * Return the list of matching nodes.
     * The returned list may be empty, if there are no matching nodes. It
     * is never {@code null}.
	 *
	 */
	public List<T> getList()
	{
		return nodeList;
	}
}	

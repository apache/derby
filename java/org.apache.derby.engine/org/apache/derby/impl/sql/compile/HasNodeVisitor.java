/*

   Derby - Class org.apache.derby.impl.sql.compile.HasNodeVisitor

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

import org.apache.derby.iapi.sql.compile.Visitable; 
import org.apache.derby.iapi.sql.compile.Visitor;

/**
 * Find out if we have a particular node anywhere in the
 * tree.  Stop traversal as soon as we find one.
 * <p>
 * Can find any type of node -- the class or class name
 * of the target node is passed in as a constructor
 * parameter.
 *
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
class HasNodeVisitor implements Visitor
{
	protected boolean hasNode;
    private Class<?> nodeClass;
    private Class<?> skipOverClass;
	/**
	 * Construct a visitor
	 *
	 * @param nodeClass the class of the node that 
	 * 	we are looking for.
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
    HasNodeVisitor(Class<?> nodeClass)
	{
		this.nodeClass = nodeClass;
	}

	/**
	 * Construct a visitor
	 *
	 * @param nodeClass the class of the node that 
	 * 	we are looking for.
	 * @param skipOverClass do not go below this
	 * node when searching for nodeClass.
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
    HasNodeVisitor(Class<?> nodeClass, Class<?> skipOverClass)
	{
		this.nodeClass = nodeClass;
		this.skipOverClass = skipOverClass;
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
			hasNode = true;
		}
		return node;
	}

	/**
	 * Stop traversal if we found the target node
	 *
	 * @return true/false
	 */
	public boolean stopTraversal()
	{
		return hasNode;
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

	/**
	 * Visit parent before children.
	 */
	public boolean visitChildrenFirst(Visitable node)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-4421
		return false;
	}

	////////////////////////////////////////////////
	//
	// CLASS INTERFACE
	//
	////////////////////////////////////////////////
	/**
	 * Indicate whether we found the node in
	 * question
	 *
	 * @return true/false
	 */
    boolean hasNode()
	{
		return hasNode;
	}

	/**
	 * Reset the status so it can be run again.
	 *
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void reset()
	{
		hasNode = false;
	}
}	

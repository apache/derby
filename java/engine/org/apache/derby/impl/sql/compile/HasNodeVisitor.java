/*

   Derby - Class org.apache.derby.impl.sql.compile.HasNodeVisitor

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.Visitable; 
import org.apache.derby.iapi.sql.compile.Visitor;

import org.apache.derby.iapi.error.StandardException;

/**
 * Find out if we have a particular node anywhere in the
 * tree.  Stop traversal as soon as we find one.
 * <p>
 * Can find any type of node -- the class or class name
 * of the target node is passed in as a constructor
 * parameter.
 *
 * @author jamie
 */
public class HasNodeVisitor implements Visitor
{
	private boolean hasNode;
	private Class 	nodeClass;
	private Class	skipOverClass;
	/**
	 * Construct a visitor
	 *
	 * @param nodeClass the class of the node that 
	 * 	we are looking for.
	 */
	public HasNodeVisitor(Class nodeClass)
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
	public HasNodeVisitor(Class nodeClass, Class skipOverClass)
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
	 * Don't visit childen under the skipOverClass
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
	 * Indicate whether we found the node in
	 * question
	 *
	 * @return true/false
	 */
	public boolean hasNode()
	{
		return hasNode;
	}

	/**
	 * Reset the status so it can be run again.
	 *
	 */
	public void reset()
	{
		hasNode = false;
	}
}	

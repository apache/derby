/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.Visitable; 
import org.apache.derby.iapi.sql.compile.Visitor;

import org.apache.derby.iapi.error.StandardException;

import java.util.Vector;

/**
 * Collect all nodes of the designated type to be returned
 * in a vector.
 * <p>
 * Can find any type of node -- the class or class name
 * of the target node is passed in as a constructor
 * parameter.
 *
 * @author jamie
 */
public class CollectNodesVisitor implements Visitor
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	private Vector	nodeList;
	private Class 	nodeClass;
	private Class	skipOverClass;
	/**
	 * Construct a visitor
	 *
	 * @param nodeClass the class of the node that 
	 * 	we are looking for.
	 */
	public CollectNodesVisitor(Class nodeClass)
	{
		this.nodeClass = nodeClass;
		nodeList = new Vector();
	}

	/**
	 * Construct a visitor
	 *
	 * @param nodeClass the class of the node that 
	 * 	we are looking for.
	 * @param skipOverClass do not go below this
	 * node when searching for nodeClass.
	 */
	public CollectNodesVisitor(Class nodeClass, Class skipOverClass)
	{
		this(nodeClass);
		this.skipOverClass = skipOverClass;
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
			nodeList.addElement(node);	
		}
		return node;
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
	 * Reset the status so it can be run again.
	 *
	 */
	public Vector getList()
	{
		return nodeList;
	}
}	

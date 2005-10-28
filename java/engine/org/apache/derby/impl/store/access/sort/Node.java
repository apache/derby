/*

   Derby - Class org.apache.derby.impl.store.access.sort.Node

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.store.access.sort;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.execute.RowUtil; 

/**
	A node in a balanced binary tree.  This class is effectively a
	struct to the balanced tree class.

**/

final class Node
{
	public int balance;
	public Node leftLink;
	public Node rightLink;
	public DataValueDescriptor[] key;
	public int id;
	public Node dupChain;
	public int aux;

	public Node(int id)
	{
		this.id = id;
		reset();
	}

	public void reset()
	{
		balance = 0;
		leftLink = null;
		rightLink = null;
		key = null;
		dupChain = null;
		aux = 0;
		// Leave id alone
	}

	public Node link(int which)
	{
		if (which < 0)
			return leftLink;
		else
			return rightLink;
	}

	public void setLink(int which, Node l)
	{
		if (which < 0)
			leftLink = l;
		else
			rightLink = l;
	}

	DataValueDescriptor[] getKey()
	{
		return key;
	}

	public String toString()
	{
        if (SanityManager.DEBUG)
        {
            int lid = (leftLink == null) ? -1 : leftLink.id;
            int rid = (rightLink == null) ? -1 : rightLink.id;
            int did = (dupChain == null) ? -1 : dupChain.id;
            return "{" 
                + " id=" + id
                + " key=" + RowUtil.toString(key) 
                + " left=" + lid
                + " right=" + rid
                + " balance=" + balance
                + " dupChain=" + did
                + " aux= " + aux
                + " }";
        }
        else
        {
            return(null);
        }
	}
}

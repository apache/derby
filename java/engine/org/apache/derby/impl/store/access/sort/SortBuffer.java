/*

   Derby - Class org.apache.derby.impl.store.access.sort.SortBuffer

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
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.SortObserver;

import org.apache.derby.iapi.types.DataValueDescriptor;

/**

  This class implements an in-memory ordered set
  based on the balanced binary tree algorithm from
  Knuth Vol. 3, Sec. 6.2.3, pp. 451-471.
  Nodes are always maintained in order,
  so that inserts and deletes can be intermixed.
  <P>
  This algorithm will insert/delete N elements
  in O(N log(N)) time using O(N) space. 

**/

class SortBuffer
{
	/**
	Returned from insert when the row was inserted
	without incident.
	**/
	public static final int INSERT_OK = 0;

	/**
	Returned from insert when the row which was
	inserted was a duplicate.  The set will have
	aggregated it in.
	**/
	public static final int INSERT_DUPLICATE = 1;

	/**
	Returned from insert when the row was not able to be
	inserted because the set was full.
	**/
	public static final int INSERT_FULL = 2;

	/**
	The sort this set is associated with.
	**/
	private MergeSort sort;

	/**
	Where to allocate nodes from.
	**/
	private NodeAllocator allocator = null;

	/**
	Special head node for the tree.  Head.rightLink is the
	root of the tree.
	**/
	private Node head = null;

	/**
	The current height of the tree.
	**/
	private int height = 0;

	/**
	Set, as a side effect of deleteLeftMost (only), to the
	key from the node that was deleted from the tree.  This
	field is only valid after a call to deleteLeftMost.
	**/
	private DataValueDescriptor[] deletedKey;

	/**
	Set, as a side effect of deleteLeftMost and rotateRight,
	if the subtree they're working on decreased in height.
	This field is only valid after a call to deleteLeftMost
	or rotateRight.
	**/
	private boolean subtreeShrunk;

	/**
	Set by the setNextAux() method.  This value is stuffed
	into the aux field of the next node that's allocated.
	**/
	private int nextAux;

	/**
	Read by the getLastAux() method.  This value is read out
	of the last node that was deallocated from the tree.
	**/
	private int lastAux;

	/**
	Arrange that the next node allocated in the tree have
	it's aux field set to the argument.
	**/
	public void setNextAux(int aux)
	{
		nextAux = aux;
	}

	/**
	Retrieve the aux value from the last node deallocated
	from the tree.
	**/
	public int getLastAux()
	{
		return lastAux;
	}

	/**
	Construct doesn't do anything, callers must call init
	and check its return code.
	**/
	public SortBuffer(MergeSort sort)
	{
		this.sort = sort;
	}

	/**
	Initialize.  Returns false if the allocator
	couldn't be initialized.
	**/
	public boolean init()
	{
		allocator = new NodeAllocator();

		boolean initOK = false;

		if (sort.sortBufferMin > 0)
			initOK = allocator.init(sort.sortBufferMin, sort.sortBufferMax);
		else
			initOK = allocator.init(sort.sortBufferMax);

		if (initOK == false)
		{
			allocator = null;
			return false;
		}
		reset();
		return true;
	}

	public void reset()
	{
		allocator.reset();
		head = allocator.newNode();
		height = 0;
	}

	public void close()
	{
		if (allocator != null)
			allocator.close();
		allocator = null;
		height = 0;
		head = null;
	}

	/**
	Grow by a certain percent if we can
	*/
	public void grow(int percent)
	{
		if (percent > 0)
			allocator.grow(percent);
	}


	/**
	Return the number of elements this sorter can sort.
	It's the capacity of the node allocator minus one
	because the sorter uses one node for the head.
	**/
	public int capacity()
	{
		if (allocator == null)
			return 0;
		return allocator.capacity() - 1;
	}

	/**
	Insert a key k into the tree. Returns true if the
	key was inserted, false if the tree is full.  Silently
	ignores duplicate keys.
	<P>
	See Knuth Vol. 3, Sec. 6.2.3, pp. 455-457 for the algorithm.
	**/
	public int insert(DataValueDescriptor[] k)
		throws StandardException
	{
		int c;
		Node p, q, r, s, t;

		if (head.rightLink == null)
		{
			if ((sort.sortObserver != null) && 
				((k = sort.sortObserver.insertNonDuplicateKey(k)) == null))
			{
				return INSERT_DUPLICATE;
			}

			q = allocator.newNode();
			q.key = k;
			q.aux = nextAux;
			head.rightLink = q;
			height = 1;
			return INSERT_OK;
		}

		// [A1. Initialize]
		t = head;
		p = s = head.rightLink;

		// Search
		while (true)
		{
			// [A2. Compare]
			c = sort.compare(k, p.key);
			if (c == 0)
			{
				// The new key compares equal to the
				// current node's key.

				// See if we can use the aggregators
				// to get rid of the new key.
				if ((sort.sortObserver != null) &&
					((k = sort.sortObserver.insertDuplicateKey(k, p.key)) == null))
				{
					return INSERT_DUPLICATE;
				}

				// Keep the duplicate key...
				// Allocate a new node for the key.
				q = allocator.newNode();
				if (q == null)
					return INSERT_FULL;
				q.aux = nextAux;

				// Link the new node onto the current
				// node's duplicate chain.  The assumption
				// is made that a newly allocated node 
				// has null left and right links.
				q.key = k;
				q.dupChain = p.dupChain;
				p.dupChain = q;

				// From the caller's perspective this was
				// not a duplicate insertion.
				return INSERT_OK;
			}

			if (c < 0)
			{
				// [A3. Move left]
				q = p.leftLink;
				if (q == null)
				{
					q = allocator.newNode();
					if (q == null)
						return INSERT_FULL;
					q.aux = nextAux;
					p.leftLink = q;
					break;
				}
			}
			else // c > 0
			{
				// [A4. Move right]
				q = p.rightLink;
				if (q == null)
				{
					q = allocator.newNode();
					if (q == null)
						return INSERT_FULL;
					q.aux = nextAux;
					p.rightLink = q;
					break;
				}
			}

			if (q.balance != 0)
			{
				t = p;
				s = q;
			}
			p = q;
		}

		/*
		 * [A5. Insert]
		 * Node has been allocated and placed for k.
		 * Initialize it.
		 */

		if ((sort.sortObserver != null) && 
			((k = sort.sortObserver.insertNonDuplicateKey(k)) == null))
		{
			return INSERT_DUPLICATE;
		}
		q.key = k;

		/*
		 * [A6. Adjust balance factors for nodes between
		 * s and q]
		 */

		c = sort.compare(k, s.key);
		if (c < 0)
			r = p = s.leftLink;
		else
			r = p = s.rightLink;

		while (p != q)
		{
			if (sort.compare(k, p.key) < 0)
			{
				p.balance = -1;
				p = p.leftLink;
			}
			else // sort.compare(k, p.key) > 0
			{
				p.balance = 1;
				p = p.rightLink;
			}
		}

		// [A7. Balancing act]

		int a = (c > 0 ? 1 : ((c == 0) ? 0 : -1));

		if (s.balance == 0)
		{
			//debug("A7 (i). The tree has gotten higher");
			s.balance = a;
			height++;
			return INSERT_OK;
		}

		if (s.balance == -a)
		{
			//debug("A7 (ii). The tree has gotten more balanced");
			s.balance = 0;
			return INSERT_OK;
		}

		//debug("A7 (iii). The tree has gotten more out of balance");

		// s.balance == a
		if (r.balance == a)
		{
			//debug("A8. Single rotation");
			p = r;
			s.setLink(a, r.link(-a));
			r.setLink(-a, s);
			s.balance = 0;
			r.balance = 0;
		}
		else // r.balance == -a
		{
			//debug("A8. Double rotation");
			p = r.link(-a);
			r.setLink(-a, p.link(a));
			p.setLink(a, r);
			s.setLink(a, p.link(-a));
			p.setLink(-a, s);

			if (p.balance == a)
			{
				s.balance = -a;
				r.balance = 0;
			}
			else if (p.balance == 0)
			{
				s.balance = 0;
				r.balance = 0;
			}
			else // p.balance == -a
			{
				s.balance = 0;
				r.balance = a;
			}

			p.balance = 0;
		}

		// [A10. Finishing touch]
		if (s == t.rightLink)
			t.rightLink = p;
		else
			t.leftLink = p;

		return INSERT_OK;
	}

	/**
	Return the lowest key and delete it from 
	the tree, preserving the balance of the tree.
	**/
	public DataValueDescriptor[] removeFirst()
	{
		if (head.rightLink == null)
			return null;
		head.rightLink = deleteLeftmost(head.rightLink);
		if (this.subtreeShrunk)
			height--;
		return this.deletedKey;
	}

	/**
	Delete the node with the lowest key from the subtree defined
	by 'thisNode', maintaining balance in the subtree.  Returns
	the node that should be the new root of the subtree.  This
	method sets this.subtreeShrunk if the subtree of thisNode
	decreased in height. Saves the key that was in the deleted
	node in 'deletedKey'.
	**/
	private Node deleteLeftmost(Node thisNode)
	{
		// If this node has no left child, we've found the
		// leftmost one, so delete it.
		if (thisNode.leftLink == null)
		{

			// See if the current node has duplicates.  If so, we'll
			// just return one of them and not change the tree.
			if (thisNode.dupChain != null)
			{
				Node dupNode = thisNode.dupChain;

				//System.out.println("deleteLeftmost(" + thisNode + "): found dup: " + dupNode);

				// Return the key from the duplicate.  Note that even
				// though the keys compare equal they may not be equal,
				// depending on how the column ordering was specified.
				this.deletedKey = dupNode.key;
				lastAux = dupNode.aux;

				// Unlink the dup node and free it.
				thisNode.dupChain = dupNode.dupChain;
				allocator.freeNode(dupNode);
				dupNode = null;

				// Tree is not changing height since we're just removing
				// a node from the duplicate chain.
				this.subtreeShrunk = false;

				// Preserve the current node as the root of this subtree..
				return thisNode;
			}
			else // thisNode.dupChain == null
			{
				//System.out.println("deleteLeftmost(" + thisNode + "): found key");

				// Key to return is this node's key.
				this.deletedKey = thisNode.key;
				lastAux = thisNode.aux;

				// We're removing this node, so it's subtree is shrinking
				// from height 1 to height 0.
				this.subtreeShrunk = true;

				// Save this node's right link which might be cleared
				// out by the allocator.
				Node newRoot = thisNode.rightLink;

				// Free the node we're deleting.
				allocator.freeNode(thisNode);

				// Rearrange the tree to put this node's right subtree where
				// this node was.
				return newRoot;
			}
		}

		// Since this wasn't the leftmost node, delete the leftmost
		// node from this node's left subtree.  This operation may
		// rearrange the subtree, including the possiblility that the
		// root note changed, so set the root of the left subtree to
		// what the delete operation wants it to be.
		thisNode.leftLink = deleteLeftmost(thisNode.leftLink);

		// If the left subtree didn't change size, then this subtree
		// could not have changed size either.
		if (this.subtreeShrunk == false)
			return thisNode;

 		// If the delete operation shrunk the subtree, we may have
		// some rebalancing to do.

		if (thisNode.balance == 1)
		{
			// Tree got more unbalanced.  Need to do some
			// kind of rotation to fix it.  The rotateRight()
			// method will set subtreeShrunk appropriately
			// and return the node that should be the new
			// root of this subtree.
			return rotateRight(thisNode);
		}

		if (thisNode.balance == -1)
		{
			// Tree became more balanced
			thisNode.balance = 0;

			// Since the left subtree was higher, and it
			// shrunk, then this subtree shrunk, too.
			this.subtreeShrunk = true;
		}
		else // thisNode.balance == 0
		{
			// Tree became acceptably unbalanced
			thisNode.balance = 1;

			// We had a balanced tree, and just the left
			// subtree shrunk, so this subtree as a whole
			// has not changed in height.
			this.subtreeShrunk = false;
		}

		// We have not rearranged this subtree.
		return thisNode;
	}

	/**
	Perform either a single or double rotation, as appropriate, 
	to get the subtree 'thisNode' back in balance, assuming
	that the right subtree of 'thisNode' is higher than the
	left subtree.  Returns the node that should be the new
	root of the subtree.
	<P>
	These are the cases depicted in diagrams (1) and (2) of
	Knuth (p. 454), and the node names reflect these diagrams.
	However, in deletion, the single rotation may encounter
	a case where the "beta" and "gamma" subtrees are the same
	height (b.balance == 0), so the resulting does not always
	shrink.
	<P>
    Note: This code will not do the "mirror image" cases.
	It only works when the right subtree is the higher one
	(this is the only case encountered when deleting leftmost
	nodes from the tree).
	**/
	private Node rotateRight(Node thisNode)
	{
		Node a = thisNode;
		Node b = thisNode.rightLink;

		if (b.balance >= 0)
		{
			// single rotation

			a.rightLink = b.leftLink;
			b.leftLink = a;

			if (b.balance == 0)
			{
				a.balance = 1;
				b.balance = -1;
				this.subtreeShrunk = false;
			}
			else // b.balance == 1
			{
				a.balance = 0;
				b.balance = 0;
				this.subtreeShrunk = true;
			}

			return b;
		}
		else // b.balance == -1
		{
			// double rotation

			Node x = b.leftLink;

			a.rightLink = x.leftLink;
			x.leftLink = a;
			b.leftLink = x.rightLink;
			x.rightLink = b;

			if (x.balance == 1)
			{
				a.balance = -1;
				b.balance = 0;
			}
			else if (x.balance == -1)
			{
				a.balance = 0;
				b.balance = 1;
			}
			else // x.balance == 0
			{
				a.balance = 0;
				b.balance = 0;
			}
			x.balance = 0;

			this.subtreeShrunk = true;

			return x;
		}
	}

	public void check()
	{
        if (SanityManager.DEBUG)
        {
            String error = null;
            if (head.rightLink == null)
            {
                if (height != 0)
                    error = "empty tree with height " + height;
            } 
            else
            {
                if (depth(head.rightLink) != height)
                    error = "tree height " + height + " != depth " + depth(head.rightLink);
                else
                    error = checkNode(head.rightLink);
            }
            if (error != null)
            {
                System.out.println("ERROR: " + error);
                print();
                System.exit(1);
            }
        }
	}

	private String checkNode(Node n)
	{
        if (SanityManager.DEBUG)
        {
            if (n == null)
                return null;
            int ld = depth(n.leftLink);
            int rd = depth(n.rightLink);
            if (n.balance != (rd - ld))
                return "node " + n + ": left height " + ld + " right height " + rd;
            
            String e;
            e = checkNode(n.rightLink);
            if (e == null)
                e = checkNode(n.leftLink);
            return e;
        }
        else
        {
            return(null);
        }
	}

	private int depth(Node n)
	{
		int ld = 0;
		int rd = 0;
		if (n == null)
			return 0;
		if (n.leftLink != null)
			ld = depth(n.leftLink);
		if (n.rightLink != null)
			rd = depth(n.rightLink);
		if (rd > ld)
			return rd + 1;
		else
			return ld + 1;
	}

	public void print()
	{
		Node root = head.rightLink;
		System.out.println("tree height: " + height
			+ " root: " + ((root == null) ? -1 : root.id));
		if (root != null)
			printRecursive(root, 0);
	}

	private void printRecursive(Node n, int depth)
	{
		if (n.rightLink != null)
			printRecursive(n.rightLink, depth + 1);
		for (int i = 0; i < depth; i++)
			System.out.print("       ");
		System.out.println(n);
		if (n.leftLink != null)
			printRecursive(n.leftLink, depth + 1);
	}

	private void debug(String s)
	{
        if (SanityManager.DEBUG)
        {
            System.out.println(" === [" + s + "] ===");
        }
	}
}

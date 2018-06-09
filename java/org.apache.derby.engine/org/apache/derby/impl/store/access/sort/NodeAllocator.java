/*

   Derby - Class org.apache.derby.impl.store.access.sort.NodeAllocator

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

package org.apache.derby.impl.store.access.sort;

/**

  NodeAllocator manages an array of nodes which can be reused.

**/

final class NodeAllocator
{
	private static final int DEFAULT_INIT_SIZE = 128;
	private static final int GROWTH_MULTIPLIER = 2;
	private static final int DEFAULT_MAX_SIZE = 1024;

	private Node array[];
	private int maxSize;
	private int nAllocated;
	private Node freeList = null;
	
	/**
	Construct an empty allocator.  The caller must call
	init() before using it.
	**/
	public NodeAllocator()
	{
		array = null;
		nAllocated = 0;
		maxSize = 0;
	}

	public Node newNode()
	{
		// Caller forgot to init?
		if (array == null)
		{
			if (init() == false)
				return null;
		}

		if (freeList != null)
		{
			Node n = freeList;
			freeList = n.rightLink;
			n.rightLink = null;
			return n;
		}
		
		// Do we need to try reallocating the array?
		if (nAllocated == array.length)
		{
			// If the array is already the maximum size, then
			// tell the caller that there are no more nodes
			// available.
			if (array.length >= maxSize)
				return null;

            // Calculate the new length. The new array should be no longer
            // than maxSize. Use a long for the intermediate result to prevent
            // newLength from going negative due to integer overflow when
            // array.length is close to Integer.MAX_VALUE.
            int newLength = (int) Math.min(
                    (long) array.length * GROWTH_MULTIPLIER,
                    (long) maxSize);

			// Attempt to allocate a new array.  If the allocation
			// fails, tell the caller that there are no more
            // nodes available. The allocation may fail if there's
            // not enough memory to allocate a new array, or if the
            // JVM doesn't support that big arrays (some JVMs have
            // a limit on the array length that is different from
            // Integer.MAX_VALUE --- DERBY-4119).
            Node[] newArray;
            try {
                newArray = new Node[newLength];
            } catch (OutOfMemoryError oome) {
                // Could not allocate a larger array, so tell the caller that
                // there are no nodes available.
                return null;
            }

			// The new array was successfully allocated.  Copy the
			// nodes from the original array into it, and make the
			// new array the allocator's array.
            System.arraycopy(array, 0, newArray, 0, array.length);
			array = newArray;
		}

		// If this slot in the array hasn't had a node
		// allocated for it yet, do so now.
		if (array[nAllocated] == null)
			array[nAllocated] = new Node(nAllocated);

		// Return the node and increase the allocated count.
		return array[nAllocated++];
	}

	/**
	Return a node to the allocator.
	**/
	public void freeNode(Node n)
	{
		n.reset();
		n.rightLink = freeList;
		freeList = n;
	}

	/**
	Initialize the allocator with default values for
	initial and maximum size.  Returns false if sufficient
	memory could not be allocated.
	**/
	public boolean init()
	{
		return init(DEFAULT_INIT_SIZE, DEFAULT_MAX_SIZE);
	}

	/**
	Initialize the allocator with default values for
	initial size and the provided maximum size.
	Returns false if sufficient	memory could not be allocated.
	**/
	public boolean init(int maxSize)
	{
		return init(DEFAULT_INIT_SIZE, maxSize);
	}

	/**
	Initialize the allocator with the given initial and 
	maximum sizes.  This method does not check, but assumes
	that the value of initSize is less than the value of
	maxSize, and that they are both powers of two. Returns
	false if sufficient memory could not be allocated.
	**/
	public boolean init(int initSize, int maxSize)
	{
		this.maxSize = maxSize;
		if (maxSize < initSize)
			initSize = maxSize;
		array = new Node[initSize];
		if (array == null)
			return false;
		nAllocated = 0;
		return true;
	}

	/**
	Expand the node allocator's capacity by certain percent.
	**/
	public void grow(int percent)
	{
        if (percent > 0) { // cannot shrink
            // Calculate the new maximum size. Use long arithmetic so that
            // intermediate results don't overflow and make maxSize go
            // negative (DERBY-4119).
            maxSize = (int) Math.min(
                    (long) maxSize * (100 + percent) / 100,
                    (long) Integer.MAX_VALUE);
        }
	}

	/**
	Clear all nodes that this allocator has allocated.
	The allocator must already have been initialized.
	**/
	public void reset()
	{
		if (array == null)
			return;
		for (int i = 0; i < nAllocated; i++)
			array[i].reset();
		nAllocated = 0;
		freeList = null;
	}

	public void close()
	{
		array = null;
		nAllocated = 0;
		maxSize = 0;
		freeList = null;
	}

	public int capacity()
	{
		return maxSize;
	}
}

/*

   Derby - Class com.ihost.cs.JBitSet

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

package org.apache.derby.iapi.util;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.util.BitSet;

/**
 * JBitSet is a wrapper class for BitSet.  It is a fixed length implementation
 * which can be extended via the grow() method.  It provides additional
 * methods to manipulate BitSets.
 * NOTE: JBitSet was driven by the (current and perceived) needs of the
 * optimizer, but placed in the util package since it is not specific to
 * query trees..
 * NOTE: java.util.BitSet is final, so we must provide a wrapper class
 * which includes a BitSet member in order to extend the functionality.
 * We want to make it look like JBitSet extends BitSet, so we need to
 * provide wrapper methods for all of BitSet's methods.
 *
 * @author Jerry Brenner
 */
public final class JBitSet
{
	/* The BitSet that we'd like to extend */
	private final BitSet	bitSet;
	/* Cache size() of bitSet, since accessed a lot */
	private int		size;

	/**
	 * Construct a JBitSet of the specified size.
	 *
	 * @param size	The number of bits in the JBitSet.
	 */
	public JBitSet(int size)
	{
		bitSet = new BitSet(size);
		this.size = size;
	}

	/**
	 * Construct a JBitSet with the specified bitSet.
	 *
	 * @param bitSet	The BitSet.
	 * @param size		The size of bitSet.
	 *					NOTE: We need to specify the size since the size of a
	 *					BitSet is not guaranteed to be the same as JBitSet.size().
	 */
	private JBitSet(BitSet bitSet, int size)
	{
		this.bitSet = bitSet;
		this.size = size;
	}

	/**
	 * Set the BitSet to have the exact same bits set as the parameter's BitSet.
	 *
	 * @param sourceBitSet	The JBitSet to copy.
	 *
	 * @return Nothing.
	 */
	public void setTo(JBitSet sourceBitSet)
	{
        if (SanityManager.DEBUG)
        {
    		SanityManager.ASSERT(size == sourceBitSet.size(),
	    						 "JBitSets are expected to be the same size");
	    }
		/* High reuse solution */
		and(sourceBitSet);
		or(sourceBitSet);
	}

	/**
	 * Test to see if one JBitSet contains another one of
	 * the same size.
	 *
	 * @param jBitSet	JBitSet that we want to know if it is
	 *					a subset of current JBitSet
	 *
	 * @return boolean	Whether or not jBitSet is a subset.
	 */
	public boolean contains(JBitSet jBitSet)
	{
        if (SanityManager.DEBUG)
        {
    		SanityManager.ASSERT(size == jBitSet.size(),
	    						 "JBitSets are expected to be the same size");
	    }
		for (int bitIndex = 0; bitIndex < size; bitIndex++)
		{
			if (jBitSet.bitSet.get(bitIndex) && ! (bitSet.get(bitIndex)))
			{
				return false;
			}
		}
		return true;
	}

	/**
	 * See of a JBitSet has exactly 1 bit set.
	 *
	 * @return boolean	Whether or not JBitSet has a single bit set.
	 */
	public boolean hasSingleBitSet()
	{
		boolean found = false;

		for (int bitIndex = 0; bitIndex < size; bitIndex++)
		{
			if (bitSet.get(bitIndex))
			{
				if (found)
				{
					return false;
				}
				else
				{
					found = true;
				}
			}
		}

		return found;
	}

	/**
	 * Get the first set bit (starting at index 0) from a JBitSet.
	 *
	 * @return int	Index of first set bit, -1 if none set.
	 */
	public int getFirstSetBit()
	{
		for (int bitIndex = 0; bitIndex < size; bitIndex++)
		{
			if (bitSet.get(bitIndex))
			{
				return bitIndex;
			}
		}

		return -1;
	}

	/**
	 * Grow an existing JBitSet to the specified size.
	 *
	 * @param newSize	The new size
	 *
	 */
	public void grow(int newSize)
	{
        if (SanityManager.DEBUG)
        {
    		SanityManager.ASSERT(newSize > size,
	    					"New size is expected to be larger than current size");
	    }

		size = newSize;

	}

	/**
	 * Clear all of the bits in this JBitSet
	 *
	 * @return Nothing.
	 */
	public void clearAll()
	{
		for (int bitIndex = 0; bitIndex < size; bitIndex++)
		{
			if (bitSet.get(bitIndex))
			{
				bitSet.clear(bitIndex);
			}
		}
	}

	/* Wrapper methods for BitSet's methods */
	public String toString()
	{
		return bitSet.toString();
	}

	public boolean equals(Object obj)
	{
        if (SanityManager.DEBUG)
        {
    		SanityManager.ASSERT((obj instanceof JBitSet),
	    						 "obj is expected to be a JBitSet " + obj);
	    }
		return bitSet.equals(((JBitSet) obj).bitSet);
	}

	public int hashCode()
	{
		return bitSet.hashCode();
	}

	public Object clone()
	{
		return new JBitSet((BitSet) bitSet.clone(), size);
	}

	public boolean get(int bitIndex)
	{
		return bitSet.get(bitIndex);
	}

	public void set(int bitIndex)
	{
		bitSet.set(bitIndex);
	}

	public void clear(int bitIndex)
	{
		bitSet.clear(bitIndex);
	}

	public void and(JBitSet set)
	{
		bitSet.and(set.bitSet);
	}

	public void or(JBitSet set)
	{
		bitSet.or(set.bitSet);
	}

	public void xor(JBitSet set)
	{
		bitSet.xor(set.bitSet);
	}

	/**
	 * Return the size of bitSet
	 *
	 * @return int	Size of bitSet
	 */
	public int size()
	{
		return size;
	}
}


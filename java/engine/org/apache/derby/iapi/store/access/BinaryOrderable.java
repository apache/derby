/*

   Derby - Class org.apache.derby.iapi.store.access.BinaryOrderable

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

package org.apache.derby.iapi.store.access;

import org.apache.derby.iapi.types.Orderable;

import org.apache.derby.iapi.error.StandardException;

import java.io.ObjectInput;
import java.io.IOException;

/** 

  The Orderable interface represents a value that can
  be linearly ordered.
  <P>
  Currently only supports linear (<, =, <=) operations.
  Eventually we may want to do other types of orderings,
  in which case there would probably be a number of interfaces
  for each "class" of ordering.
  <P>
  The implementation must handle the comparison of null
  values.  This may require some changes to the interface,
  since (at least in some contexts) comparing a value with
  null should return unknown instead of true or false.

**/

public interface BinaryOrderable extends Orderable
{
	/**
	 * Compare this Orderable with a given Orderable for the purpose of
	 * index positioning.  This method treats nulls as ordered values -
	 * that is, it treats SQL null as equal to null and less than all
	 * other values.
	 *
	 * @param other		The Orderable to compare this one to.
	 *
	 * @return  <0 - this Orderable is less than other.
	 * 			 0 - this Orderable equals other.
	 *			>0 - this Orderable is greater than other.
     *
     *			The code should not explicitly look for -1, or 1.
	 *
	 * @exception IOException		Thrown on error
	 */
	int binarycompare(
    ObjectInput  in, 
    Orderable    other) 
        throws IOException;

	/**
	 * Compare this Orderable with a given Orderable for the purpose of
	 * qualification and sorting.  The caller gets to determine how nulls
	 * should be treated - they can either be ordered values or unknown
	 * values.
	 *
	 * @param op	Orderable.ORDER_OP_EQUALS means do an = comparison.
	 *				Orderable.ORDER_OP_LESSTHAN means compare this < other.
	 *				Orderable.ORDER_OP_LESSOREQUALS means compare this <= other.
	 * @param other	The Orderable to compare this one to.
	 * @param orderedNulls	True means to treat nulls as ordered values,
	 *						that is, treat SQL null as equal to null, and less
	 *						than all other values.
	 *						False means to treat nulls as unknown values,
	 *						that is, the result of any comparison with a null
	 *						is the UNKNOWN truth value.
	 * @param unknownRV		The return value to use if the result of the
	 *						comparison is the UNKNOWN truth value.  In other
	 *						words, if orderedNulls is false, and a null is
	 *						involved in the comparison, return unknownRV.
	 *						This parameter is not used orderedNulls is true.
	 *
	 * @return	true if the comparison is true.
	 *
	 * @exception IOException		Thrown on error
	 */
	boolean binarycompare(
    ObjectInput in,
    int         op, 
    Orderable   other,
    boolean     orderedNulls, 
    boolean     unknownRV)
				throws IOException;
}

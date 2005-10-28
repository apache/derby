/*

   Derby - Class org.apache.derby.iapi.store.access.KeyHasher

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
	Provides the ability to hash on multiple objects.
*/
public class KeyHasher 
{
	
	private final Object[] objects;

	public KeyHasher(int size)
	{
		objects = new Object[size];
	}

	/**
	 * Set array element at the specified index to the specified object.
	 *
	 * @param index		The specified index
	 * @param object	The specified object.
	 *
	 * @return	Nothing.
	 */
	public void setObject(int index, Object object)
	{
		objects[index] = object;
	}

	/**
	 * Get the object stored at the specified index.
	 *
	 * @param index	The specified index.
	 *
	 * @return The object stored in the array element.
	 */
	public Object getObject(int index)
	{
		return objects[index];
	}

	/**
	 * Static method to return the object to hash on.
	 * (Object stored in specifed array, if only a single
	 * object, otherwise a KeyHasher wrapping the
	 * objects to hash on.
	 * (NOTE: We optimize for in-memory hash tables, hence
	 * we only create a wrapper when needed.)
	 *
	 * @param objects	The array of objects to consider
	 * @param indexes	The indexes of the objects in the hash key.
	 *
	 * @return	The object to hash on.
	 */
	public static Object buildHashKey(Object[] objects,
									  int[] indexes)
	{
		// Hash on single object
		if (indexes.length == 1)
		{
			return objects[indexes[0]];
		}

		// Hash on multiple objects
		KeyHasher mh = new KeyHasher(indexes.length);
		for (int index = 0; index < indexes.length; index++)
		{
			mh.setObject(index, objects[indexes[index]]);
		}
		return mh;
	}

	/*
	** Methods from java.lang.Object
	*/

	public int hashCode()
	{
		int retval = 0;
		for (int index = 0; index < objects.length; index++)
		{
			retval += objects[index].hashCode();
		}

		return retval;
	}

	public boolean equals(Object obj)
	{
		if (!(obj instanceof KeyHasher))
			return false;

		KeyHasher mh = (KeyHasher) obj;

		if (mh.objects.length != objects.length)
			return false;

		for (int index = 0; index < objects.length; index++)
		{
			if (! (mh.objects[index].equals(objects[index])))
			{
				return false;
			}
		}

		return true;
	}
}

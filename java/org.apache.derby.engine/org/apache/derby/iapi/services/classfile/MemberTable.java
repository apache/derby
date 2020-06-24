/*

   Derby - Class org.apache.derby.iapi.services.classfile.MemberTable

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

package org.apache.derby.iapi.services.classfile;

import java.io.IOException;

import java.util.Hashtable;
import java.util.Vector;



class MemberTable {
	protected Vector<ClassMember> entries;
	private Hashtable<MemberTableHash,MemberTableHash> hashtable;
	private MemberTableHash	mutableMTH = null;

	public MemberTable(int count) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
		entries = new Vector<ClassMember>(count);
		hashtable = new Hashtable<MemberTableHash,MemberTableHash>((count > 50) ? count : 50);
		mutableMTH = new MemberTableHash(null, null);
	}

	void addEntry(ClassMember item) {
		MemberTableHash mth= new MemberTableHash(
									item.getName(), 
									item.getDescriptor(),
									entries.size());
		/* Add to the Vector */
		entries.add(item);
//IC see: https://issues.apache.org/jira/browse/DERBY-5060

		/* Add to the Hashtable */
		hashtable.put(mth, mth);
	}

	ClassMember find(String name, String descriptor) {

		/* Set up the mutable MTH for the search */
		mutableMTH.name = name;
		mutableMTH.descriptor = descriptor;
		mutableMTH.setHashCode();

		/* search the hash table */
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
		MemberTableHash mth = hashtable.get(mutableMTH);
		if (mth == null)
		{
			return null;
		}

		return entries.get(mth.index);
	}

	void put(ClassFormatOutput out) throws IOException {

		Vector<ClassMember> lentries = entries;
		int count = lentries.size();
		for (int i = 0; i < count; i++) {
			lentries.get(i).put(out);
		}
	}

	int size() {
		return entries.size();
	}

	int classFileSize() {
		int size = 0;

//IC see: https://issues.apache.org/jira/browse/DERBY-6213
		Vector<ClassMember> lentries = entries;
		int count = lentries.size();
		for (int i = 0; i < count; i++) {
			size += lentries.get(i).classFileSize();
		}

		return size;
	}
}

class MemberTableHash 
{
	String name;
	String descriptor;
	int	   index;
	int	   hashCode;
	
	MemberTableHash(String name, String descriptor, int index)
	{
		this.name = name;
		this.descriptor = descriptor;
		this.index = index;
		/* Only set hashCode if both name and descriptor are non-null */
		if (name != null && descriptor != null)
		{
			setHashCode();
		}
	}

	MemberTableHash(String name, String descriptor)
	{
		this(name, descriptor, -1);
	}

	void setHashCode()
	{
		hashCode = name.hashCode() + descriptor.hashCode();
	}

	public boolean equals(Object other)
	{
		MemberTableHash mth = (MemberTableHash) other;

		if (other == null)
		{
			return false;
		}

		if (name.equals(mth.name) && descriptor.equals(mth.descriptor))
		{
			return true;
		}
		else
		{
			return false;
		}
	}

	public int hashCode()
	{
		return hashCode;
	}
}







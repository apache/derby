/*

   Derby - Class org.apache.derby.iapi.services.classfile.ConstantPoolEntry

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.services.classfile;

import org.apache.derby.iapi.services.classfile.VMDescriptor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.io.IOException;

/** Constant Pool class - pages 92-99 */
public abstract class ConstantPoolEntry /*implements PoolEntry*/
{
	
	protected int tag; // u1 (page 83)
	protected boolean doubleSlot; // Some entries take up two slots! (see footnote page 98) 

	/* Index within Vector */
	protected int index;

	protected ConstantPoolEntry(int tag) {
		this.tag = tag;
	}

	int getIndex() {
		if (SanityManager.DEBUG) {
			if (index <= 0)
			{
				SanityManager.THROWASSERT("index is expected to be > 0, is " + index);
			}
		}
		return index;
	}

	void setIndex(int index) {
		this.index = index;
	}

	boolean doubleSlot() {
		return doubleSlot;
	}

	/**
		Return the key used to key this object in a hashtable
	*/
	Object getKey() {
		return this;
	}

	/**
		Return an estimate of the size of the constant pool entry.
	*/
	abstract int classFileSize();

	void put(ClassFormatOutput out) throws IOException {
		out.putU1(tag);
	}

	/*
	** Public API methods
	*/

	/**
		Return the tag or type of the entry. Will be equal to one of the
		constants above, e.g. CONSTANT_Class.
	*/
	final int getTag() {
		return tag;
	}

	/**	
		Get the first index in a index type pool entry.
		This call is valid when getTag() returns one of
		<UL> 
		<LI> CONSTANT_Class
		<LI> CONSTANT_Fieldref
		<LI> CONSTANT_Methodref
		<LI> CONSTANT_InterfaceMethodref
		<LI> CONSTANT_String
		<LI> CONSTANT_NameAndType
		</UL>
	*/
	int getI1() { return 0; }

	/**	
		Get the second index in a index type pool entry.
		This call is valid when getTag() returns one of
		<UL> 
		<LI> CONSTANT_Fieldref
		<LI> CONSTANT_Methodref
		<LI> CONSTANT_InterfaceMethodref
		<LI> CONSTANT_NameAndType
		</UL>
	*/	
	int getI2() { return 0; };
}


/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.classfile
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.classfile;

import java.util.Vector;

import java.io.IOException;

class Attributes extends Vector {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;
	private int classFileSize;

	Attributes(int count) {
		super(count);
	}

	void put(ClassFormatOutput out) throws IOException {
		int size = size();
		for (int i = 0; i < size; i++) {
			((AttributeEntry) elementAt(i)).put(out);
		}
	}

	int classFileSize() {
		return classFileSize;
	}

	/**
	*/

	void addEntry(AttributeEntry item) {
		addElement(item);
		classFileSize += item.classFileSize();
	}
}


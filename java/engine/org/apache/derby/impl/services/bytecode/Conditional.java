/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.bytecode
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.bytecode;
import org.apache.derby.iapi.services.classfile.VMOpcode;

/**
	A code chunk that gets pushed to handle if-else blocks.
	When this is created the mainChunk will already have
	the conditional check code.

     if condition
	 then code
	 else code

     what actually gets built is

     if !condition goto eb:
	  then code
	  goto end:
	 eb:
	  else code
	 end:
*/
class Conditional {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

	private final Conditional parent;
	private final int   ifOffset;
	private /*final*/ int	clearTo;
	private int thenGotoOffset;

	Conditional(Conditional parent, CodeChunk chunk, short ifOpcode, int clearTo) {
		this.parent = parent;
		ifOffset = chunk.getRelativePC();
		this.clearTo = clearTo;

		// reserve the space for the branch, will overwrite later
		chunk.addInstrU2(ifOpcode, 0);
	}

	int startElse(CodeChunk chunk, int thenSize) {

		thenGotoOffset = chunk.getRelativePC();

		// reserve space for the goto we will be adding
		chunk.addInstrU2(VMOpcode.GOTO, 0);

		// fill in the branch opcode
		fillIn(chunk, ifOffset);

		int ret = clearTo;
		clearTo = thenSize;
		return ret;
	}


	Conditional end(CodeChunk chunk, int elseSize) {

		if (thenGotoOffset == 0) {
			// no else condition
			fillIn(chunk, ifOffset);
		} else {
			fillIn(chunk, thenGotoOffset);
		}
		if (clearTo != elseSize) {
			throw new RuntimeException("mismatched sizes then " + clearTo + " else " + elseSize);
		}

		return parent;
	}

	private void fillIn(CodeChunk chunk, int where) {

		byte[] codeBytes = chunk.getCout().getData();

		int offset = chunk.getRelativePC() - where;

		where += 8;

		codeBytes[where + 1] = (byte)(offset >> 8 );
		codeBytes[where + 2] = (byte)(offset);
	}


}

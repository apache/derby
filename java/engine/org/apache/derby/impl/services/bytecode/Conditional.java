/*

   Derby - Class org.apache.derby.impl.services.bytecode.Conditional

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

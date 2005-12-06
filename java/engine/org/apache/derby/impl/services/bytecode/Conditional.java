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
import org.apache.derby.iapi.services.sanity.SanityManager;

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
	private Type[]	stack;
	private int thenGotoOffset;

	/**
	 * Start a conditional block.
	 * @param parent Current conditional block, null if no nesting is going on.
	 * @param chunk CodeChunk this conditional lives in
	 * @param ifOpcode Opcode for the if check.
	 * @param entryStack Type stack on entering the conditional then block.
	 */
	Conditional(Conditional parent, CodeChunk chunk, short ifOpcode, Type[] entryStack) {
		this.parent = parent;
		ifOffset = chunk.getRelativePC();
		this.stack = entryStack;

		// reserve the space for the branch, will overwrite later
		// with the correct branch offset.
		chunk.addInstrU2(ifOpcode, 0);
	}

	/**
	 * Complete the 'then' block and start the 'else' block for this conditional
	 * @param chunk CodeChunk this conditional lives in
	 * @param thenStack Type stack on completing the conditional then block.
	 * @return the type stack on entering the then block
	 */
	Type[] startElse(CodeChunk chunk, Type[] thenStack) {

		thenGotoOffset = chunk.getRelativePC();

		// reserve space for the goto we will be adding
		chunk.addInstrU2(VMOpcode.GOTO, 0);

		// fill in the branch opcode
		fillIn(chunk, ifOffset);
		
		Type[] entryStack = stack;
		stack = thenStack;
		
		return entryStack;
	}


	/**
	 * Complete the conditional and patch up any jump instructions.
	 * @param chunk CodeChunk this conditional lives in
	 * @param elseStack Current stack, which is the stack at the end of the else
	 * @param stackNumber Current number of valid elements in elseStack
	 * @return The conditional this conditional was nested in, if any.
	 */
	Conditional end(CodeChunk chunk, Type[] elseStack, int stackNumber) {

		if (thenGotoOffset == 0) {
			// no else condition
			fillIn(chunk, ifOffset);
		} else {
			fillIn(chunk, thenGotoOffset);
		}
		
		if (SanityManager.DEBUG)
		{
			if (stackNumber != stack.length)
				SanityManager.THROWASSERT("ByteCode Conditional then/else stack depths differ then:"
						+ stack.length + " else: " + stackNumber);
			
			for (int i = 0; i < stackNumber; i++)
			{
				if (!stack[i].vmName().equals(elseStack[i].vmName()))
					SanityManager.THROWASSERT("ByteCode Conditional then/else stack mismatch: then: "
							+ stack[i].vmName() + 
							" else: " + elseStack[i].vmName());
			}
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

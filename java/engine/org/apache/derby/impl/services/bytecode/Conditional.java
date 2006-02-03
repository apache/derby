/*

   Derby - Class org.apache.derby.impl.services.bytecode.Conditional

   Copyright 2000, 2006 The Apache Software Foundation or its licensors, as applicable.

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
	A Conditional represents an if/then/else block.
	When this is created the code  will already have
	the conditional check code. The code is optimized for branch
	offsets that fit in 2 bytes, though will handle 4 byte offsets.
<code>
     if condition
	 then code
	 else code
</code>
     what actually gets built is
<code>
     if !condition branch to eb:
	  then code
	  goto end:  // skip else
	 eb:
	  else code
	 end:
</code>

    If no else condition was provided then the code is:
    
<code>
     if !condition branch to end:
	  then code
	 end:
</code>

Note all branches here are using relative offsets, not absolute program counters.

If the then code leads to the conditional branch offset being too big (>32k)
because the then code is larger than 32767 bytes then this is built:
<code>
     // when else code is present
     if condition branch to tb: (relative offset +8)
     goto_w eb: // indirect for else block (5 bytes)
     tb:
	    then code (> 32767 bytes)
	    goto end:
	 eb:
	  else code
	 end:
</code>

<code>
     // when only then code is present
     if condition branch to tb: (relative offset +8)
     goto_w end: // indirect for else block (5 bytes)
     tb:
	    then code (> 32767 bytes)
	 end:
</code>

If there is an else branch and only it is larger than 32767 bytes then
the code is:

<code>
     if !condition branch to eb: (offset increased by two over previous value)
	  then code
	  goto_w end:  // skip else
	 eb:
	  else code (> 32767 bytes)
	 end:
</code>

This has one special case where the size of conditional branch to eb:
now must change from a 16bit value to a 32 bit value. The generated code
for this is the same as when both the then code and the else code require
32bit offsets for the branches. This code is:

<code>
     if condition branch to tb: (relative offset +8)
     goto_w eb: // indirect for else block (5 bytes)
     tb:
	    then code (> 32767 bytes)
	    goto_w end:
	 eb:
	  else code (> 32767 bytes)
	 end:
</code>

In theory, at the moment this should not happen as this would mean a total
code size that exceeds the limit on the code size for a method (64k). This
code handles this case as it does occur if the limit for a branch is lowered
for testing purposes, to ensure the complete set of branch re-write code works.
This lowering of the limit can be done by changing the constant BRANCH16LIMIT.
  
*/
class Conditional {
	
	/**
	 * Limit of a 16 bit branch.
	 * <P>
	 * If broad testing of the switch from 16bit to 32bit
	 * offsets is required then this constant can be reduced
	 * to a lower value, say 50 and run complete tests. This
	 * will cover all the combinations. This works because the
	 * GOTO_W instruction works with any offset value.
	 */
	private static final int BRANCH16LIMIT = 32767;


	private final Conditional parent;
	/**
	 * pc of the 'if' opcode.
	 */
	private final int   if_pc;
	
	private Type[]	stack;
	
	/**
	 * pc of the GOTO added at the end of the then block
	 * to transfer control to the end of this conditional.
	 * That is at the end of the else block.
	 */
	private int thenGoto_pc;

	/**
	 * Start a conditional block.
	 * @param parent Current conditional block, null if no nesting is going on.
	 * @param chunk CodeChunk this conditional lives in
	 * @param ifOpcode Opcode for the if check.
	 * @param entryStack Type stack on entering the conditional then block.
	 */
	Conditional(Conditional parent, CodeChunk chunk, short ifOpcode, Type[] entryStack) {
		this.parent = parent;
		if_pc = chunk.getPC();
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
	Type[] startElse(BCMethod mb, CodeChunk chunk, Type[] thenStack) {
		
		// reserve space for the goto end we will be adding
		chunk.addInstrU2(VMOpcode.GOTO, 0);

		// fill in the branch opcode to branch to
		// the code after the goto, which is the current pc.
		fillIn(mb, chunk, if_pc, chunk.getPC());
		
		// Cannot use the pc before adding the GOTO above
		// as the fillIn may insert bytes that move the GOTO,
		// thus calculate at the end, and subtract the number of
		// instructions in a goto to get its pc.
		thenGoto_pc = chunk.getPC() - 3;
		
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
	Conditional end(BCMethod mb, CodeChunk chunk, Type[] elseStack, int stackNumber) {
		int branch_pc;
		if (thenGoto_pc == 0) {
			// no else condition, make the conditional branch to the end
			branch_pc = if_pc;
		} else {
			// otherwise make the goto branch to the end
			branch_pc = thenGoto_pc;
		}
		
		fillIn(mb, chunk, branch_pc, chunk.getPC());
		
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

	/**
	 * Fill in the offsets for a conditional or goto instruction that
	 * were dummied up as zero during code generation. Handles modifying
	 * branch logic when the offset for the branch is greater than can
	 * fit in 16 bits. In this case a GOTO_W with a 32 bit offset will
	 * be used, see details within the method for how this is acheived
	 * in all situations. This method might insert instructions in the
	 * already generated byte code, thus increasing the program counter.
	 * 
	 * @param mb Method this conditional is for
	 * @param chunk Our code chunk
	 * @param branch_pc pc of the branch or goto opcode in the code stream
	 * @param target_pc pc where we want to jump to.
	 */
	private void fillIn(BCMethod mb, CodeChunk chunk,
			int branch_pc, int target_pc) {

		int offset = target_pc - branch_pc;

		// Following code assumes that this class only
		// generates forward jumps. Jump of zero is
		// wrong as well, would be infinite loop or stack problems.
		if (SanityManager.DEBUG)
		{
			if (offset <= 0)
				SanityManager.THROWASSERT("Conditional branch zero or negative " + offset);
		}

		// Original opcode written.
		short branchOpcode = chunk.getOpcode(branch_pc);
		
		// Handle 16bit offsets, two byte.
		if (offset <= BRANCH16LIMIT)
		{
			// Code was already setup for two byte offsets,
			// branch or goto instruction was written with
			// offset zero, ready to be overwritten by this code.
			CodeChunk mod = chunk.insertCodeSpace(branch_pc, 0);
			mod.addInstrU2(branchOpcode, offset);
			return;
		}
		
		if (branchOpcode == VMOpcode.GOTO)
		{
			// Ensure the pc we are jumping to (the current pc)
			// is within bounds of a valid method *after*
			// we have added the extra bytes.
			if ((target_pc + 2) >= VMOpcode.MAX_CODE_LENGTH)
			{
				mb.cb.addLimitExceeded(mb, "goto_target",
						VMOpcode.MAX_CODE_LENGTH, target_pc + 2);
				
				// even if we fail continue to generate the correct code
				// so that the assumptions in the patch up code are not broken.
			}
			
		
			// Change the GOTO to a GOTO_W, which means
			// inserting 2 bytes into the stream.
			CodeChunk mod = chunk.insertCodeSpace(branch_pc, 2);
			
			// Offset we are jumping to is now two bytes futher away
			offset += 2;
			
			// replace the original GOTO with a GOTO_W
			mod.addInstrU4(VMOpcode.GOTO_W, offset);
			
			// Now need to patch up the original conditional
			// as the else code it was branching to is now
			// another two bytes away.
			// There are three cases, given the original branch_offset:
			//
			// 1) branch_offset 16bit, branch_offset+2 16 bit
			// 2) branch_offset 16bit, branch_offset+2 32 bit
			// 3) branch_offset 32bit, branch_offset+2 32 bit
			//
			int startElse_pc = mod.getPC();
			
			int branchOffset = startElse_pc - if_pc;
							
			if (branchOffset <= BRANCH16LIMIT + 2)
			{					
				// case 1) branch_offset 16bit, branch_offset+2 16 bit
                // case 2) branch_offset 16bit, branch_offset+2 32 bit
				//
				// Branch to the else code is on the original conditional
				
				// both handled by the standard fillIn method.
				fillIn(mb, chunk, if_pc, mod.getPC());
				return;

			}

			// branch to the else code was changed from the conditional
			// to a GOTO_W as the branch was out of the range of the
			// conditional.
			
			// Overwrite the offset of the existing GOTO_W, the instruction
			// after the conditional instruction, which is three bytes long
			mod = chunk.insertCodeSpace(if_pc + 3, 0);
			
			// Above branchOffset was calculated from the conditional
			// but we need to branch from the GOTO_W that was inserted
			// which is three bytes after the conditional.
			branchOffset -= 3;
			
			mod.addInstrU4(VMOpcode.GOTO_W, branchOffset);
			return;

		}
		else
		{
			// Ensure the pc we are jumping to (the current pc)
			// is within bounds of a valid method *after*
			// we have added the extra bytes.
			if ((target_pc + 5) >= VMOpcode.MAX_CODE_LENGTH)
			{
			    mb.cb.addLimitExceeded(mb,
			    		"branch_target", VMOpcode.MAX_CODE_LENGTH, target_pc + 5);
				// even if we fail continue to generate the correct code
				// so that the assumptions in the patch up code are not broken.
			}

			// Conditional branch
			// branch on the conditional, need to add
			// indirection. Basically changing
			// (actual conditional might be different)
			// Note branch inverting.
			//
			// IFNONNULL branch offset (to else code)
			//   <then code>
			// GOTO end:
			//   <else code>
			// end:
			// to
			//
			// IFNULL branch +8  (to then code, 3 bytes in stream)
			// GOTO_W offset* (to else code, 5 new bytes in stream)
			//    <then code>
			// GOTO end:
			//    <else code>
					
			// Invert branch.
			switch (branchOpcode)
			{
			case VMOpcode.IFNONNULL:
				branchOpcode = VMOpcode.IFNULL;
				break;
			case VMOpcode.IFEQ:
				branchOpcode = VMOpcode.IFNE;
				break;
			default:
				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT("Conditional does not handle opcode " + branchOpcode);
				
			}
												
			// Thus we need to insert 5 bytes
			//
			CodeChunk mod = chunk.insertCodeSpace(branch_pc, 5);
			
			// mod is positioned at the current branch.
			mod.addInstrU2(branchOpcode, 8);
											
			// Indirect goto for the conditional else block or end.
			// Offset was from the comparision instruction to the
			// start of the real code. Now the branch location
			// is an additional two bytes away, because this
			// GOTO_W instruction occupies 5 bytes, and the original
			// branch 3.
			offset += 2;
			
			mod.addInstrU4(VMOpcode.GOTO_W, offset);
	
			return;	
		}
	}


}

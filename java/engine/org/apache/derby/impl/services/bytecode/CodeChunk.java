/*

   Derby - Class org.apache.derby.impl.services.bytecode.CodeChunk

   Copyright 1998, 2006 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.classfile.ClassFormatOutput;
import org.apache.derby.iapi.services.classfile.ClassHolder;
import org.apache.derby.iapi.services.classfile.ClassMember;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.io.ArrayOutputStream;

import java.io.IOException;
import java.util.Arrays;

/**
 * This class represents a chunk of code in a CodeAttribute.
 * Typically, a CodeAttribute represents the code in a method.
 * If there is a try/catch block, each catch block will get its
 * own code chunk.  This allows the catch blocks to all be put at
 * the end of the generated code for a method, which eliminates
 * the need to generate a jump around each catch block, which
 * would be a forward reference.
 */
final class CodeChunk {
	
	/**
	 * Starting point of the byte code stream in the underlying stream/array.
	 */
	private static final int CODE_OFFSET = 8;
		
	// The use of ILOAD for the non-integer types is correct.
	// We have to assume that the appropriate checks/conversions
	// are defined on math operation results to ensure that
	// the type is preserved when/as needed.
	static final short[] LOAD_VARIABLE = {
		VMOpcode.ILOAD,	/* vm_byte */
		VMOpcode.ILOAD,	/* vm_short */
		VMOpcode.ILOAD,	/* vm_int */
		VMOpcode.LLOAD,	/* vm_long */
		VMOpcode.FLOAD,	/* vm_float */
		VMOpcode.DLOAD,	/* vm_double */
		VMOpcode.ILOAD,	/* vm_char */
		VMOpcode.ALOAD	/* vm_reference */
	};

	static final short[] LOAD_VARIABLE_FAST = {
		VMOpcode.ILOAD_0,	/* vm_byte */
		VMOpcode.ILOAD_0,	/* vm_short */
		VMOpcode.ILOAD_0,	/* vm_int */
		VMOpcode.LLOAD_0,	/* vm_long */
		VMOpcode.FLOAD_0,	/* vm_float */
		VMOpcode.DLOAD_0,	/* vm_double */
		VMOpcode.ILOAD_0,	/* vm_char */
		VMOpcode.ALOAD_0	/* vm_reference */
	};

	// The ISTOREs for non-int types are how things work.
	// It assumes that the appropriate casts are done
	// on operations on non-ints to ensure that the values
	// remain in the valid ranges.
	static final short[] STORE_VARIABLE = {
		VMOpcode.ISTORE,	/* vm_byte */
		VMOpcode.ISTORE,	/* vm_short */
		VMOpcode.ISTORE,	/* vm_int */
		VMOpcode.LSTORE,	/* vm_long */
		VMOpcode.FSTORE,	/* vm_float */
		VMOpcode.DSTORE,	/* vm_double */
		VMOpcode.ISTORE,	/* vm_char */
		VMOpcode.ASTORE	/* vm_reference */
	};

	static final short[] STORE_VARIABLE_FAST = {
		VMOpcode.ISTORE_0,	/* vm_byte */
		VMOpcode.ISTORE_0,	/* vm_short */
		VMOpcode.ISTORE_0,	/* vm_int */
		VMOpcode.LSTORE_0,	/* vm_long */
		VMOpcode.FSTORE_0,	/* vm_float */
		VMOpcode.DSTORE_0,	/* vm_double */
		VMOpcode.ISTORE_0,	/* vm_char */
		VMOpcode.ASTORE_0	/* vm_reference */
	};

	static final short ARRAY_ACCESS[] = {
		VMOpcode.BALOAD,	/* vm_byte */
		VMOpcode.SALOAD,	/* vm_short */
		VMOpcode.IALOAD,	/* vm_int */
		VMOpcode.LALOAD,	/* vm_long */
		VMOpcode.FALOAD,	/* vm_float */
		VMOpcode.DALOAD,	/* vm_double */
		VMOpcode.CALOAD,	/* vm_char */
		VMOpcode.AALOAD	/* vm_reference */
	};
	static final short ARRAY_STORE[] = {
		VMOpcode.BASTORE,	/* vm_byte */
		VMOpcode.SASTORE,	/* vm_short */
		VMOpcode.IASTORE,	/* vm_int */
		VMOpcode.LASTORE,	/* vm_long */
		VMOpcode.FASTORE,	/* vm_float */
		VMOpcode.DASTORE,	/* vm_double */
		VMOpcode.CASTORE,	/* vm_char */
		VMOpcode.AASTORE	/* vm_reference */
	};	
	static final short[] RETURN_OPCODE = {
		VMOpcode.IRETURN,  /* 0 = byte      */
		VMOpcode.IRETURN,  /* 1 = short     */
		VMOpcode.IRETURN,  /* 2 = int       */
		VMOpcode.LRETURN,  /* 3 = long      */
		VMOpcode.FRETURN,  /* 4 = float     */
		VMOpcode.DRETURN,  /* 5 = double    */
		VMOpcode.IRETURN,  /* 6 = char      */
		VMOpcode.ARETURN   /* 7 = reference */
		};

	// the first dimension is the current vmTypeId
	// the second dimension is the target vmTypeId
	//
	// the cells of the entry at [current,target] are:
	// 0: operation
	// 1: result type of operation
	// if entry[1] = target, we are done. otherwise,
	// you have to continue with entry[1] as the new current
	// after generating the opcode listed (don't generate if it is NOP).
	// if entry[0] = BAD, we can
	
	static final short CAST_CONVERSION_INFO[][][] = {
		/* current = vm_byte */
		{
		/* target = vm_byte      */ { VMOpcode.NOP, BCExpr.vm_byte },
		/* target = vm_short     */ { VMOpcode.NOP, BCExpr.vm_short },
		/* target = vm_int       */ { VMOpcode.NOP, BCExpr.vm_int },
		/* target = vm_long      */ { VMOpcode.NOP, BCExpr.vm_int },
		/* target = vm_float     */ { VMOpcode.NOP, BCExpr.vm_int },
		/* target = vm_double    */ { VMOpcode.NOP, BCExpr.vm_int },
		/* target = vm_char      */ { VMOpcode.NOP, BCExpr.vm_char }, 
		/* target = vm_reference */ { VMOpcode.BAD, BCExpr.vm_reference }
		},
		/* current = vm_short */
		{
		/* target = vm_byte      */ { VMOpcode.NOP, BCExpr.vm_byte },
		/* target = vm_short     */ { VMOpcode.NOP, BCExpr.vm_short },
		/* target = vm_int       */ { VMOpcode.NOP, BCExpr.vm_int },
		/* target = vm_long      */ { VMOpcode.NOP, BCExpr.vm_int },
		/* target = vm_float     */ { VMOpcode.NOP, BCExpr.vm_int },
		/* target = vm_double    */ { VMOpcode.NOP, BCExpr.vm_int },
		/* target = vm_char      */ { VMOpcode.NOP, BCExpr.vm_char }, 
		/* target = vm_reference */ { VMOpcode.BAD, BCExpr.vm_reference }
		},
		/* current = vm_int */
		{
		/* target = vm_byte      */ { VMOpcode.I2B, BCExpr.vm_byte },
		/* target = vm_short     */ { VMOpcode.I2S, BCExpr.vm_short },
		/* target = vm_int       */ { VMOpcode.NOP, BCExpr.vm_int },
		/* target = vm_long      */ { VMOpcode.I2L, BCExpr.vm_long },
		/* target = vm_float     */ { VMOpcode.I2F, BCExpr.vm_float },
		/* target = vm_double    */ { VMOpcode.I2D, BCExpr.vm_double },
		/* target = vm_char      */ { VMOpcode.I2B, BCExpr.vm_char }, 
		/* target = vm_reference */ { VMOpcode.BAD, BCExpr.vm_reference }
		},
		/* current = vm_long */
		{
		/* target = vm_byte      */ { VMOpcode.L2I, BCExpr.vm_int },
		/* target = vm_short     */ { VMOpcode.L2I, BCExpr.vm_int },
		/* target = vm_int       */ { VMOpcode.L2I, BCExpr.vm_int },
		/* target = vm_long      */ { VMOpcode.NOP, BCExpr.vm_long },
		/* target = vm_float     */ { VMOpcode.L2F, BCExpr.vm_float },
		/* target = vm_double    */ { VMOpcode.L2D, BCExpr.vm_double },
		/* target = vm_char      */ { VMOpcode.L2I, BCExpr.vm_int }, 
		/* target = vm_reference */ { VMOpcode.BAD, BCExpr.vm_reference }
		},
		/* current = vm_float */
		{
		/* target = vm_byte      */ { VMOpcode.F2I, BCExpr.vm_int },
		/* target = vm_short     */ { VMOpcode.F2I, BCExpr.vm_int },
		/* target = vm_int       */ { VMOpcode.F2I, BCExpr.vm_int },
		/* target = vm_long      */ { VMOpcode.F2L, BCExpr.vm_long },
		/* target = vm_float     */ { VMOpcode.NOP, BCExpr.vm_float },
		/* target = vm_double    */ { VMOpcode.F2D, BCExpr.vm_double },
		/* target = vm_char      */ { VMOpcode.F2I, BCExpr.vm_int }, 
		/* target = vm_reference */ { VMOpcode.BAD, BCExpr.vm_reference }
		},
		/* current = vm_double */
		{
		/* target = vm_byte      */ { VMOpcode.D2I, BCExpr.vm_int },
		/* target = vm_short     */ { VMOpcode.D2I, BCExpr.vm_int },
		/* target = vm_int       */ { VMOpcode.D2I, BCExpr.vm_int },
		/* target = vm_long      */ { VMOpcode.D2L, BCExpr.vm_long },
		/* target = vm_float     */ { VMOpcode.D2F, BCExpr.vm_float },
		/* target = vm_double    */ { VMOpcode.NOP, BCExpr.vm_double },
		/* target = vm_char      */ { VMOpcode.D2I, BCExpr.vm_int }, 
		/* target = vm_reference */ { VMOpcode.BAD, BCExpr.vm_reference }
		},
		/* current = vm_char */
		{
		/* target = vm_byte      */ { VMOpcode.NOP, BCExpr.vm_byte },
		/* target = vm_short     */ { VMOpcode.NOP, BCExpr.vm_short },
		/* target = vm_int       */ { VMOpcode.NOP, BCExpr.vm_int },
		/* target = vm_long      */ { VMOpcode.NOP, BCExpr.vm_int },
		/* target = vm_float     */ { VMOpcode.NOP, BCExpr.vm_int },
		/* target = vm_double    */ { VMOpcode.NOP, BCExpr.vm_int },
		/* target = vm_char      */ { VMOpcode.NOP, BCExpr.vm_char },
		/* target = vm_reference */ { VMOpcode.BAD, BCExpr.vm_reference }
		},
		/* current = vm_reference */
		{
		/* target = vm_byte      */ { VMOpcode.BAD, BCExpr.vm_byte },
		/* target = vm_short     */ { VMOpcode.BAD, BCExpr.vm_short },
		/* target = vm_int       */ { VMOpcode.BAD, BCExpr.vm_int },
		/* target = vm_long      */ { VMOpcode.BAD, BCExpr.vm_long },
		/* target = vm_float     */ { VMOpcode.BAD, BCExpr.vm_float },
		/* target = vm_double    */ { VMOpcode.BAD, BCExpr.vm_double },
		/* target = vm_char      */ { VMOpcode.BAD, BCExpr.vm_char },
		/* target = vm_reference */ { VMOpcode.NOP, BCExpr.vm_reference }
		}
	};
	/**
	 * Add an instruction that has no opcodes.
	 * All instructions are 1 byte large.
	 */
	void addInstr(short opcode) {
		try {
		cout.putU1(opcode);
		} catch (IOException ioe) {
		}
	}

	void addInstrU2(short opcode, int operand) {
		try {
		cout.putU1(opcode);
		cout.putU2(operand);
		} catch (IOException ioe) {
		}
	}
	void addInstrU4(short opcode, int operand) {
		try {
		cout.putU1(opcode);
		cout.putU4(operand);
		} catch (IOException ioe) {
		}
	}
	void addInstrU1(short opcode, int operand) {
		try {
		cout.putU1(opcode);
		cout.putU1(operand);
		} catch (IOException ioe) {
		}
	}

	/**
	 * This takes an instruction that has a narrow
	 * and a wide form for CPE access, and
	 * generates accordingly the right one.
	 * We assume the narrow instruction is what
	 * we were given, and that the wide form is
	 * the next possible instruction.
	 */
	void addInstrCPE(short opcode, int cpeNum) {
		try {
		// REMIND: used 256 and 1 as magic numbers...
		if (cpeNum < 256) {
			cout.putU1(opcode);
			cout.putU1(cpeNum);
		}
		else {
			cout.putU1((short) (opcode+1));
			cout.putU2(cpeNum);
		}
		} catch (IOException ioe) {
		}
	}

	/**
	 * This takes an instruction that can be wrapped in
	 * a wide for large variable #s and does so.
	 * REVISIT: could hide this in addInstrU2?
	 */
	void addInstrWide(short opcode, int varNum) {
		try {
		// REMIND: used 256 as magic number...
		if (varNum < 256) {
			cout.putU1(opcode);
			cout.putU1(varNum);
		}
		else {
			cout.putU1(VMOpcode.WIDE);
			cout.putU1(opcode);
			cout.putU2(varNum);
		}
		} catch (IOException ioe) {
		}
	}

	/**
	 * For adding an instruction with 3 operands, a U2 and two U1's.
	 * So far, this is used by VMOpcode.INVOKEINTERFACE.
	 */
	void addInstrU2U1U1(short opcode, int operand1, short operand2,
		short operand3) {
		try {
		cout.putU1(opcode);
		cout.putU2(operand1);
		cout.putU1(operand2);
		cout.putU1(operand3);
		} catch (IOException ioe) {
		}
	}

	/** Get the current program counter */
	public int getPC() {
		return cout.size() + pcDelta;
	}
	
	/**
	 * The delta between cout.size() and the pc.
	 * For an initial code chunk this is -8 (CODE_OFFSET)
	 * since 8 bytes are written.
	 * For a nested CodeChunk return by insertCodeSpace the delta
	 * corresponds to the original starting pc.
	 * @see insetCodeSpace
	 */
	private final int pcDelta;

	CodeChunk() {
        cout = new ClassFormatOutput();
		try {
			cout.putU2(0); // max_stack, placeholder for now
			cout.putU2(0); // max_locals, placeholder for now
			cout.putU4(0); // code_length, placeholder 4 now
		} catch (IOException ioe) {
		}
		pcDelta = - CodeChunk.CODE_OFFSET;
	}
	
	/**
	 * Return a CodeChunk that has limited visibility into
	 * this CodeChunk. Used when a caller needs to insert instructions
	 * into an existing stream.
	 * @param pc
	 * @param byteCount
	 * @throws IOException 
	 */
	private CodeChunk(CodeChunk main, int pc, int byteCount)
	{
		ArrayOutputStream aos =
			new ArrayOutputStream(main.cout.getData());
		
		try {
			aos.setPosition(CODE_OFFSET + pc);
			aos.setLimit(byteCount);
		} catch (IOException e) {
		}
		
		cout = new ClassFormatOutput(aos);
		pcDelta = pc;
	}

	private final ClassFormatOutput cout;

	/**
	 * now that we have codeBytes, fix the lengths fields in it
	 * to reflect what was stored.
	 * Limits checked here are from these sections of the JVM spec.
	 * <UL>
	 * <LI> 4.7.3 The Code Attribute
	 * <LI> 4.10 Limitations of the Java Virtual Machine 
	 * </UL>
	 */
	private void fixLengths(BCMethod mb, int maxStack, int maxLocals, int codeLength) {

		byte[] codeBytes = cout.getData();

		// max_stack is in bytes 0-1
		if (mb != null && maxStack > 65535)
			mb.cb.addLimitExceeded(mb, "max_stack", 65535, maxStack);
			
		codeBytes[0] = (byte)(maxStack >> 8 );
		codeBytes[1] = (byte)(maxStack );

		// max_locals is in bytes 2-3
		if (mb != null && maxLocals > 65535)
			mb.cb.addLimitExceeded(mb, "max_locals", 65535, maxLocals);
		codeBytes[2] = (byte)(maxLocals >> 8 );
		codeBytes[3] = (byte)(maxLocals );

		// code_length is in bytes 4-7
		if (mb != null && codeLength > VMOpcode.MAX_CODE_LENGTH)
			mb.cb.addLimitExceeded(mb, "code_length",
					VMOpcode.MAX_CODE_LENGTH, codeLength);
		codeBytes[4] = (byte)(codeLength >> 24 );
		codeBytes[5] = (byte)(codeLength >> 16 );
		codeBytes[6] = (byte)(codeLength >> 8 );
		codeBytes[7] = (byte)(codeLength );
	}

	/**
	 * wrap up the entry and stuff it in the class,
	 * now that it holds all of the instructions and
	 * the exception table.
	 */
	void complete(BCMethod mb, ClassHolder ch,
			ClassMember method, int maxStack, int maxLocals) {

		int codeLength = getPC();

		ClassFormatOutput out = cout;

		try {

			out.putU2(0); // exception_table_length

			if (SanityManager.DEBUG) {
			  if (SanityManager.DEBUG_ON("ClassLineNumbers")) {
				// Add a single attribute - LineNumberTable
				// This add fake line numbers that are the pc offset in the method.
				out.putU2(1); // attributes_count

				int cpiUTF = ch.addUtf8("LineNumberTable");

				out.putU2(cpiUTF);
				out.putU4((codeLength * 4) + 2);
				out.putU2(codeLength);
				for (int i = 0; i < codeLength; i++) {
					out.putU2(i);
					out.putU2(i);
				}
			  } else {
				  out.putU2(0); // attributes_count
			  }

			} else {
				out.putU2(0); // attributes_count
				// attributes is empty, a 0-element array.
			}
		} catch (IOException ioe) {
		}

		fixLengths(mb, maxStack, maxLocals, codeLength);
		method.addAttribute("Code", out);
	}
	
	/**
	 * Return the opcode at the given pc.
	 */
	short getOpcode(int pc)
	{
		return (short) (cout.getData()[CODE_OFFSET + pc] & 0xff);
	}
	
	/**
	 * Insert room for byteCount bytes after the instruction at pc
	 * and prepare to replace the instruction at pc. The instruction
	 * at pc is not modified by this call, space is allocated after it.
	 * The newly inserted space will be filled with NOP instructions.
	 * 
	 * Returns a CodeChunk positioned at pc and available to write
	 * instructions upto (byteCode + length(existing instruction at pc) bytes.
	 * 
	 * This chunk is left correctly positioned at the end of the code
	 * stream, ready to accept more code. Its pc will have increased by
	 * additionalBytes.
	 * 
	 * It is the responsibility of the caller to patch up any
	 * branches or gotos.
	 * 
	 * @param pc
	 * @param additionalBytes
	 * @return
	 */
	CodeChunk insertCodeSpace(int pc, int additionalBytes)
	{
		short existingOpcode = getOpcode(pc);
		int lengthOfExistingInstruction;
		if (existingOpcode == VMOpcode.GOTO_W)
			lengthOfExistingInstruction = 5;
		else
			lengthOfExistingInstruction = 3;
		
		
		if (additionalBytes > 0)
		{
			// Size of the current code after this pc.
			int sizeToMove = (getPC() - pc) - lengthOfExistingInstruction;

			// Increase the code by the number of bytes to be
			// inserted. These NOPs will be overwritten by the
			// moved code by the System.arraycopy below.
			// It's assumed that the number of inserted bytes
			// is small, one or two instructions worth, so it
			// won't be a performance issue.
			for (int i = 0; i < additionalBytes; i++)
				addInstr(VMOpcode.NOP);
		
			// Must get codeBytes here as the array might have re-sized.
			byte[] codeBytes = cout.getData();
			
			int byteOffset = CODE_OFFSET + pc + lengthOfExistingInstruction;
					
			
			// Shift the existing code stream down
			// pc + 3
			// Pc + 3 + 5
			System.arraycopy(
					codeBytes, byteOffset,
					codeBytes, byteOffset + additionalBytes,
					sizeToMove);
			
			// Place NOPs in the space just freed by the move.
			// This is not required, it ias assumed the caller
			// will overwrite all the bytes they requested, but
			// to be safe fill in with NOPs rather than leaving code
			// that could break the verifier.
			Arrays.fill(codeBytes, byteOffset, byteOffset + additionalBytes,
					(byte) VMOpcode.NOP);
		}
		
		// The caller must overwrite the original instruction
		// at pc, thus increase the range of the limit stream
		// created to include those bytes.
		additionalBytes += lengthOfExistingInstruction;
		
		// Now the caller needs to fill in the instructions
		// that make up the modified byteCount bytes of bytecode stream.
		// Return a CodeChunk that can be used for this and
		// is limited to only those bytes.
		// The pc of the original code chunk is left unchanged.
		
		return new CodeChunk(this, pc, additionalBytes);
						
	}
}

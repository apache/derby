/*

   Derby - Class org.apache.derby.impl.services.bytecode.CodeChunk

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

package org.apache.derby.impl.services.bytecode;

import org.apache.derby.iapi.services.classfile.ClassFormatOutput;
import org.apache.derby.iapi.services.classfile.ClassHolder;
import org.apache.derby.iapi.services.classfile.ClassMember;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.io.IOException;

/**
 * This class represents a chunk of code in a CodeAttribute.
 * Typically, a CodeAttribute represents the code in a method.
 * If there is a try/catch block, each catch block will get its
 * own code chunk.  This allows the catch blocks to all be put at
 * the end of the generated code for a method, which eliminates
 * the need to generate a jump around each catch block, which
 * would be a forward reference.
 */
class CodeChunk {
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

	/** Add an arbitrary array of bytes */
	void addChunk(CodeChunk other) {
		
		try {
			other.getCout().writeTo(cout);
		} catch (IOException ioe) {
		}
	}

	/** Get the ClassFormatOutput */
	ClassFormatOutput getCout() {
		return cout;
	}

	/** Get the current PC */
	public int getRelativePC() {
		return cout.size() - codeOffset;
	}

	CodeChunk(boolean main) {
        cout = new ClassFormatOutput();
		if (main) {
			try {
				cout.putU2(0); // max_stack, placeholder for now
				cout.putU2(0); // max_locals, placeholder for now
				cout.putU4(0); // code_length, placeholder 4 now
			} catch (IOException ioe) {
			}
			
			codeOffset = 8; // just wrote eight bytes
		}
	}

	/* This is the PC relative to the beginning of this code chunk */
	private int codeOffset;

	private final ClassFormatOutput cout;

	/**
	 * now that we have codeBytes, fix the lengths fields in it
	 * to reflect what was stored.
	 */
	void fixLengths(int maxStack, int maxLocals, int codeLength) {

		byte[] codeBytes = cout.getData();

		// max_stack is in bytes 0-1
		codeBytes[0] = (byte)(maxStack >> 8 );
		codeBytes[1] = (byte)(maxStack );

		// max_locals is in bytes 2-3
		codeBytes[2] = (byte)(maxLocals >> 8 );
		codeBytes[3] = (byte)(maxLocals );

		// code_length is in bytes 4-7
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
	void complete(ClassHolder ch, ClassMember method, int maxStack, int maxLocals) {

		int codeLength = getRelativePC();

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

		fixLengths(maxStack, maxLocals, codeLength);
		method.addAttribute("Code", out);
	}

}

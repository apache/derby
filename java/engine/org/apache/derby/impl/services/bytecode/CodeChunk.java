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

import org.apache.derby.iapi.services.classfile.CONSTANT_Index_info;
import org.apache.derby.iapi.services.classfile.CONSTANT_Utf8_info;
import org.apache.derby.iapi.services.classfile.ClassFormatOutput;
import org.apache.derby.iapi.services.classfile.ClassHolder;
import org.apache.derby.iapi.services.classfile.ClassMember;
import org.apache.derby.iapi.services.classfile.VMDescriptor;

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
	 * Constant used by OPCODE_ACTION to represent the
	 * common action of push one word, 1 byte
	 * for the instruction.
	 */
	private static final byte[] push1_1i = {1, 1};

	/**
	 * Constant used by OPCODE_ACTION to represent the
	 * common action of push two words, 1 byte
	 * for the instruction.
	 */
	private static final byte[] push2_1i = {2, 1};	
	/**
	 * Constant used by OPCODE_ACTION to the opcode is
	 * not yet supported.
	 */
	private static final byte[] NS = {0, -1};
	
	
	/**
	 * Value for OPCODE_ACTION[opcode][0] to represent
	 * the number of words popped or pushed in variable.
	 */
	private static final byte VARIABLE_STACK = -128;
	
	/**
	 * Array that provides two pieces of information about
	 * each VM opcode. Each opcode has a two byte array.
	 * <P>
	 * The first element in the array [0] is the number of
	 * stack words (double/long count as two) pushed by the opcode.
	 * Will be negative if the opcode pops values.
	 * 
	 * <P>
	 * The second element in the array [1] is the number of bytes
	 * in the instruction stream that this opcode's instruction
	 * takes up, including the opocode.
	 */
	private static final byte[][] OPCODE_ACTION =
	{
	
    /* NOP  0 */           { 0, 1 },
    
    /* ACONST_NULL  1 */  push1_1i,
    /* ICONST_M1  2 */    push1_1i,
    /* ICONST_0  3 */     push1_1i,
    /* ICONST_1  4 */     push1_1i,
    /* ICONST_2  5 */     push1_1i,
    /* ICONST_3  6 */     push1_1i,
    /* ICONST_4  7 */     push1_1i,
    /* ICONST_5  8 */     push1_1i,
    /* LCONST_0  9 */     push2_1i,
    /* LCONST_1  10 */    push2_1i,
    /* FCONST_0  11 */    push1_1i,
    /* FCONST_1  12 */    push1_1i,
    /* FCONST_2  13 */    push1_1i,
    /* DCONST_0  14 */    push2_1i,
    /* DCONST_1  15 */    push2_1i,
    
    /* BIPUSH  16 */     {1, 2},
    /* SIPUSH  17 */     {1, 3},
    /* LDC  18 */        {1, 2},
    /* LDC_W  19 */      {1, 3},
    /* LDC2_W  20 */     {2, 3},
    
    /* ILOAD  21 */     { 1, 2 },
    /* LLOAD  22 */     { 2, 2 },
    /* FLOAD  23 */     { 1, 2 },
    /* DLOAD  24 */     { 2, 2 },
    /* ALOAD  25 */     { 1, 2 },
    /* ILOAD_0  26 */   push1_1i,
    /* ILOAD_1  27 */   push1_1i,
    /* ILOAD_2  28 */   push1_1i,
    /* ILOAD_3  29 */   push1_1i,
    /* LLOAD_0  30 */   push2_1i,
    /* LLOAD_1  31 */   push2_1i,
    /* LLOAD_2  32 */   push2_1i,
    /* LLOAD_3  33 */   push2_1i,
    /* FLOAD_0  34 */   push1_1i,
    /* FLOAD_1  35 */   push1_1i,
    /* FLOAD_2  36 */   push1_1i,
    /* FLOAD_3  37 */   push1_1i,
    /* DLOAD_0  38 */   push2_1i,
    /* DLOAD_1  39 */   push2_1i,
    /* DLOAD_2  40 */   push2_1i,
    /* DLOAD_3  41 */   push2_1i,
    /* ALOAD_0  42 */   push1_1i,
    /* ALOAD_1  43 */   push1_1i,
    /* ALOAD_2  44 */   push1_1i,
    /* ALOAD_3  45 */   push1_1i,
    /* IALOAD  46 */    { -1, 1 },
    /* LALOAD  47 */    { 0, 1 },
    /* FALOAD  48 */    { -1, 1 },
    /* DALOAD  49 */    { 0, 1 },
    /* AALOAD  50 */    { -1, 1 },
    /* BALOAD  51 */    { -1, 1 },
    /* CALOAD  52 */    { -1, 1 },
    
    /* SALOAD  53 */       { -1, 1 },
    /* ISTORE  54 */       { -1, 2 },
    /* LSTORE  55 */       { -2, 2 },
    /* FSTORE  56 */       { -1, 2 },
    /* DSTORE  57 */       { -2, 2 },
    /* ASTORE  58 */       { -1, 2 },
    /* ISTORE_0  59 */     { -1, 1 },
    /* ISTORE_1  60 */     { -1, 1 },
    /* ISTORE_2  61 */     { -1, 1 },
    /* ISTORE_3  62 */     { -1, 1 },
    /* LSTORE_0  63 */     { -2, 1 },
    /* LSTORE_1  64 */     { -2, 1 },
    /* LSTORE_2  65 */     { -2, 1 },
    /* LSTORE_3  66 */     { -2, 1 },
    /* FSTORE_0  67 */     { -1, 1 },
    /* FSTORE_1  68 */     { -1, 1 },
    /* FSTORE_2  69 */     { -1, 1 },
    /* FSTORE_3  70 */     { -1, 1 },
    /* DSTORE_0  71 */     { -2, 1 },
    /* DSTORE_1  72 */     { -2, 1 },
    /* DSTORE_2  73 */     { -2, 1 },
    /* DSTORE_3  74 */     { -2, 1 },
    /* ASTORE_0  75 */     { -1, 1 },
    /* ASTORE_1  76 */     { -1, 1 },
    /* ASTORE_2  77 */     { -1, 1 },
    /* ASTORE_3  78 */     { -1, 1 },
    /* IASTORE  79 */      { -3, 1 },
    /* LASTORE  80 */      { -4, 1 },
    /* FASTORE  81 */      { -3, 1 },
    /* DASTORE  82 */      { -4, 1 },
    /* AASTORE  83 */      { -3, 1 },
    /* BASTORE  84 */      { -3, 1 },
    /* CASTORE  85 */      { -3, 1 },
    /* SASTORE  86 */      { -3, 1 },
    
    /* POP  87 */      { -1, 1 },
    /* POP2  88 */     { -2, 1 },
    /* DUP  89 */      push1_1i,
    /* DUP_X1  90 */   push1_1i,
    /* DUP_X2  91 */   push1_1i,
    /* DUP2  92 */     push2_1i,
    /* DUP2_X1  93 */  push2_1i,
    /* DUP2_X2  94 */  push2_1i,
    /* SWAP  95 */     { 0, 1 },
    
    /* IADD  96 */     NS,
    /* LADD  97 */     NS,
    /* FADD  98 */     { -1, 1 },
    /* DADD  99 */     { -2, 1 },
    /* ISUB  100 */     NS,
    /* LSUB  101 */     NS,
    /* FSUB  102 */     { -1, 1 },
    /* DSUB  103 */     { -2, 1 },
    /* IMUL  104 */     NS,
    /* LMUL  105 */     NS,
    /* FMUL  106 */     { -1, 1 },
    /* DMUL  107 */     { -2, 1 },
    /* IDIV  108 */     NS,
    /* LDIV  109 */     NS,
    /* FDIV  110 */     { -1, 1 },
    /* DDIV  111 */     { -2, 1 },
    /* IREM  112 */     { -1, 1 },
    /* LREM  113 */     { -2, 1 },
    /* FREM  114 */     { -1, 1 },
    /* DREM  115 */     { -2, 1 },
    /* INEG  116 */     { 0, 1 },
    /* LNEG  117 */     { 0, 1 },
    /* FNEG  118 */     { 0, 1 },
    /* DNEG  119 */     { 0, 1 },
    /* ISHL  120 */     { -1, 1 },
    /* LSHL  121 */     NS,
    /* ISHR  122 */     NS,
    /* LSHR  123 */     NS,
    /* IUSHR  124 */     NS,
    /* LUSHR  125 */     NS,
    
    /* IAND  126 */     { -1, 1 },
    /* LAND  127 */     NS,
    /* IOR  128 */      { -1, 1 },
    /* LOR  129 */      NS,
    /* IXOR  130 */     NS,
    /* LXOR  131 */     NS,
    /* IINC  132 */     NS,
    
    /* I2L  133 */     push1_1i,
    /* I2F  134 */     { 0, 1 },
    /* I2D  135 */     push1_1i,
    /* L2I  136 */     { -1, 1 },
    /* L2F  137 */     { -1, 1 },
    /* L2D  138 */     { 0, 1 },
    /* F2I  139 */     { 0, 1 },
    /* F2L  140 */     push2_1i,
    /* F2D  141 */     push1_1i,
    /* D2I  142 */     { -1, 1 },
    /* D2L  143 */     { 0, 1 },
    /* D2F  144 */     { -1, 1 },
    /* I2B  145 */     { 0, 1 },
    /* I2C  146 */     { 0, 1 },
    /* I2S  147 */     { 0, 1 },
    
    /* LCMP  148 */        NS,
    /* FCMPL  149 */       { -1, 1 },
    /* FCMPG  150 */       { -1, 1 },
    /* DCMPL  151 */       { -3, 1 },
    /* DCMPG  152 */       { -3, 1 },
    /* IFEQ  153 */        { -1, VMOpcode.IF_INS_LENGTH },
    /* IFNE  154 */        { -1, VMOpcode.IF_INS_LENGTH },
    /* IFLT  155 */        { -1, VMOpcode.IF_INS_LENGTH },
    /* IFGE  156 */        { -1, VMOpcode.IF_INS_LENGTH },
    /* IFGT  157 */        { -1, VMOpcode.IF_INS_LENGTH },
    /* IFLE  158 */        { -1, VMOpcode.IF_INS_LENGTH },
    /* IF_ICMPEQ  159 */   NS,
    /* IF_ICMPNE  160 */   NS,
    /* IF_ICMPLT  161 */   NS,
    /* IF_ICMPGE  162 */   NS,
    /* IF_ICMPGT  163 */   NS,
    /* IF_ICMPLE  164 */   NS,
    /* IF_ACMPEQ  165 */   NS,
    /* IF_ACMPNE  166 */   NS,
    /* GOTO  167 */        { 0, VMOpcode.GOTO_INS_LENGTH },
    /* JSR  168 */         NS,
    /* RET  169 */         NS,
    /* TABLESWITCH  170 */ NS,
    /* LOOKUPSWITCH  171 */NS,
    
    /* IRETURN  172 */     { -1, 1 }, // strictly speaking all words on the stack are popped.
    /* LRETURN  173 */     { -2, 1 }, // strictly speaking all words on the stack are popped.
    /* FRETURN  174 */     { -1, 1 }, // strictly speaking all words on the stack are popped.
    /* DRETURN  175 */     { -2, 1 }, // strictly speaking all words on the stack are popped.
    /* ARETURN  176 */     { -1, 1 }, // strictly speaking all words on the stack are popped.
    /* RETURN  177 */      { 0, 1 }, // strictly speaking all words on the stack are popped.

    /* GETSTATIC  178 */           {VARIABLE_STACK, 3 },
    /* PUTSTATIC  179 */           {VARIABLE_STACK, 3 },
    /* GETFIELD  180 */            {VARIABLE_STACK, 3 },
    /* PUTFIELD  181 */            {VARIABLE_STACK, 3 },
    /* INVOKEVIRTUAL  182 */       {VARIABLE_STACK, 3 },
    /* INVOKESPECIAL  183 */       {VARIABLE_STACK, 3 },
    /* INVOKESTATIC  184 */        {VARIABLE_STACK, 3 },
    /* INVOKEINTERFACE  185 */     {VARIABLE_STACK, 5 },
    
    /* XXXUNUSEDXXX  186 */        NS,

    /* NEW  187 */                 { 1, 3 },
    /* NEWARRAY  188 */            { 0, 2 },
    /* ANEWARRAY  189 */           { 0, 3 },
    /* ARRAYLENGTH  190 */         { 0, 1 },
    /* ATHROW  191 */              NS,
    /* CHECKCAST  192 */           { 0, 3},
    /* INSTANCEOF  193 */          { 0, 3 },
    /* MONITORENTER  194 */        NS,
    /* MONITOREXIT  195 */         NS,
    /* WIDE  196 */                NS,
    /* MULTIANEWARRAY  197 */      NS,
    /* IFNULL  198 */              { -1, VMOpcode.IF_INS_LENGTH },
    /* IFNONNULL  199 */           { -1, VMOpcode.IF_INS_LENGTH },
    /* GOTO_W  200 */              {0, VMOpcode.GOTO_W_INS_LENGTH },
    /* JSR_W  201 */               NS,
    /* BREAKPOINT  202 */          NS,
	
	};
	
	
	
	/**
	 * Add an instruction that has no operand.
	 * All opcodes are 1 byte large.
	 */
	void addInstr(short opcode) {
		try {
		cout.putU1(opcode);
		} catch (IOException ioe) {
		}

		if (SanityManager.DEBUG) {			
			if (OPCODE_ACTION[opcode][1] != 1)
				SanityManager.THROWASSERT("Opcode " + opcode + " incorrect entry in OPCODE_ACTION -" +
						" writing 1 byte - set as " + OPCODE_ACTION[opcode][1]);		
		}
	}

	/**
	 * Add an instruction that has a 16 bit operand.
	 */
	void addInstrU2(short opcode, int operand) {
		try {
		cout.putU1(opcode);
		cout.putU2(operand);
		} catch (IOException ioe) {
		}

		if (SanityManager.DEBUG) {			
			if (OPCODE_ACTION[opcode][1] != 3)
				SanityManager.THROWASSERT("Opcode " + opcode + " incorrect entry in OPCODE_ACTION -" +
						" writing 3 bytes - set as " + OPCODE_ACTION[opcode][1]);		
		}
	}

	/**
	 * Add an instruction that has a 32 bit operand.
	 */
     void addInstrU4(short opcode, int operand) {
		try {
		cout.putU1(opcode);
		cout.putU4(operand);
		} catch (IOException ioe) {
		}
		if (SanityManager.DEBUG) {			
			if (OPCODE_ACTION[opcode][1] != 5)
				SanityManager.THROWASSERT("Opcode " + opcode + " incorrect entry in OPCODE_ACTION -" +
						" writing 5 bytes - set as " + OPCODE_ACTION[opcode][1]);		
		}
	}

     
 	/**
 	 * Add an instruction that has an 8 bit operand.
 	 */
     void addInstrU1(short opcode, int operand) {
		try {
		cout.putU1(opcode);
		cout.putU1(operand);
		} catch (IOException ioe) {
		}

		// Only debug code from here.
		if (SanityManager.DEBUG) {
			
			if (OPCODE_ACTION[opcode][1] != 2)
				SanityManager.THROWASSERT("Opcode " + opcode + " incorrect entry in OPCODE_ACTION -" +
						" writing 2 bytes - set as " + OPCODE_ACTION[opcode][1]);
		
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
		if (cpeNum < 256) {
			addInstrU1(opcode, cpeNum);
		}
		else {
			addInstrU2((short) (opcode+1), cpeNum);
		}
	}

	/**
	 * This takes an instruction that can be wrapped in
	 * a wide for large variable #s and does so.
	 */
	void addInstrWide(short opcode, int varNum) {
		if (varNum < 256) {
			addInstrU1(opcode, varNum);
		}
		else {
			addInstr(VMOpcode.WIDE);
			addInstrU2(opcode, varNum);
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
		if (SanityManager.DEBUG) {			
			if (OPCODE_ACTION[opcode][1] != 5)
				SanityManager.THROWASSERT("Opcode " + opcode + " incorrect entry in OPCODE_ACTION -" +
						" writing 5 bytes - set as " + OPCODE_ACTION[opcode][1]);		
		}
	}

	/** Get the current program counter */
	int getPC() {
		return cout.size() + pcDelta;
	}
	
	/**
	 * Return the complete instruction length for the
	 * passed in opcode. This will include the space for
	 * the opcode and its operand.
	 */
	private static int instructionLength(short opcode)
	{
		int instructionLength = OPCODE_ACTION[opcode][1];
		
		if (SanityManager.DEBUG)
		{
			if (instructionLength < 0)
				SanityManager.THROWASSERT("Opcode without instruction length " + opcode);
		}
		
		return instructionLength;
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
		
		if (SanityManager.DEBUG)
		{
			if (codeLength <= VMOpcode.MAX_CODE_LENGTH)
			{
				// Validate the alternate way to calculate the
				// max stack agrees with the dynamic as the code
				// is built way.
				int walkedMaxStack = findMaxStack(ch, 0, codeLength);
				if (walkedMaxStack != maxStack)
				{
					SanityManager.THROWASSERT("MAX STACK MISMATCH!! " +
							maxStack + " <> " + walkedMaxStack);
				}
			}
		}

	}
	/**
	 * Return the opcode at the given pc.
	 */
	short getOpcode(int pc)
	{
		return (short) (cout.getData()[CODE_OFFSET + pc] & 0xff);
	}
	
	/**
	 * Get the unsigned short value for the opcode at the program
	 * counter pc.
	 */
	private int getU2(int pc)
	{
		byte[] codeBytes = cout.getData();
		
		int u2p = CODE_OFFSET + pc + 1;
		
		return ((codeBytes[u2p] & 0xff) << 8) | (codeBytes[u2p+1] & 0xff);
	}

	/**
	 * Get the unsigned 32 bit value for the opcode at the program
	 * counter pc.
	 */
	private int getU4(int pc)
	{
		byte[] codeBytes = cout.getData();
		
		int u4p = CODE_OFFSET + pc + 1;
		
		return (((codeBytes[u4p] & 0xff) << 24) |
		        ((codeBytes[u4p+1] & 0xff) << 16) |
		        ((codeBytes[u4p+2] & 0xff) << 8) |
		        ((codeBytes[u4p+3] & 0xff)));
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

		int lengthOfExistingInstruction
		    = instructionLength(existingOpcode);
			
		
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
	
	/*
     * * Methods related to splitting the byte code chunks into sections that
     * fit in the JVM's limits for a single method.
     */

    /**
     * For a block of byte code starting at program counter pc for codeLength
     * bytes return the maximum stack value, assuming a initial stack depth of
     * zero.
     */
    private int findMaxStack(ClassHolder ch, int pc, int codeLength) {

        int endPc = pc + codeLength;
        int stack = 0;
        int maxStack = 0;

        for (; pc < endPc;) {

            short opcode = getOpcode(pc);

            int stackDelta = stackWordDelta(ch, pc, opcode);

            stack += stackDelta;
            if (stack > maxStack)
                maxStack = stack;

            int[] cond_pcs = findConditionalPCs(pc, opcode);
            if (cond_pcs != null) {
                // an else block exists.
                if (cond_pcs[3] != -1) {
                    int blockMaxStack = findMaxStack(ch, cond_pcs[1],
                            cond_pcs[2]);
                    if ((stack + blockMaxStack) > maxStack)
                        maxStack = stack + blockMaxStack;

                    pc = cond_pcs[3];
                    continue;
                }
            }

            pc += instructionLength(opcode);
        }

        return maxStack;
    }

    /**
     * Return the number of stack words pushed (positive) or popped (negative)
     * by this instruction.
     */
    private int stackWordDelta(ClassHolder ch, int pc, short opcode) {
        if (SanityManager.DEBUG) {
            // this validates the OPCODE_ACTION entry
            instructionLength(opcode);
        }

        int stackDelta = OPCODE_ACTION[opcode][0];
        if (stackDelta == VARIABLE_STACK) {
            stackDelta = getVariableStackDelta(ch, pc, opcode);
        }

        return stackDelta;
    }

    /**
     * Get the type descriptor in the virtual machine format for the type
     * defined by the constant pool index for the instruction at pc.
     */
    private String getTypeDescriptor(ClassHolder ch, int pc) {
        int cpi = getU2(pc);

        // Field reference or method reference
        CONSTANT_Index_info cii = (CONSTANT_Index_info) ch.getEntry(cpi);

        // NameAndType reference
        int nameAndType = cii.getI2();
        cii = (CONSTANT_Index_info) ch.getEntry(nameAndType);

        // UTF8 descriptor
        int descriptor = cii.getI2();
        CONSTANT_Utf8_info type = (CONSTANT_Utf8_info) ch.getEntry(descriptor);

        String vmDescriptor = type.toString();

        return vmDescriptor;
    }

    /**
     * Get the word count for a type descriptor in the format of the virual
     * machine. For a method this returns the the word count for the return
     * type.
     */
    private static int getDescriptorWordCount(String vmDescriptor) {
        // System.out.println("vmDescriptor" + vmDescriptor);

        int width;
        if (VMDescriptor.DOUBLE.equals(vmDescriptor))
            width = 2;
        else if (VMDescriptor.LONG.equals(vmDescriptor))
            width = 2;
        else if (vmDescriptor.charAt(0) == VMDescriptor.C_METHOD) {
            switch (vmDescriptor.charAt(vmDescriptor.length() - 1)) {
            case VMDescriptor.C_DOUBLE:
            case VMDescriptor.C_LONG:
                width = 2;
                break;
            case VMDescriptor.C_VOID:
                width = 0;
                break;
            default:
                width = 1;
                break;
            }
        } else
            width = 1;

        return width;
    }

    /**
     * Get the number of words pushed (positive) or popped (negative) by this
     * instruction. The instruction is a get/put field or a method call, thus
     * the size of the words is defined by the field or method being access.
     */
    private int getVariableStackDelta(ClassHolder ch, int pc, int opcode) {
        String vmDescriptor = getTypeDescriptor(ch, pc);
        int width = CodeChunk.getDescriptorWordCount(vmDescriptor);

        int stackDelta = 0;
        // Stack delta depends on context.
        switch (opcode) {
        case VMOpcode.GETSTATIC:
            stackDelta = width;
            break;

        case VMOpcode.GETFIELD:
            stackDelta = width - 1; // one for popped object ref
            break;

        case VMOpcode.PUTSTATIC:
            stackDelta = -width;
            break;

        case VMOpcode.PUTFIELD:
            stackDelta = -width - 1; // one for pop object ref
            break;

        case VMOpcode.INVOKEVIRTUAL:
        case VMOpcode.INVOKESPECIAL:
            stackDelta = -1; // for instance reference for method call.
        case VMOpcode.INVOKESTATIC:
            stackDelta += (width - CodeChunk.parameterWordCount(vmDescriptor));
            // System.out.println("invoked non-interface " + stackDelta);
            break;

        case VMOpcode.INVOKEINTERFACE:
            // third byte contains the number of arguments to be popped
            stackDelta = width - getOpcode(pc + 3);
            // System.out.println("invoked interface " + stackDelta);
            break;
        default:
            System.out.println("WHO IS THIS ");
            break;

        }

        return stackDelta;
    }

    /**
     * Calculate the number of stack words in the arguments pushed for this
     * method descriptor.
     */
    private static int parameterWordCount(String methodDescriptor) {
        int wordCount = 0;
        for (int i = 1;; i++) {
            switch (methodDescriptor.charAt(i)) {
            case VMDescriptor.C_ENDMETHOD:
                return wordCount;
            case VMDescriptor.C_DOUBLE:
            case VMDescriptor.C_LONG:
                wordCount += 2;
                break;
            case VMDescriptor.C_ARRAY:
                // skip while there are array symbols.
                do {
                    i++;
                } while (methodDescriptor.charAt(i) == VMDescriptor.C_ARRAY);
                if (methodDescriptor.charAt(i) != VMDescriptor.C_CLASS) {
                    // an array is a reference, even an array of doubles.
                    wordCount += 1;
                    break;
                }

            // fall through to skip the Lclassname; after the array.

            case VMDescriptor.C_CLASS:
                // skip until ;
                do {
                    i++;
                } while (methodDescriptor.charAt(i) != VMDescriptor.C_ENDCLASS);
                wordCount += 1;
                break;
            default:
                wordCount += 1;
                break;
            }
        }
    }

    /**
     * Find the limits of a conditional block starting at the instruction with
     * the given opcode at the program counter pc.
     * <P>
     * Returns a six element integer array of program counters and lengths.
     * <code. [0] - program counter of the IF opcode (passed in as pc) [1] -
     * program counter of the start of the then block [2] - length of the then
     * block [3] - program counter of the else block, -1 if no else block
     * exists. [4] - length of of the else block, -1 if no else block exists.
     * [5] - program counter of the common end point. </code>
     * 
     * Looks for and handles conditionals that are written by the Conditional
     * class.
     * 
     * @return Null if the opcode is not the start of a conditional otherwise
     *         the array of values.
     */
    private int[] findConditionalPCs(int pc, short opcode) {
        switch (opcode) {
        default:
            return null;
        case VMOpcode.IFNONNULL:
        case VMOpcode.IFNULL:
        case VMOpcode.IFEQ:
        case VMOpcode.IFNE:
            break;
        }

        int then_pc;
        int else_pc;
        int if_off = getU2(pc);

        if ((if_off == 8)
                && (getOpcode(pc + VMOpcode.IF_INS_LENGTH) == VMOpcode.GOTO_W)) {
            // 32 bit branch
            then_pc = pc + VMOpcode.IF_INS_LENGTH + VMOpcode.GOTO_W_INS_LENGTH;

            // Get else PC from the 32 bit offset within the GOTO_W
            // instruction remembering to add it to the pc of that
            // instruction, not the original IF.
            else_pc = pc + VMOpcode.IF_INS_LENGTH
                    + getU4(pc + VMOpcode.IF_INS_LENGTH);

        } else {
            then_pc = pc + VMOpcode.IF_INS_LENGTH;
            else_pc = pc + if_off;
        }

        // Need to look for the goto or goto_w at the
        // end of the then block. There might not be
        // one for the case when there is no else block.
        // In that case the then block will just run into
        // what we currently think the else pc.

        int end_pc = -1;
        for (int tpc = then_pc; tpc < else_pc;) {
            short opc = getOpcode(tpc);

            // need to handle conditionals
            int[] innerCond = findConditionalPCs(tpc, opc);
            if (innerCond != null) {
                // skip to the end of this conditional
                tpc = innerCond[5]; // end_pc
                continue;
            }

            if (opc == VMOpcode.GOTO) {
                // not at the end of the then block
                // so not our goto. Shouldn't see this
                // with the current code due to the
                // skipping of the conditional above.
                // But safe defensive programming.
                if (tpc != (else_pc - VMOpcode.GOTO_INS_LENGTH))
                    continue;

                end_pc = tpc + getU2(tpc);
                break;
            } else if (opc == VMOpcode.GOTO_W) {
                // not at the end of the then block
                // so not our goto. SHouldn't see this
                // with the current code due to the
                // skipping of the conditional above.
                // But safe defensive programming.
                if (tpc != (else_pc - VMOpcode.GOTO_W_INS_LENGTH))
                    continue;

                end_pc = tpc + getU4(tpc);
                break;

            }
            tpc += instructionLength(opc);
        }

        int else_len;
        int then_len;
        if (end_pc == -1) {
            // no else block;
            end_pc = else_pc;
            else_pc = -1;

            then_len = end_pc - then_pc;
            else_len = -1;
        } else {
            then_len = else_pc - then_pc;
            else_len = end_pc - else_pc;
        }

        int[] ret = new int[6];

        ret[0] = pc;
        ret[1] = then_pc;
        ret[2] = then_len;
        ret[3] = else_pc;
        ret[4] = else_len;
        ret[5] = end_pc;

        return ret;
    }
}

/*

   Derby - Class org.apache.derby.iapi.services.classfile.VMOpcode

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

package org.apache.derby.iapi.services.classfile;

/**
 * This contains all the opcodes for the JVM
 * as defined in The Java Virtual Machine Specification.
 *
 * REMIND: might want a debugging version of this,
 * that stored the stack depth and operand expectations.
 */
public interface VMOpcode {
    short BAD = -999; // used in mapping arrays to say "don't do that"
    short NOP = 0;
    short ACONST_NULL = 1;
    short ICONST_M1 = 2;
    short ICONST_0 = 3;
    short ICONST_1 = 4;
    short ICONST_2 = 5;
    short ICONST_3 = 6;
    short ICONST_4 = 7;
    short ICONST_5 = 8;
    short LCONST_0 = 9;
    short LCONST_1 = 10;
    short FCONST_0 = 11;
    short FCONST_1 = 12;
    short FCONST_2 = 13;
    short DCONST_0 = 14;
    short DCONST_1 = 15;
    short BIPUSH = 16;
    short SIPUSH = 17;
    short LDC = 18;
    short LDC_W = 19;
    short LDC2_W = 20;
    short ILOAD = 21;
    short LLOAD = 22;
    short FLOAD = 23;
    short DLOAD = 24;
    short ALOAD = 25;
    short ILOAD_0 = 26;
    short ILOAD_1 = 27;
    short ILOAD_2 = 28;
    short ILOAD_3 = 29;
    short LLOAD_0 = 30;
    short LLOAD_1 = 31;
    short LLOAD_2 = 32;
    short LLOAD_3 = 33;
    short FLOAD_0 = 34;
    short FLOAD_1 = 35;
    short FLOAD_2 = 36;
    short FLOAD_3 = 37;
    short DLOAD_0 = 38;
    short DLOAD_1 = 39;
    short DLOAD_2 = 40;
    short DLOAD_3 = 41;
    short ALOAD_0 = 42;
    short ALOAD_1 = 43;
    short ALOAD_2 = 44;
    short ALOAD_3 = 45;
    short IALOAD = 46;
    short LALOAD = 47;
    short FALOAD = 48;
    short DALOAD = 49;
    short AALOAD = 50;
    short BALOAD = 51;
    short CALOAD = 52;
    short SALOAD = 53;
    short ISTORE = 54;
    short LSTORE = 55;
    short FSTORE = 56;
    short DSTORE = 57;
    short ASTORE = 58;
    short ISTORE_0 = 59;
    short ISTORE_1 = 60;
    short ISTORE_2 = 61;
    short ISTORE_3 = 62;
    short LSTORE_0 = 63;
    short LSTORE_1 = 64;
    short LSTORE_2 = 65;
    short LSTORE_3 = 66;
    short FSTORE_0 = 67;
    short FSTORE_1 = 68;
    short FSTORE_2 = 69;
    short FSTORE_3 = 70;
    short DSTORE_0 = 71;
    short DSTORE_1 = 72;
    short DSTORE_2 = 73;
    short DSTORE_3 = 74;
    short ASTORE_0 = 75;
    short ASTORE_1 = 76;
    short ASTORE_2 = 77;
    short ASTORE_3 = 78;
    short IASTORE = 79;
    short LASTORE = 80;
    short FASTORE = 81;
    short DASTORE = 82;
    short AASTORE = 83;
    short BASTORE = 84;
    short CASTORE = 85;
    short SASTORE = 86;
    short POP = 87;
    short POP2 = 88;
    short DUP = 89;
    short DUP_X1 = 90;
    short DUP_X2 = 91;
    short DUP2 = 92;
    short DUP2_X1 = 93;
    short DUP2_X2 = 94;
    short SWAP = 95;
    short IADD = 96;
    short LADD = 97;
    short FADD = 98;
    short DADD = 99;
    short ISUB = 100;
    short LSUB = 101;
    short FSUB = 102;
    short DSUB = 103;
    short IMUL = 104;
    short LMUL = 105;
    short FMUL = 106;
    short DMUL = 107;
    short IDIV = 108;
    short LDIV = 109;
    short FDIV = 110;
    short DDIV = 111;
    short IREM = 112;
    short LREM = 113;
    short FREM = 114;
    short DREM = 115;
    short INEG = 116;
    short LNEG = 117;
    short FNEG = 118;
    short DNEG = 119;
    short ISHL = 120;
    short LSHL = 121;
    short ISHR = 122;
    short LSHR = 123;
    short IUSHR = 124;
    short LUSHR = 125;
    short IAND = 126;
    short LAND = 127;
    short IOR = 128;
    short LOR = 129;
    short IXOR = 130;
    short LXOR = 131;
    short IINC = 132;
    short I2L = 133;
    short I2F = 134;
    short I2D = 135;
    short L2I = 136;
    short L2F = 137;
    short L2D = 138;
    short F2I = 139;
    short F2L = 140;
    short F2D = 141;
    short D2I = 142;
    short D2L = 143;
    short D2F = 144;
    short I2B = 145;
    short I2C = 146;
    short I2S = 147;
    short LCMP = 148;
    short FCMPL = 149;
    short FCMPG = 150;
    short DCMPL = 151;
    short DCMPG = 152;
    short IFEQ = 153;
    short IFNE = 154;
    short IFLT = 155;
    short IFGE = 156;
    short IFGT = 157;
    short IFLE = 158;
    short IF_ICMPEQ = 159;
    short IF_ICMPNE = 160;
    short IF_ICMPLT = 161;
    short IF_ICMPGE = 162;
    short IF_ICMPGT = 163;
    short IF_ICMPLE = 164;
    short IF_ACMPEQ = 165;
    short IF_ACMPNE = 166;
    short GOTO = 167;
    short JSR = 168;
    short RET = 169;
    short TABLESWITCH = 170;
    short LOOKUPSWITCH = 171;
    short IRETURN = 172;
    short LRETURN = 173;
    short FRETURN = 174;
    short DRETURN = 175;
    short ARETURN = 176;
    short RETURN = 177;
    short GETSTATIC = 178;
    short PUTSTATIC = 179;
    short GETFIELD = 180;
    short PUTFIELD = 181;
    short INVOKEVIRTUAL = 182;
    short INVOKESPECIAL = 183;
    short INVOKESTATIC = 184;
    short INVOKEINTERFACE = 185;
    short XXXUNUSEDXXX = 186;
    short NEW = 187;
    short NEWARRAY = 188;
    short ANEWARRAY = 189;
    short ARRAYLENGTH = 190;
    short ATHROW = 191;
    short CHECKCAST = 192;
    short INSTANCEOF = 193;
    short MONITORENTER = 194;
    short MONITOREXIT = 195;
    short WIDE = 196;
    short MULTIANEWARRAY = 197;
    short IFNULL = 198;
    short IFNONNULL = 199;
    short GOTO_W = 200;
    short JSR_W = 201;
    short BREAKPOINT = 202;
}

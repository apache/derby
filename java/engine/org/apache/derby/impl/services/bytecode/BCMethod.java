/*

   Derby - Class org.apache.derby.impl.services.bytecode.BCMethod

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.services.bytecode;

import org.apache.derby.iapi.services.compiler.ClassBuilder;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.classfile.ClassFormatOutput;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.classfile.ClassHolder;
import org.apache.derby.iapi.services.classfile.ClassMember;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.services.classfile.VMDescriptor;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.lang.reflect.Modifier;
import java.util.Vector;
import java.io.IOException;

/**
 * MethodBuilder is used to piece together a method when
 * building a java class definition.
 * <p>
 * When a method is first created, it has:
 * <ul>
 * <li> a return type
 * <li> modifiers
 * <li> a name
 * <li> an empty parameter list
 * <li> an empty throws list
 * <li> an empty statement block
 * </ul>
 * <p>
 * MethodBuilder implementations are required to supply a way for
 * Statements and Expressions to give them code.  Most typically, they may have
 * a stream to which their contents writes the code that is of
 * the type to satisfy what the contents represent.
 * MethodBuilder implementations also have to have a way to supply
 * ClassBuilders with their code, that satisfies the type of class
 * builder they are implemented with.  This is implementation-dependent,
 * so ClassBuilders, MethodBuilders, Statements, and Expressions all have
 * to be of the same implementation in order to interact to generate a class.
 * <p>
 * Method Builder implementation for generating bytecode.
 *
 */
class BCMethod implements MethodBuilder {
    
    /**
     * Code length at which to split into sub-methods.
     * Normally set to the maximim code length the
     * JVM can support, but for testing the split code
     * it can be reduced so that the standard tests
     * cause some splitting. Tested with value set to 2000.
     */
    static final int CODE_SPLIT_LENGTH = VMOpcode.MAX_CODE_LENGTH;
    
	final BCClass		cb;
	protected final ClassHolder modClass; // the class it is in (modifiable fmt)
	final String myReturnType;
	
	/**
	 * The original name of the method, this
	 * represents how any user would call this method.
	 */
	private final String myName;

    /**
     * Fast access for the parametes, will be null
     * if the method has no parameters.
     */
	BCLocalField[] parameters; 
    
    /**
     * List of parameter types with java language class names.
     * Can be null or zero length for no parameters.
     */
    private final String[] parameterTypes;
    
    
	Vector<String> thrownExceptions; // expected to be names of Classes under Throwable

	CodeChunk myCode;
	protected ClassMember myEntry;

	private int currentVarNum;
	private int statementNum;
	
	/**
	 * True if we are currently switching control
	 * over to a sub method to avoid hitting the code generation
	 * limit of 65535 bytes per method.
	 */
	private boolean handlingOverflow;
	
	/**
	 * How many sub-methods we have overflowed to.
	 */
	private int subMethodCount;

	BCMethod(ClassBuilder cb,
			String returnType,
			String methodName,
			int modifiers,
			String[] parms,
			BCJava factory) {

		this.cb = (BCClass) cb;
		modClass = this.cb.modify();
		myReturnType = returnType;
		myName = methodName;

		if (SanityManager.DEBUG) {
   			this.cb.validateType(returnType);
		}

		// if the method is not static, allocate for "this".
		if ((modifiers & Modifier.STATIC) == 0 )
			currentVarNum = 1;

		String[] vmParamterTypes;

		if (parms != null && parms.length != 0) {
			int len = parms.length;
			vmParamterTypes = new String[len];
			parameters = new BCLocalField[len];
			for (int i = 0; i < len; i++) {
				Type t = factory.type(parms[i]);
				parameters[i] = new BCLocalField(t, currentVarNum);
				currentVarNum += t.width();

				// convert to vmname for the BCMethodDescriptor.get() call
				vmParamterTypes[i] = t.vmName();
			}
		}
		else
			vmParamterTypes = BCMethodDescriptor.EMPTY;

		// create a code attribute
		String sig = BCMethodDescriptor.get(vmParamterTypes, factory.type(returnType).vmName(), factory);

		// stuff the completed information into the class.
		myEntry = modClass.addMember(methodName, sig, modifiers);

		// get code chunk
		myCode = new CodeChunk(this.cb);
        
        parameterTypes = parms;
	}
	//
	// MethodBuilder interface
	//

	/**
	 * Return the logical name of the method. The current
	 * myEntry refers to the sub method we are currently
	 * overflowing to. Those sub-methods are hidden from any caller.
	 */
	public String getName() {
		return myName;
	}

	public void getParameter(int id) {

		int num = parameters[id].cpi;
		short typ = parameters[id].type.vmType();
		if (num < 4)
			myCode.addInstr((short) (CodeChunk.LOAD_VARIABLE_FAST[typ]+num));
		else
			myCode.addInstrWide(CodeChunk.LOAD_VARIABLE[typ], num);

		growStack(parameters[id].type);
	}

	/**
	 * a throwable can be added to the end of
	 * the list of thrownExceptions.
	 */
	public void addThrownException(String exceptionClass) {
		
		// cannot add exceptions after code generation has started.
		// Allowing this would cause the method overflow/split to
		// break as the top-level method would not have the exception
		// added in the sub method.
		if (SanityManager.DEBUG)
		{
			if (myCode.getPC() != 0)
				SanityManager.THROWASSERT("Adding exception after code generation " + exceptionClass
						+ " to method " + getName());
		}

		if (thrownExceptions == null)
			thrownExceptions = new Vector<String>();
		thrownExceptions.add(exceptionClass);
	}

	/**
	 * when the method has had all of its parameters
	 * and thrown exceptions defined, and its statement
 	 * block has been completed, it can be completed and
	 * its class file information generated.
	 * <p>
	 * further alterations of the method will not be
	 * reflected in the code generated for it.
	 */
	public void complete() {
        
        // myCode.getPC() gives the code length since
        // the program counter will be positioned after
        // the last instruction. Note this value can
        // be changed by the splitMethod call.
        
        if (myCode.getPC() > CODE_SPLIT_LENGTH)
            splitMethod();
                         
       // write exceptions attribute info
        writeExceptions();
        	
		// get the code attribute to put itself into the class
		// provide the final header information needed
		myCode.complete(this, modClass, myEntry, maxStack, currentVarNum);
	}
    
    /**
     * Attempt to split a large method by pushing code out to several
     * sub-methods. Performs a number of steps.
     * <OL>
     * <LI> Split at zero stack depth.
     * <LI> Split at non-zero stack depth (FUTURE)
     * </OL>
     * 
     * If the class has already exceeded some limit in building the
     * class file format structures then don't attempt to split.
     * Most likely the number of constant pool entries has been exceeded
     * and thus the built class file no longer has integrity.
     * The split code relies on being able to read the in-memory
     * version of the class file in order to determine descriptors
     * for methods and fields.
     */
    private void splitMethod() {
        
        int split_pc = 0;
        boolean splittingZeroStack = true;
        for (int codeLength = myCode.getPC();
               (cb.limitMsg == null) &&
               (codeLength > CODE_SPLIT_LENGTH);
            codeLength = myCode.getPC())
        {
            int lengthToCheck = codeLength - split_pc;

            int optimalMinLength;
            if (codeLength < CODE_SPLIT_LENGTH * 2) {
                // minimum required
                optimalMinLength = codeLength - CODE_SPLIT_LENGTH;
            } else {
                // try to split as much as possible
                // need one for the return instruction
                optimalMinLength = CODE_SPLIT_LENGTH - 1;
            }

            if (optimalMinLength > lengthToCheck)
                optimalMinLength = lengthToCheck;

            if (splittingZeroStack)
            {
                split_pc = myCode.splitZeroStack(this, modClass, split_pc,
                    optimalMinLength);
            }
            else
            {
                // Note the split expression does not re-start split
                // at point left off by the previous split expression.
                // This could be done but would require some level
                // of stack depth history to be kept across calls.
                split_pc = myCode.splitExpressionOut(this, modClass,
                        optimalMinLength, maxStack);

             }

            // Negative split point returned means that no split
            // was possible. Give up on this approach and goto
            // the next approach.
            if (split_pc < 0) {
                if (!splittingZeroStack)
                   break;
                splittingZeroStack = false;
                split_pc = 0;
            }

            // success, continue on splitting after the call to the
            // sub-method if the method still execeeds the maximum length.
        }
        
 
    }

	/*
     * class interface
     */

	/**
     * In their giveCode methods, the parts of the method body will want to get
     * to the constant pool to add their constants. We really only want them
     * treating it like a constant pool inclusion mechanism, we could write a
     * wrapper to limit it to that.
     */
	ClassHolder constantPool() {
		return modClass;
	}


    //
    // Class implementation
    //


	/**
	 * sets exceptionBytes to the attribute_info needed
	 * for a method's Exceptions attribute.
	 * The ClassUtilities take care of the header 6 bytes for us,
	 * so they are not included here.
	 * See The Java Virtual Machine Specification Section 4.7.5,
	 * Exceptions attribute.
	 */
	protected void writeExceptions() {
		if (thrownExceptions == null)
			return;

		int numExc = thrownExceptions.size();

		// don't write an Exceptions attribute if there are no exceptions.
		if (numExc != 0) {

			try{
				ClassFormatOutput eout = new ClassFormatOutput((numExc * 2) + 2);

				eout.putU2(numExc); // number_of_exceptions

				for (int i = 0; i < numExc; i++) {
					// put each exception into the constant pool
					String e = thrownExceptions.get(i).toString();
					int ei2 = modClass.addClassReference(e);

					// add constant pool index to exception attribute_info
					eout.putU2(ei2);
				}

				myEntry.addAttribute("Exceptions", eout);

			} catch (IOException ioe) {
			}			
		}
	}

	/*
	** New push compiler api.
	*/

	/**
	 * Array of the current types of the values on the stack.
	 * A type that types up two words on the stack, e.g. double
	 * will only occupy one element in this array.
	 * This array is dynamically re-sized as needed.
	 */
	private Type[]	stackTypes = new Type[8];
	
	/**
	 * Points to the next array offset in stackTypes
	 * to be used. Really it's the number of valid entries
	 * in stackTypes.
	 */
	private int     stackTypeOffset;

	/**
	 * Maximum stack depth seen in this method, measured in words.
	 * Corresponds to max_stack in the Code attribute of section 4.7.3
	 * of the vm spec.
	 */
	int maxStack;
	
	/**
	 * Current stack depth in this method, measured in words.
	 */
	private int stackDepth;

	private void growStack(int size, Type type) {
		stackDepth += size;
		if (stackDepth > maxStack)
			maxStack = stackDepth;
		
		if (stackTypeOffset >= stackTypes.length) {

			Type[] newStackTypes = new Type[stackTypes.length + 8];
			System.arraycopy(stackTypes, 0, newStackTypes, 0, stackTypes.length);
			stackTypes = newStackTypes;
		}

		stackTypes[stackTypeOffset++] = type;

		if (SanityManager.DEBUG) {

			int sum = 0;
			for (int i = 0 ; i < stackTypeOffset; i++) {
				sum += stackTypes[i].width();
			}
			if (sum != stackDepth) {
				SanityManager.THROWASSERT("invalid stack depth " + stackDepth + " calc " + sum);
			}
		}
	}

	private void growStack(Type type) {
		growStack(type.width(), type);
	}

	private Type popStack() {
		stackTypeOffset--;
		Type topType = stackTypes[stackTypeOffset];
		stackDepth -= topType.width();
		return topType;

	}
	
	private Type[] copyStack()
	{
		Type[] stack = new Type[stackTypeOffset];
		System.arraycopy(stackTypes, 0, stack, 0, stackTypeOffset);
		return stack;
	}

	public void pushThis() {
		myCode.addInstr(VMOpcode.ALOAD_0);
		growStack(1, cb.classType);
	}

	public void push(byte value) {
		push(value, Type.BYTE);
	}

	public void push(boolean value) {
		push(value ? 1 : 0, Type.BOOLEAN);
	}

	public void push(short value) {
		push(value, Type.SHORT);
	}

	public void push(int value) {
		push(value, Type.INT);
	}

	public void dup() {
		Type dup = popStack();
		myCode.addInstr(dup.width() == 2  ? VMOpcode.DUP2 : VMOpcode.DUP);
		growStack(dup);
		growStack(dup);

	}

	public void swap() {

		// have A,B
		// want B,A

		Type wB = popStack();
		Type wA = popStack();
		growStack(wB);
		growStack(wA);

		if (wB.width() == 1) {
			// top value is one word
			if (wA.width() == 1) {
				myCode.addInstr(VMOpcode.SWAP);
				return;
			} else {
				myCode.addInstr(VMOpcode.DUP_X2);
				myCode.addInstr(VMOpcode.POP);
			}
		} else {
			// top value is two words
			if (wA.width() == 1) {
				myCode.addInstr(VMOpcode.DUP2_X1);
				myCode.addInstr(VMOpcode.POP2);
			} else {
				myCode.addInstr(VMOpcode.DUP2_X2);
				myCode.addInstr(VMOpcode.POP2);
			}
		}

		// all except the simple swap push an extra
		// copy of B which needs to be popped.
		growStack(wB);
		popStack();

	}

    /**
     * Push an integer value. Uses the special integer opcodes
     * for the constants -1 to 5, BIPUSH for values that fit in
     * a byte and SIPUSH for values that fit in a short. Otherwise
     * uses LDC with a constant pool entry.
     * 
     * @param value Value to be pushed
     * @param type Final type of the value.
     */
	private void push(int value, Type type) {

		CodeChunk chunk = myCode;

		if (value >= -1 && value <= 5)
			chunk.addInstr((short)(VMOpcode.ICONST_0+value));
		else if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE)
			chunk.addInstrU1(VMOpcode.BIPUSH,value);
		else if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE)
			chunk.addInstrU2(VMOpcode.SIPUSH,value);
		else {
			int cpe = modClass.addConstant(value);
			addInstrCPE(VMOpcode.LDC, cpe);
		}
		growStack(type.width(), type);
		
	}

    /**
     * Push a long value onto the stack.
     * For the values zero and one the LCONST_0 and
     * LCONST_1 instructions are used.
     * For values betwee Short.MIN_VALUE and Short.MAX_VALUE
     * inclusive an byte/short/int value is pushed
     * using push(int, Type) followed by an I2L instruction.
     * This saves using a constant pool entry for such values.
     * All other values use a constant pool entry. For values
     * in the range of an Integer an integer constant pool
     * entry is created to allow sharing with integer constants
     * and to reduce constant pool slot entries.
     */
	public void push(long value) {
        CodeChunk chunk = myCode;

        if (value == 0L || value == 1L) {
            short opcode = value == 0L ? VMOpcode.LCONST_0 : VMOpcode.LCONST_1;
            chunk.addInstr(opcode);
        } else if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            // the push(int, Type) method grows the stack for us.
            push((int) value, Type.LONG);
            chunk.addInstr(VMOpcode.I2L);
            return;
        } else {
            int cpe = modClass.addConstant(value);
            chunk.addInstrU2(VMOpcode.LDC2_W, cpe);
        }
        growStack(2, Type.LONG);
    }
	public void push(float value) {

		CodeChunk chunk = myCode;
		
		if (value == 0.0)
		{
			chunk.addInstr(VMOpcode.FCONST_0);
		}
		else if (value == 1.0)
		{
			chunk.addInstr(VMOpcode.FCONST_1);
		}
		else if (value == 2.0)
		{
			chunk.addInstr(VMOpcode.FCONST_2);
		}
		else 
		{
			int cpe = modClass.addConstant(value);
			addInstrCPE(VMOpcode.LDC, cpe);
		}
		growStack(1, Type.FLOAT);
	}

	public void push(double value) {
		CodeChunk chunk = myCode;

		if (value == 0.0) {
				chunk.addInstr(VMOpcode.DCONST_0);
		}
		else {
			int cpe = modClass.addConstant(value);
			chunk.addInstrU2(VMOpcode.LDC2_W, cpe);
		}
		growStack(2, Type.DOUBLE);
	}
	public void push(String value) {
		int cpe = modClass.addConstant(value);
		addInstrCPE(VMOpcode.LDC, cpe);
		growStack(1, Type.STRING);
	}
 

	public void methodReturn() {

		short opcode;		
		if (stackDepth != 0) {
			Type topType = popStack();
			opcode = CodeChunk.RETURN_OPCODE[topType.vmType()];
		} else {
			opcode = VMOpcode.RETURN;
		}

		myCode.addInstr(opcode);

		if (SanityManager.DEBUG) {
			if (stackDepth != 0)
				SanityManager.THROWASSERT("items left on stack " + stackDepth);
		}
	}

	public Object describeMethod(short opcode, String declaringClass, String methodName, String returnType) {

		Type rt = cb.factory.type(returnType);

		String methodDescriptor = BCMethodDescriptor.get(BCMethodDescriptor.EMPTY, rt.vmName(), cb.factory);

		if ((declaringClass == null) && (opcode != VMOpcode.INVOKESTATIC)) {

			Type dt = stackTypes[stackTypeOffset - 1];

			if (declaringClass == null)
				declaringClass = dt.javaName();
		}
		
		int cpi = modClass.addMethodReference(declaringClass, methodName,
				methodDescriptor, opcode == VMOpcode.INVOKEINTERFACE);

		return new BCMethodCaller(opcode, rt, cpi);
	}

	public int callMethod(Object methodDescriptor) {

		// pop the reference off the stack
		popStack();

		BCMethodCaller mc = (BCMethodCaller) methodDescriptor;

		int cpi = mc.cpi;
		short opcode = mc.opcode;

		if (opcode == VMOpcode.INVOKEINTERFACE) {
			myCode.addInstrU2U1U1(opcode, cpi, (short) 1, (short) 0);
		}
		else
			myCode.addInstrU2(opcode, cpi);
		
		// this is the return type of the method
		Type rt = mc.type;
		int rw = rt.width();
		if (rw != 0)
			growStack(rw, rt);
		else
		{
            overflowMethodCheck();
		}
		return cpi;
	}

	public int callMethod(short opcode, String declaringClass, String methodName,
		String returnType, int numArgs) {

		Type rt = cb.factory.type(returnType);

		int initialStackDepth = stackDepth;

		// get the array of parameter types

		String [] debugParameterTypes = null;
		String[] vmParameterTypes;
		if (numArgs == 0) {
			vmParameterTypes = BCMethodDescriptor.EMPTY;
		} else {
			if (SanityManager.DEBUG) {
				debugParameterTypes = new String[numArgs];
			}
			vmParameterTypes = new String[numArgs];
			for (int i = numArgs - 1; i >= 0; i--) {
				Type at = popStack();

				vmParameterTypes[i] = at.vmName();
				if (SanityManager.DEBUG) {
					debugParameterTypes[i] = at.javaName();
				}
			}
		}
		
		String methodDescriptor = BCMethodDescriptor.get(vmParameterTypes, rt.vmName(), cb.factory);

		Type dt = null;
		if (opcode != VMOpcode.INVOKESTATIC) {

			dt = popStack();
		}
		Type dtu = vmNameDeclaringClass(declaringClass);
		if (dtu != null)
			dt = dtu;
		
		int cpi = modClass.addMethodReference(dt.vmNameSimple, methodName,
				methodDescriptor, opcode == VMOpcode.INVOKEINTERFACE);

		if (opcode == VMOpcode.INVOKEINTERFACE) {
			short callArgCount = (short) (initialStackDepth - stackDepth);
			myCode.addInstrU2U1U1(opcode, cpi, callArgCount, (short) 0);
		}
		else
			myCode.addInstrU2(opcode, cpi);
		
		// this is the return type of the method
		int rw = rt.width();
		if (rw != 0)
			growStack(rw, rt);
		else
		{
            overflowMethodCheck();
		}
		// Check the declared type of the method
		if (SanityManager.DEBUG) {

			d_BCValidate.checkMethod(opcode, dt, methodName, debugParameterTypes, rt);
		}

		return cpi;
	}

	private Type vmNameDeclaringClass(String declaringClass) {
		if (declaringClass == null)
			return null;
		return cb.factory.type(declaringClass);
	}

	public void callSuper() {

		pushThis();
		callMethod(VMOpcode.INVOKESPECIAL, cb.getSuperClassName(), "<init>", "void", 0);
	}

	public void pushNewStart(String className) {

		int cpi = modClass.addClassReference(className);

		// Use U2, not CPE, since only wide form exists.
		myCode.addInstrU2(VMOpcode.NEW, cpi);
		myCode.addInstr(VMOpcode.DUP);

		// Grow the stack twice as we are pushing
		// two instances of newly created reference
		Type nt = cb.factory.type(className);
		growStack(1, nt);
		growStack(1, nt);
	}

	public void pushNewComplete(int numArgs) {
		callMethod(VMOpcode.INVOKESPECIAL, (String) null, "<init>", "void", numArgs);
	}

	public void upCast(String className) {
		Type uct = cb.factory.type(className);

		stackTypes[stackTypeOffset - 1] = uct;
		//popStack();
		//growStack(1, uct);
	}

	public void cast(String className) {
		
		// Perform a simple optimization to not
		// insert a checkcast when the classname
		// of the cast exactly matches the type name
		// currently on the stack.
		// This can reduce the amount of generated code.
		// This compiler/class generator does not load
		// classes to check relationships or any other
		// information. Thus other optimizations where a cast
		// is not required are not implemented.
		Type tbc = stackTypes[stackTypeOffset - 1];
		
		short sourceType = tbc.vmType();
		
		if (sourceType == BCExpr.vm_reference)
		{
			// Simple optimize step
			if (className.equals(tbc.javaName()))
			{
				// do nothing, exact matching type
				return;
			}
		}
		
		Type ct = cb.factory.type(className);
		popStack();
		
		short targetType = ct.vmType();

		if (SanityManager.DEBUG) {

			if (!((sourceType == BCExpr.vm_reference &&
				targetType == BCExpr.vm_reference) ||
				(sourceType != BCExpr.vm_reference &&
				targetType != BCExpr.vm_reference))) {
				SanityManager.THROWASSERT("Both or neither must be object types " + ct.javaName() + " " + tbc.javaName());
			}
		}

		// if it is an object type, do a checkcast on it.
		if (sourceType == BCExpr.vm_reference) {

			int cpi = modClass.addClassReference(ct.vmNameSimple);
			myCode.addInstrU2(VMOpcode.CHECKCAST, cpi);
		}
		// otherwise, try to convert it.
		else {
			short opcode = VMOpcode.NOP;

			// we use the conversionInfo array
			// to determine how to convert; if
			// the result type of the conversion
			// is not our target type, we are not done
			// yet.  Make sure there are no
			// infinite loop possibilities in the
			// conversionInfo array!
			while (sourceType!=targetType && opcode!=VMOpcode.BAD) {
				short[] currentConversion = 
					CodeChunk.CAST_CONVERSION_INFO[sourceType][targetType];
				sourceType = currentConversion[1];
				opcode = currentConversion[0];
				if (opcode != VMOpcode.NOP) {
					myCode.addInstr(opcode);
				}
			}
			if (SanityManager.DEBUG) {
				SanityManager.ASSERT(opcode != VMOpcode.BAD,
					"BAD VMOpcode not expected in cast");
			}
		}
		growStack(ct);
	}

	public void isInstanceOf(String className) {
		int cpi = modClass.addClassReference(className);
		myCode.addInstrU2(VMOpcode.INSTANCEOF, cpi);
		popStack();
		growStack(1, Type.BOOLEAN);
	}

	public void pushNull(String type) {
		myCode.addInstr(VMOpcode.ACONST_NULL);
		growStack(1, cb.factory.type(type));
	}


	public void getField(LocalField field) {

		BCLocalField lf = (BCLocalField) field;
		Type lt = lf.type;

		pushThis();
		myCode.addInstrU2(VMOpcode.GETFIELD, lf.cpi);

		popStack();
		growStack(lt);

	}

	public void getField(String declaringClass, String fieldName, String fieldType) {
		Type dt = popStack();

		Type dtu = vmNameDeclaringClass(declaringClass);
		if (dtu != null)
			dt = dtu;

		getField(VMOpcode.GETFIELD, dt.vmNameSimple, fieldName, fieldType);
	}
	/**
		Push the contents of the described static field onto the stack.		
	*/
	public void getStaticField(String declaringClass, String fieldName, String fieldType) {
		getField(VMOpcode.GETSTATIC, declaringClass, fieldName, fieldType);
	}

	private void getField(short opcode, String declaringClass, String fieldName, String fieldType) { 

		Type ft = cb.factory.type(fieldType);
		int cpi = modClass.addFieldReference(vmNameDeclaringClass(declaringClass).vmNameSimple, fieldName, ft.vmName());
		myCode.addInstrU2(opcode, cpi);

		growStack(ft);
	}
	
	/**
	 * Set the field but don't duplicate its value so
	 * nothing is left on the stack after this call.
	 */
	public void setField(LocalField field) {
		BCLocalField lf = (BCLocalField) field;
		putField(lf.type, lf.cpi, false);
        overflowMethodCheck();
	}

	/**
		Upon entry the top word(s) on the stack is
		the value to be put into the field. Ie.
		we have
		<PRE>
		word
		</PRE>

		Before the call we need 
		<PRE>
		word
		this
		word
		</PRE>
		word2,word1 -&gt; word2, word1, word2

		So that we are left with word after the put.

	*/
	public void putField(LocalField field) {
		BCLocalField lf = (BCLocalField) field;
		putField(lf.type, lf.cpi, true);
	}

	/**
		Pop the top stack value and store it in the instance field of this class.
	*/
	public void putField(String fieldName, String fieldType) {

		Type ft = cb.factory.type(fieldType);
		int cpi = modClass.addFieldReference(cb.classType.vmNameSimple, fieldName, ft.vmName());

		putField(ft, cpi, true);
	}

	private void putField(Type fieldType, int cpi, boolean dup) {

		// now have ...,value
		if (dup)
		{
			myCode.addInstr(fieldType.width() == 2  ? VMOpcode.DUP2 : VMOpcode.DUP);
			growStack(fieldType);
		}
		// now have
		// dup true:  ...,value,value
		// dup false: ...,value,

		pushThis();
		// now have
		// dup true:  ...,value,value,this
		// dup false: ...,value,this

		swap();
		// now have
		// dup true:  ...,value,this,value
		// dup false: ...,this,value

		myCode.addInstrU2(VMOpcode.PUTFIELD, cpi);
		popStack(); // the value
		popStack(); // this

		// now have
		// dup true:  ...,value
		// dup false: ...
	}
	/**
		Pop the top stack value and store it in the field.
		This call requires the instance to be pushed by the caller.
	*/
	public void putField(String declaringClass, String fieldName, String fieldType) {
		Type vt = popStack();
		Type dt = popStack();

		if (SanityManager.DEBUG) {
			if (dt.width() != 1)
				SanityManager.THROWASSERT("reference expected for field access - is " + dt.javaName());
		}

		// have objectref,value
		// need value,objectref,value

		myCode.addInstr(vt.width() == 2  ? VMOpcode.DUP2_X1 : VMOpcode.DUP_X1);
		growStack(vt);
		growStack(dt);
		growStack(vt);

		Type dtu = vmNameDeclaringClass(declaringClass);
		if (dtu != null)
			dt = dtu;

		Type ft = cb.factory.type(fieldType);
		int cpi = modClass.addFieldReference(dt.vmNameSimple, fieldName, ft.vmName());
		myCode.addInstrU2(VMOpcode.PUTFIELD, cpi);

		popStack(); // value
		popStack(); // reference
	}

	public void conditionalIfNull() {

		conditionalIf(VMOpcode.IFNONNULL);
	}

	public void conditionalIf() {
		conditionalIf(VMOpcode.IFEQ);
	}

	private Conditional condition;

	private void conditionalIf(short opcode) {
		popStack();
		
		// Save the stack upon entry to the 'then' block of the
		// 'if' so that we can set up the 'else' block with the
		// correct stack on entry.

		condition = new Conditional(condition, myCode, opcode, copyStack());
	}

	public void startElseCode() {
		
		// start the else code
		Type[] entryStack = condition.startElse(this, myCode, copyStack());
		
		for (int i = stackDepth = 0; i  < entryStack.length; i++)
		{
			stackDepth += (stackTypes[i] = entryStack[i]).width();
		}
		this.stackTypeOffset = entryStack.length;

	}
	public void completeConditional() {
		condition = condition.end(this, myCode, stackTypes, stackTypeOffset);
	}
	
	public void pop() {
		if (SanityManager.DEBUG) {
			if (stackDepth == 0)
				SanityManager.THROWASSERT("pop when stack is empty!");
		}
		Type toPop = popStack();

		myCode.addInstr(toPop.width() == 2  ? VMOpcode.POP2 : VMOpcode.POP);
		
        overflowMethodCheck();
	}	

	public void endStatement() {
		if (stackDepth != 0) {
			pop();
		}

		//if (SanityManager.DEBUG) {
		//	if (stackDepth != 0)
		//		SanityManager.THROWASSERT("items left on stack " + stackDepth);
	//	}
	}

	/**
	*/
	public void getArrayElement(int element) {

		push(element);
		popStack(); // int just pushed will be popped by array access

		Type arrayType = popStack();

		String arrayJava = arrayType.javaName();
		String componentString = arrayJava.substring(0,arrayJava.length()-2);

		Type componentType = cb.factory.type(componentString);

		short typ = componentType.vmType();

		// boolean has a type id of integer, here it needs to be byte.
		if ((typ == BCExpr.vm_int) && (componentType.vmName().equals("Z")))
			typ = BCExpr.vm_byte;
		myCode.addInstr(CodeChunk.ARRAY_ACCESS[typ]);

		growStack(componentType);

	}
	// come in with ref, value

	public void setArrayElement(int element) {

		// ref, value

		push(element);

		// ref, value, index
		swap();
		
		Type componentType = popStack(); // value
		popStack(); // int just pushed will be popped by array access
		
		popStack(); // array ref.

		short typ = componentType.vmType();

		// boolean has a type id of integer, here it needs to be byte.
		if ((typ == BCExpr.vm_int) && (componentType.vmName().equals("Z")))
			typ = BCExpr.vm_byte;

		myCode.addInstr(CodeChunk.ARRAY_STORE[typ]);
	}
	/**
		this array maps the BCExpr vm_* constants 0..6 to
		the expected VM type constants for the newarray instruction.
		<p>
		Because boolean was mapped to integer for general instructions,
		it will have to be specially matched and mapped to its value
		directly (4).
	 */
	private static final byte newArrayElementTypeMap[] = { 8, 9, 10, 11, 6, 7, 5 };
	static final byte T_BOOLEAN = 4;
	/**
		Create an array instance

		Stack ... =&gt;
		      ...,arrayref
	*/
	public void pushNewArray(String className, int size) {

		push(size);
		popStack(); // int just pushed will be popped by array creation

		Type elementType = cb.factory.type(className);

		// determine the instruction to use based on the element type
		if (elementType.vmType() == BCExpr.vm_reference) {

			// For an array of Java class/interface elements, generate:
			// ANEWARRAY #cpei ; where cpei is a constant pool index for the class

			int cpi = modClass.addClassReference(elementType.javaName());
			// Use U2, not CPE, since only wide form exists.
			myCode.addInstrU2(VMOpcode.ANEWARRAY, cpi);
		} else {
			byte atype;

			// get the argument for the array type
			// if the element type is boolean, we can't use the map
			// because the type id will say integer.
			// but we can use vm_int test to weed out some tests
			if (elementType.vmType() == BCExpr.vm_int &&
			    VMDescriptor.C_BOOLEAN == elementType.vmName().charAt(0))
				atype = T_BOOLEAN;
			else
				atype = newArrayElementTypeMap[elementType.vmType()];

			// For an array of Java builtin type elements, generate:
			// NEWARRAY #atype ; where atype is a constant for the builtin type

			myCode.addInstrU1(VMOpcode.NEWARRAY, atype);
		}

		// an array reference is an object, hence width of 1
		growStack(1, cb.factory.type(className.concat("[]")));
	}
    
    /**
     * Write a instruction that uses a constant pool entry
     * as an operand, add a limit exceeded message if
     * the number of constant pool entries has exceeded
     * the limit.
     */
    private void addInstrCPE(short opcode, int cpe)
    {
        if (cpe >= VMOpcode.MAX_CONSTANT_POOL_ENTRIES)
            cb.addLimitExceeded(this, "constant_pool_count",
                    VMOpcode.MAX_CONSTANT_POOL_ENTRIES, cpe);
        
        myCode.addInstrCPE(opcode, cpe);
    }

	/**
		Tell if statement number in this method builder hits limit.  This
		method builder keeps a counter of how many statements are added to it.
		Caller should call this function every time it tries to add a statement
		to this method builder (counter is increased by 1), then the function
		returns whether the accumulated statement number hits a limit.
		The reason of doing this is that Java compiler has a limit of 64K code
		size for each method.  We might hit this limit if an extremely long
		insert statement is issued, for example (see beetle 4293).  Counting
		statement number is an approximation without too much overhead.
	*/
	public boolean statementNumHitLimit(int noStatementsAdded)
	{
		if (statementNum > 2048)    // 2K limit
		{
			return true;
		}
		else
		{
			statementNum = statementNum + noStatementsAdded;
			return false;
		}
	}
	
	/**
	 * Check to see if the current method byte code is nearing the
	 * limit of 65535. If it is start overflowing to a new method.
	 * <P>
	 * Overflow is handled for a method named e23 as:
	 * <CODE>
	 public Object e23()
	 {
	   ... existing code
	   // split point
	   return e23_0();
	 }
	 private Object e23_0()
	 {
	    ... first set overflowed code
	    // split point
	    return e23_1(); 
	 }
	 private Object e23_1()
	 {
	    ... second set overflowed code
	    // method complete
	    return result; 
	 }
	 	 </CODE>
	 <P>
	 
	 These overflow methods are hidden from the code using this MethodBuilder,
	 it continues to think that it is building a single method with the
	 original name.


	 * <BR> Restrictions:
	 * <UL>
	 * <LI> Only handles methods with no arguments
	 * <LI> Stack depth must be zero
	 * </UL>
	 * 
	 *
	 */
	private void overflowMethodCheck()
	{
        if (stackDepth != 0) {
            // Can only overflow to new method if the stack is empty.
            return;
        }

		if (handlingOverflow)
			return;
		
		// don't sub method in the middle of a conditional
		if (condition != null)
			return;
		
		int currentCodeSize = myCode.getPC();
		
		// Overflow at >= 55,000 bytes which is someway
		// below the limit of 65,535. Ideally overflow
		// would occur at 65535 minus the few bytes needed
		// to call the sub-method, but the issue is at this level
		// we don't know frequently we are called given the restriction
		// of only being called when the stack depth is zero.
		// Thus split earlier to try ensure most cases are caught.
		// Only downside is that we may split into N methods when N-1 would suffice.
		if (currentCodeSize < 55000)
			return;
				
		// only handle no-arg methods at the moment.
		if (parameters != null)
		{
			if (parameters.length != 0)
				return;
		}
        		
		BCMethod subMethod = getNewSubMethod(myReturnType, false);
				
		// stop any recursion
		handlingOverflow = true;
		
		// in this method make a call to the sub method we will
		// be transferring control to.
        callSubMethod(subMethod);
	
		// and return its value, works just as well for a void method!
		this.methodReturn();
		this.complete();
		
		handlingOverflow = false;
		
		// now the tricky bit, make this object take over the
		// code etc. from the sub method. This is done so
		// that any code that has a reference to this MethodBuilder
		// will continue to work. They will be writing code into the
		// new sub method.
		
		this.myEntry = subMethod.myEntry;
		this.myCode = subMethod.myCode;
		this.currentVarNum = subMethod.currentVarNum;
		this.statementNum = subMethod.statementNum;
		
		// copy stack info
		this.stackTypes = subMethod.stackTypes;
		this.stackTypeOffset = subMethod.stackTypeOffset;
		this.maxStack = subMethod.maxStack;
		this.stackDepth = subMethod.stackDepth;
	}
	
    /**
     * Create a sub-method from this method to allow the code builder to split a
     * single logical method into multiple methods to avoid the 64k per-method
     * code size limit. The sub method with inherit the thrown exceptions of
     * this method.
     * 
     * @param returnType
     *            Return type of the new method
     * @param withParameters
     *            True to define the method with matching parameters false to
     *            define it with no parameters.
     * @return A valid empty sub method.
     */
    final BCMethod getNewSubMethod(String returnType, boolean withParameters) {
        int modifiers = myEntry.getModifier();

        // the sub-method can be private to ensure that no-one
        // can call it accidentally from outside the class.
        modifiers &= ~(Modifier.PROTECTED | Modifier.PUBLIC);
        modifiers |= Modifier.PRIVATE;

        String subMethodName = myName + "_s"
                + Integer.toString(subMethodCount++);
        BCMethod subMethod = (BCMethod) cb.newMethodBuilder(modifiers,
                returnType, subMethodName, withParameters ? parameterTypes
                        : null);
        subMethod.thrownExceptions = this.thrownExceptions;
        
        return subMethod;
    }

    /**
     * Call a sub-method created by getNewSubMethod handling parameters
     * correctly.
     */
    final void callSubMethod(BCMethod subMethod) {
        // in this method make a call to the sub method we will
        // be transferring control to.
        short op;
        if ((myEntry.getModifier() & Modifier.STATIC) == 0) {
            op = VMOpcode.INVOKEVIRTUAL;
            this.pushThis();
        } else {
            op = VMOpcode.INVOKESTATIC;
        }

        int parameterCount = subMethod.parameters == null ? 0
                : subMethod.parameters.length;

        // push my parameter values for the call.
        for (int pi = 0; pi < parameterCount; pi++)
            this.getParameter(pi);

        this.callMethod(op, modClass.getName(), subMethod.getName(),
                subMethod.myReturnType, parameterCount);
    }
}


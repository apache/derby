/*

   Derby - Class org.apache.derby.impl.services.bytecode.BCMethod

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.compiler.ClassBuilder;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.classfile.ClassFormatOutput;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.classfile.ClassHolder;
import org.apache.derby.iapi.services.classfile.ClassMember;

import org.apache.derby.iapi.services.sanity.SanityManager;

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
public class BCMethod implements MethodBuilder {

	final BCClass		cb;
	protected final ClassHolder modClass; // the class it is in (modifiable fmt)

	protected BCLocalField[] parameters; 
	protected Vector thrownExceptions; // expected to be names of Classes under Throwable

	final CodeChunk myCode;
	protected ClassMember myEntry;

	private int currentVarNum;
	private int statementNum;

	BCMethod(ClassBuilder cb,
			String returnType,
			String methodName,
			int modifiers,
			String[] parms,
			BCJava factory) {

		this.cb = (BCClass) cb;
		modClass = this.cb.modify();
		//this.modifiers = modifiers;

		if (SanityManager.DEBUG) {
   			this.cb.validateType(returnType);
		}

		// if the method is not static, allocate for "this".
		if ((modifiers & Modifier.STATIC) == 0 )
			currentVarNum = 1;

		String[] vmParamterTypes;

		if (parms != null) {
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
		myCode = new CodeChunk(true);
	}
	//
	// MethodBuilder interface
	//

	public String getName() {
		return myEntry.getName();
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

		if (thrownExceptions == null)
			thrownExceptions = new Vector();
		thrownExceptions.addElement(exceptionClass);
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
		// write exceptions attribute info
		writeExceptions();

		// get the code attribute to put itself into the class
		// provide the final header information needed
		myCode.complete(modClass, myEntry, maxStack, currentVarNum);
	}

	/*
	 * class interface
	 */

	/**
	 * In their giveCode methods, the parts of the method body
	 * will want to get to the constant pool to add their constants.
	 * We really only want them treating it like a constant pool
	 * inclusion mechanism, we could write a wrapper to limit it to that.
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
					String e = thrownExceptions.elementAt(i).toString();
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

	private Type[]	stackTypes = new Type[8];
	private int     stackTypeOffset;

	private int maxStack;
	private int stackDepth;
	// public Stack stackTypes = new Stack();

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
			chunk.addInstrCPE(VMOpcode.LDC, cpe);
		}
		growStack(1, type);
		
	}

	public void push(long value){
		CodeChunk chunk = myCode;

		if (value == 0 || value == 1) {
				chunk.addInstr((short)(VMOpcode.LCONST_0+(short)value));
		}
		else {
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
			chunk.addInstrCPE(VMOpcode.LDC, cpe);
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
		myCode.addInstrCPE(VMOpcode.LDC, cpe);
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
		Type ct = cb.factory.type(className);
		Type tbc = popStack();

		short sourceType = tbc.vmType();
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
		word2,word1 -> word2, word1, word2

		So that we are left with word after the put.

	*/
	public void putField(LocalField field) {
		BCLocalField lf = (BCLocalField) field;
		Type lt = lf.type;

		putField(lf.type, lf.cpi);
	}

	/**
		Pop the top stack value and store it in the instance field of this class.
	*/
	public void putField(String fieldName, String fieldType) {

		Type ft = cb.factory.type(fieldType);
		int cpi = modClass.addFieldReference(cb.classType.vmNameSimple, fieldName, ft.vmName());

		putField(ft, cpi);
	}

	private void putField(Type fieldType, int cpi) {

		// now have ...,value
		myCode.addInstr(fieldType.width() == 2  ? VMOpcode.DUP2 : VMOpcode.DUP);
		growStack(fieldType);

		// now have ...,value,value
		pushThis();
		// now have ...,value,value,this
		swap();
		/*
		if (fieldType.width() == 1) {
			myCode.addInstr(VMOpcode.SWAP);
			Type t1 = popStack();
			Type t2 = popStack();
			growStack(t1);
			growStack(t2);

			// now have ...,word,this,word

		} else {

			// now have wA,wB,wA,wB,this
			myCode.addInstr(VMOpcode.DUP_X2);

			Type t1 = popStack();
			Type t2 = popStack();
			growStack(t1);
			growStack(t2);
			growStack(t1);

			// now have wA,wB,this,wA,wB,this
			myCode.addInstr(VMOpcode.POP);
			popStack();

			// now have wA,wB,this,wA,wB
		}
*/
		myCode.addInstrU2(VMOpcode.PUTFIELD, cpi);
		popStack(); // the value
		popStack(); // this
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


		int clearTo = stackTypeOffset;

		condition = new Conditional(condition, myCode, opcode, clearTo);
	}

	public void startElseCode() {
		int clearTo = condition.startElse(myCode, stackTypeOffset);

		if (SanityManager.DEBUG) {
			if ((stackTypeOffset - 1) != clearTo)
				SanityManager.THROWASSERT(stackTypeOffset + " is not one more than " + clearTo);
		}

		while (stackTypeOffset > clearTo) {
			popStack();
		}
	}
	public void completeConditional() {
		condition = condition.end(myCode, stackTypeOffset);
	}

	public void endStatement() {
		if (stackDepth != 0) {
			Type toPop = popStack();

			myCode.addInstr(toPop.width() == 2  ? VMOpcode.POP2 : VMOpcode.POP);

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

		Stack ... =>
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
			return true;
		else
		{
			statementNum = statementNum + noStatementsAdded;
			return false;
		}
	}
}


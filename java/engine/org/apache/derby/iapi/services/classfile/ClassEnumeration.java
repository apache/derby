/*

   Derby - Class org.apache.derby.iapi.services.classfile.ClassEnumeration

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

package org.apache.derby.iapi.services.classfile;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.StringTokenizer;
import org.apache.derby.iapi.services.classfile.VMDescriptor;

/**
	An enumeration that filters only classes
	from the enumeration of the class pool.

	Code has been added to also include classes referenced in method and
	field signatures.
*/


class ClassEnumeration implements Enumeration {
	ClassHolder	cpt;
	Enumeration			inner;
	CONSTANT_Index_info	position;
	HashSet           foundClasses;
    Enumeration         classList;

    ClassEnumeration(   ClassHolder cpt,
                        Enumeration e,
                        Enumeration methods,
                        Enumeration fields)
    {
		this.cpt = cpt;
		inner = e;
		foundClasses = new HashSet(30, 0.8f);
		findMethodReferences(methods, foundClasses);
		findFieldReferences(fields, foundClasses);
		findClassReferences(foundClasses);
		classList = java.util.Collections.enumeration(foundClasses);

	}

	public boolean hasMoreElements() {
	    return classList.hasMoreElements();
	}

	// uses cpt and inner
	private void findClassReferences(HashSet foundClasses)
	{

		ConstantPoolEntry	item;
		CONSTANT_Index_info	ref;


		while (inner.hasMoreElements())
		{
			item = (ConstantPoolEntry) inner.nextElement();
			if (item == null)
				continue;
			if (item.getTag() == VMDescriptor.CONSTANT_Class)
			{
				ref = (CONSTANT_Index_info) item;

				String className = cpt.className(ref.getIndex());

				// if this is an array type, distillClasses can
				// handle it
                if (className.startsWith("["))
                {
                   distillClasses(className, foundClasses);
                   continue;
                }

                // now we've got either a primitive type or a classname
                // primitive types are all a single char

                if (className.length() > 1)
                {
                    //we've got a class
                    if (className.startsWith("java"))
                    {
                        //skip it
                        continue;
                    }

                    foundClasses.add(className);
                }
			}
		}

	}

	private void findMethodReferences(  Enumeration methods,
	                                    HashSet foundClasses)
	{
	    while (methods.hasMoreElements())
	    {
	        ClassMember member = (ClassMember) methods.nextElement();
	        String description = member.getDescriptor();
	        distillClasses(description, foundClasses);
	    }
	}

	private void findFieldReferences(   Enumeration fields,
	                                    HashSet foundClasses)
	{
	    while (fields.hasMoreElements())
	    {
	        ClassMember member = (ClassMember) fields.nextElement();
	        String description = member.getDescriptor();
	        distillClasses(description, foundClasses);
	    }
	}

	void distillClasses(String fieldOrMethodSig, HashSet foundClasses)
	{
	    if (fieldOrMethodSig == null || fieldOrMethodSig.length() < 1)
	    {
	        //empty string
	        return;
	    }

	    if (fieldOrMethodSig.charAt(0) != '(')
	    {
    	    // first time through, we're dealing with a field here
    	    // otherwise, it is a token from a method signature

            int classNameStart = fieldOrMethodSig.indexOf('L');

            if (classNameStart == -1)
            {
                // no class in the type, so stop
                return;
            }

            // chop off any leading ['s or other Java-primitive type
            // signifiers (like I or L) *AND* substitute the dots
	        String fieldType =
	            fieldOrMethodSig.substring(classNameStart + 1).replace('/', '.');

            // we have to check for the semi-colon in case we are
            // actually looking at a token from a method signature
	        if (fieldType.endsWith(";"))
	        {
    	        fieldType = fieldType.substring(0,fieldType.length()-1);
            }

	        if (fieldType.startsWith("java"))
	        {
	            return;     // it's a java base class and we don't care about
	                        // that either
	        }

            foundClasses.add(fieldType);
            return;
         }
         else
         {
            // it's a method signature
            StringTokenizer tokens = new StringTokenizer(fieldOrMethodSig, "();[");
            while (tokens.hasMoreElements())
            {
                String aToken = (String) tokens.nextToken();
                // because of the semi-colon delimiter in the tokenizer, we
                // can have only one class name per token and it must be the
                // last item in the token
                int classNameStart = aToken.indexOf('L');
                if (classNameStart != -1)
                {
                    distillClasses(aToken, foundClasses);
                }
                else
                {
                    continue;
                }
            }
         }
     }

	public Object nextElement() {
        return classList.nextElement();
	}

}

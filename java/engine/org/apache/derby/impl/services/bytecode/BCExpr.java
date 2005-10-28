/*

   Derby - Class org.apache.derby.impl.services.bytecode.BCExpr

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

/**
 *
 * To be able to identify the expressions as belonging to this
 * implementation, and to be able to generate code off of
 * it if so.
 *
 */
interface BCExpr {

	// maybe these should go into Declarations, instead?
	// note there is no vm_boolean; boolean is an int
	// except in arrays, where it is a byte.
	short vm_void = -1; // not used in array mappings.
	short vm_byte = 0;
	short vm_short = 1;
	short vm_int = 2;
	short vm_long = 3;
	short vm_float = 4;
	short vm_double = 5;
	short vm_char = 6;
	short vm_reference = 7;

}

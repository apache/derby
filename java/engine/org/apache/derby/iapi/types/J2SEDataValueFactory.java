/*

   Derby - Class org.apache.derby.iapi.types.J2SEDataValueFactory

   Copyright 1999, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.types;

import java.util.Properties;

import org.apache.derby.iapi.error.StandardException;

/**
 * DataValueFactory implementation for J2SE.
 * Uses SQLDecimal for DECIMAL which implements
 * DECIMAL functionality using java.math.BigDecimal.
 *
 * @see DataValueFactory
 */
public class J2SEDataValueFactory extends DataValueFactoryImpl
{
	public J2SEDataValueFactory() {
	}

   	public void boot(boolean create, Properties properties) throws StandardException {
   		
   		NumberDataType.MINLONG_MINUS_ONE = SQLDecimal.MINLONG_MINUS_ONE;
   		NumberDataType.MAXLONG_PLUS_ONE = SQLDecimal.MAXLONG_PLUS_ONE;

    	super.boot(create, properties);
   	}
	
	public NumberDataValue getDecimalDataValue(Long value,
			NumberDataValue previous) throws StandardException {
		if (previous == null)
			previous = new SQLDecimal();

		previous.setValue(value);
		return previous;
	}

	public NumberDataValue getDecimalDataValue(String value)
			throws StandardException {
		if (value != null)
			return new SQLDecimal(value);
		else
			return new SQLDecimal();
	}

	public NumberDataValue getNullDecimal(NumberDataValue dataValue) {
		if (dataValue == null) {
			return new SQLDecimal();
		} else {
			dataValue.setToNull();
			return dataValue;
		}
	}
}

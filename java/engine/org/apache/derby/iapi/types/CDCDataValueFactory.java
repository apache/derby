/*

   Derby - Class org.apache.derby.iapi.types.CDCDataValueFactory

   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

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
import org.apache.derby.iapi.services.info.JVMInfo;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.error.StandardException;

/**
 * DataValueFactory implementation for J2ME/CDC/Foundation.
 * Cannot use SQLDecimal since that requires java.math.BigDecimal.
 * Uses BigIntegerDecimal for DECIMAL support.
 *
 * @see DataValueFactory
 */

public class CDCDataValueFactory extends DataValueFactoryImpl
	implements ModuleSupportable
{
     /**
	 *     Make the constructor public.
	 *
	 */
	public CDCDataValueFactory() {
	}
	
	/* (non-Javadoc)
	 * @see org.apache.derby.iapi.services.monitor.ModuleSupportable#canSupport(java.util.Properties)
	 */
	public boolean canSupport(Properties properties) {
		return JVMInfo.J2ME;
	}
	public NumberDataValue getDecimalDataValue(Long value,
			NumberDataValue previous) throws StandardException {
		if (previous == null)
			previous = new BigIntegerDecimal();

		previous.setValue(value);
		return previous;
	}

	public NumberDataValue getDecimalDataValue(String value)
			throws StandardException {
		NumberDataValue ndv = new BigIntegerDecimal();

		ndv.setValue(value);
		return ndv;
	}

	public NumberDataValue getNullDecimal(NumberDataValue dataValue) {
		if (dataValue == null) {
			return new BigIntegerDecimal();
		} else {
			dataValue.setToNull();
			return dataValue;
		}
	}
}

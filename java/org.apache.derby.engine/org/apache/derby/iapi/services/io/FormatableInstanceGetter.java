/*

   Derby - Class org.apache.derby.iapi.services.io.FormatableInstanceGetter

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

package org.apache.derby.iapi.services.io;

import org.apache.derby.iapi.services.loader.InstanceGetter;

/**
 * Class that loads Formattables (typically from disk)through
 * one level of indirection.
 * A concrete implementation of this class is registered as the
 * class to handle a number of format identifiers in RegisteredFormatIds.
 * When the in-memory representation of RegisteredFormatIds is set up
 * an instance of the concrete class will be created for each format
 * identifier the class is registered for, and each instances will
 * have its setFormatId() called once with the appropriate format identifier.
 * 
 * <BR>
 * When a Formattable object is read from disk and its registered class
 * is an instance of FormatableInstanceGetter the getNewInstance() method
 * will be called to create the object.
 * The implementation can use the fmtId field to determine the
 * class of the instance to be returned.
 * <BR>
 * Instances of FormatableInstanceGetter are system wide, that is there is
 * a single set of RegisteredFormatIds per system.
 * 
 * @see RegisteredFormatIds
 */
public abstract class FormatableInstanceGetter implements InstanceGetter {

    /**
     * Format identifier of the object 
     */
	protected int fmtId;

    /**
     * Set the format identifier that this instance will be loading from disk.
     *
     * @param fmtId A format identifier
    */
	public final void setFormatId(int fmtId) {
		this.fmtId = fmtId;
	}
}

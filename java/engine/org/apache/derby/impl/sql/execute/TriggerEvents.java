/*

   Derby - Class org.apache.derby.impl.sql.execute.TriggerEvents

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.execute;

/**
 * Static final trigger events.  One for
 * each known trigger event.  Use these rather
 * than constructing a new TriggerEvent.
 *
 * @author jamie
 */
public class TriggerEvents
{
	public static final TriggerEvent BEFORE_INSERT = new TriggerEvent(TriggerEvent.BEFORE_INSERT);
	public static final TriggerEvent BEFORE_DELETE = new TriggerEvent(TriggerEvent.BEFORE_DELETE);
	public static final TriggerEvent BEFORE_UPDATE = new TriggerEvent(TriggerEvent.BEFORE_UPDATE);
	public static final TriggerEvent AFTER_INSERT = new TriggerEvent(TriggerEvent.AFTER_INSERT);
	public static final TriggerEvent AFTER_DELETE = new TriggerEvent(TriggerEvent.AFTER_DELETE);
	public static final TriggerEvent AFTER_UPDATE = new TriggerEvent(TriggerEvent.AFTER_UPDATE);
}

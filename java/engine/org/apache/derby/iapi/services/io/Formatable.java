/*

   Derby - Class org.apache.derby.iapi.services.io.Formatable

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

package org.apache.derby.iapi.services.io;

import java.io.Externalizable;

/**
  Cloudscape interface for creating a stored form for
  an object and re-constructing an equivalent object
  from this stored form. The object which creates the
  stored form and the re-constructed object need not be
  the same or related classes. They must share the same
  TypedFormat.

 */
public interface Formatable
extends Externalizable, TypedFormat
{
}

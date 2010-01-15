/*

   Derby - Class org.apache.derby.iapi.services.io.InputStreamUtil

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

import java.io.*;

/**
	Utility methods for InputStream that are stand-ins for
	a small subset of DataInput methods. This avoids pushing
	a DataInputStream just to get this functionality.
*/
public final class InputStreamUtil extends org.apache.derby.shared.common.io.InputStreamUtil
{}

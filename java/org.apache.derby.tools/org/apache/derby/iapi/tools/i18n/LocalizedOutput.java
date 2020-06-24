/*

   Derby - Class org.apache.derby.iapi.tools.i18n.LocalizedOutput

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derby.iapi.tools.i18n;

import java.io.PrintWriter;
import java.io.OutputStreamWriter;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

public class LocalizedOutput extends PrintWriter {
	private OutputStream out;
	public LocalizedOutput(OutputStream o){
		super(new OutputStreamWriter(o), true);
		out = o;
	}
//IC see: https://issues.apache.org/jira/browse/DERBY-777
	LocalizedOutput(OutputStream o, String enc) throws UnsupportedEncodingException {
		super(new OutputStreamWriter(o, enc), true);
		out = o;
	}
	public boolean isStandardOutput(){
		return (out == System.out);
	}
	public void close() {
		if (!isStandardOutput()) {
			super.close();
		}
	}
}

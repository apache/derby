/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.jdk111

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

package org.apache.derbyTesting.functionTests.harness;

import java.util.Vector;
import java.util.StringTokenizer;


public class jdk111 extends jdk110
{
    public String getName(){return "jdk111";}
    public jdk111(boolean noasyncgc, boolean verbosegc, boolean noclassgc,
        long ss, long oss, long ms, long mx, String classpath, String prof,
        boolean verify, boolean noverify, boolean nojit, Vector<String> D) {
        super(noasyncgc,verbosegc,noclassgc,ss,oss,ms,mx,classpath,prof,
            verify,noverify,nojit,D);
    }

    public jdk111(String classpath, Vector<String> D) {
        super(classpath,D);
    }

    public jdk111(long ms, long mx, String classpath, Vector<String> D) {
        super(ms,mx,classpath,D);
    }

    public jdk111() { super(); }
}

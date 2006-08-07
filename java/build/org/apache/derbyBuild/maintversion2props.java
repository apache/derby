/*

   Derby - Class org.apache.derbyBuild.classlister

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

package org.apache.derbyBuild;

import java.io.*;
import java.util.*;

/**

    A quick and dirty class for generating a properties file from the maint
    property in DBMS.properties and release.properties. Useful for getting
    the values of the third and fourth parts of the version number into Ant
    as separate properties. It puts the third value into the output properties
    as the property "interim", and the fourth value as "point".

    Usage: java maintversion2props input_properties_file output_properties_file

**/
    
public class maintversion2props
{
    public static void main(String[] args) throws Exception
    {
        InputStream is = new FileInputStream(args[0]);
        Properties p = new Properties();
        p.load(is);
	String maint = "";
        if (args[0].indexOf("DBMS") > 0)
        {
          maint = p.getProperty("derby.version.maint");
        } else if (args[0].indexOf("release") > 0)
        { 
          maint = p.getProperty("maint");
        }
        Properties p2 = new Properties();
        p2.setProperty("interim", Integer.toString(Integer.parseInt(maint) / 1000000));
        p2.setProperty("point", Integer.toString(Integer.parseInt(maint) % 1000000));
        OutputStream os = new FileOutputStream(args[1]);
        p2.store(os, ""); 
    }
}

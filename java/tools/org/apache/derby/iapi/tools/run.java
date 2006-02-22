/*

   Derby - Class org.apache.derby.tools.iapi.run

   Copyright 2006 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.tools.iapi;

import java.io.IOException;
import org.apache.derby.tools.dblook;
import org.apache.derby.tools.ij;
import org.apache.derby.tools.sysinfo;
import org.apache.derby.iapi.tools.i18n.LocalizedResource;

/**
  The run class facilitates running the various Derby utilities with the
  java -jar command. For example:

  java -jar derbytools.jar ij
  java -jar derbytools.jar sysinfo
  java -jar derbytools.jar dblook
*/
public class run {

  /**
  	 Switch on the first argument to choose the tool, pass the remaining
         arguments to the tool.
   */
  static public void main(String[] args) throws IOException {
      if (args.length < 1) {
          printUsage();
      } else if (args[0].equals("ij")) {
          ij.main(trimArgs(args));
      } else if (args[0].equals("sysinfo")) {
          sysinfo.main(trimArgs(args));
      } else if (args[0].equals("dblook")) {
          dblook.main(trimArgs(args));
      } else printUsage();
  }

  /*
       Private constructor. No instances allowed.
   */
  private run() { 
  }
  
  /*
       Utility method to trim one element off of the argument array.
       @param args the arguments array
       @return trimmed the trimmed array
   */
  private static String[] trimArgs(String[] args)
  {
      String [] trimmed = new String[args.length - 1];
      System.arraycopy(args, 1, trimmed, 0, args.length - 1);
      return trimmed; 
  }

  /*
       Print the usage statement if the user didn't enter a valid choice
       of tool.
   */
  public static void printUsage()
  {
      LocalizedResource locRes = LocalizedResource.getInstance();
      System.err.println(locRes.getTextMessage("RUN_Usage"));
  }
}

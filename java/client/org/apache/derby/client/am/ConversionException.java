/*

   Derby - Class org.apache.derby.client.am.ConversionException

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

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

package org.apache.derby.client.am;

public class ConversionException extends java.lang.Exception
{
  String sqlState_;
  int errorCode_;

  public ConversionException (String s, String sqlState, int errorCode)
  {
    super ("[converters] " + s);
    sqlState_ = sqlState;
    errorCode_ = errorCode;
  }

  public ConversionException (String s)
  { super ("[converters] " + s); }

  public ConversionException ()
  { this ("[converters] "); }
}

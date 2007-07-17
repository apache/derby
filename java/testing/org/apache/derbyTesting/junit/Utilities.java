/*
 *
 * Derby - Class Utilities
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.junit;

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * General non-JDBC related utilities relocated from TestUtil
 *
 *
 */
public class Utilities {

    public Utilities() {
        // TODO Auto-generated constructor stub
    }
        /**
         * Just converts a string to a hex literal to assist in converting test
         * cases that used to insert strings into bit data tables
         * Converts using UTF-16BE just like the old casts used to.
         *
         * @param s  String to convert  (e.g
         * @return hex literal that can be inserted into a bit column.
         */
        public static String stringToHexLiteral(String s)
        {
                byte[] bytes;
                String hexLiteral = null;
                try {
                        bytes = s.getBytes("UTF-16BE");
                        hexLiteral = convertToHexString(bytes);
                }
                catch (UnsupportedEncodingException ue)
                {
                        System.out.println("This shouldn't happen as UTF-16BE should be supported");
                        ue.printStackTrace();
                }

                return hexLiteral;
        }

        /**
         * Convert a byte array to a hex string suitable for insert 
         * @param buf  byte array to convert
         * @return     formated string representing byte array
         */
        private static String convertToHexString(byte [] buf)
        {
                StringBuffer str = new StringBuffer();
                str.append("X'");
                String val;
                int byteVal;
                for (int i = 0; i < buf.length; i++)
                {
                        byteVal = buf[i] & 0xff;
                        val = Integer.toHexString(byteVal);
                        if (val.length() < 2)
                                str.append("0");
                        str.append(val);
                }
                return str.toString() +"'";
        }

    	/**
    	 * repeatChar is used to create strings of varying lengths.
    	 * called from various tests to test edge cases and such.
    	 *
    	 * @param c             character to repeat
    	 * @param repeatCount   Number of times to repeat character
    	 * @return              String of repeatCount characters c
    	 */
       public static String repeatChar(String c, int repeatCount)
       {
    	   char ch = c.charAt(0);

    	   char[] chArray = new char[repeatCount];
    	   for (int i = 0; i < repeatCount; i++)
    	   {
    		   chArray[i] = ch;
    	   }

    	   return new String(chArray);

       }

        /**
         * Print out resultSet in two dimensional array format, for use by
         * JDBC.assertFullResultSet(rs,expectedRows) expectedRows argument.
         * Useful while converting tests to get output in correct format.
         * 
         * @param rs
         * @throws SQLException
         */
        public static void showResultSet(ResultSet rs) throws SQLException {
            System.out.print("{");
            int row = 0;
            boolean next = rs.next();
            while (next) {
                row++;
                ResultSetMetaData rsmd = rs.getMetaData();
                int nocols = rsmd.getColumnCount();
                System.out.print("{");
                
                for (int i = 0; i < nocols; i++)
                {
                    System.out.print("\"" + rs.getString(i+1) + "\"");
                    if (i == (nocols -1))
                        System.out.print("}");
                    else
                        System.out.print(",");
                           
                }
                next = rs.next();
                   
                if (next)
                    System.out.println(",");
                else
                    System.out.println("};");
            }
        }       

}

/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.TestRowId
 
   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.
 
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

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import org.apache.derby.impl.jdbc.Util;
import java.sql.RowId;
import java.lang.UnsupportedOperationException;

public class TestRowId {
    
    RowId rid=null;
    
    java.lang.UnsupportedOperationException unop=null;
    String message=null;
    
    public TestRowId() {
        unop = new java.lang.UnsupportedOperationException();
        message = unop.getMessage();
    }
    
    void t_equals() {
        try {
            rid.equals(new String());
        } catch(java.lang.UnsupportedOperationException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Unexpected Exception caught");
            e.printStackTrace();
        }
    }
    
    void t_getBytes() {
        try {
            byte b[] = rid.getBytes();
        } catch(java.lang.UnsupportedOperationException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Unexpected Exception caught");
            e.printStackTrace();
        }
    }
    
    void t_toString() {
        try {
            String s = rid.toString();
        } catch(java.lang.UnsupportedOperationException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Unexpected Exception caught");
            e.printStackTrace();
        }
    }
    
    void t_hashCode() {
        try {
            int h = rid.hashCode();
        } catch(java.lang.UnsupportedOperationException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Unexpected Exception caught");
            e.printStackTrace();
        }
    }
    
    void startTestRowIdMethods(RowId rid_in) {
        rid = rid_in;
        t_equals();
        t_getBytes();
        t_toString();
        t_hashCode();
    }
    
    public static void main(String args[]) {
        RowId rowId_main=null;
        TestRowId rid_obj = new TestRowId();
        
        try {
            rowId_main = new org.apache.derby.impl.jdbc.EmbedRowId();
        } catch(Exception e) {
            System.out.println("Exception occurred in the main method");
            e.printStackTrace();
        }
        
        try {
            System.out.println(""+rid_obj);
            System.out.println(""+rowId_main);
            rid_obj.startTestRowIdMethods(rowId_main);
        } catch(Exception e) {
            System.out.println("Caught it here"+e);
            e.printStackTrace();
        }
    }
}

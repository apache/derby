/*

   Derby - Class org.apache.derby.impl.tools.ij.AttributeHolder

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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


package org.apache.derby.impl.tools.ij;

import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.tools.i18n.LocalizedResource;
import org.apache.derby.tools.URLCheck;
import java.util.Locale;
import java.util.Vector;
import java.util.Properties;
import java.util.Enumeration;
import java.util.StringTokenizer;
import java.lang.reflect.Field;
import java.sql.SQLException;

public class AttributeHolder {

    //This is an inner class.  This class hold the details about each
    //specific attribute which includes what the attribute is and
    //any error found.
    String name;
    String value;
    String token;
    Vector errors = new Vector();

    public String getName(){
      return name;
    }
    public void setName(String aString){
      name = aString;
    }
    public String getValue(){
      return value;
    }
    public void setValue(String aString){
      value = aString;
    }
    public String getToken(){
      return token;
    }
    public void setToken(String aString){
      token = aString;
    }
    public void addError(String aString) {
      //Keep track of error message for later display.
      if (!errors.contains(aString))
        errors.addElement(aString);
    }
    public void check( Vector validProps){
      checkName( validProps);
      //checkValue();
      displayErrors();
    }
    public void displayErrors(){
      //If no error are found then nothing is displayed.
      Enumeration enum = errors.elements();
      //In the first line, show the exact token that was parsed from
      //the URL.
      if (enum.hasMoreElements())
        display(LocalizedResource.getMessage("TL_urlLabel1", "[", getToken(), "]"));
      //Show all errors.  More than one error can be found for an attribute.
      while (enum.hasMoreElements()){
        String aString = (String)enum.nextElement();
        displayIndented(aString);
      }
    }
    public void checkName( Vector validProps){
      if( validProps == null)
          return; // valid properties are unknown
      String anAtt = getName();
      try {
        //Check the found name against valid names.
        if (!validProps.contains(anAtt)) {
          //Check for case spelling of the name.
          if (validProps.contains(anAtt.toLowerCase(java.util.Locale.ENGLISH))) {
            errors.addElement(LocalizedResource.getMessage("TL_incorCase"));
          }
          //Check if this is even a valid attribute name.
          else {
            errors.addElement(LocalizedResource.getMessage("TL_unknownAtt"));
          }
        }
        else {
          //This Is a valid attribute.
        }
      }
      catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    public void checkValue(){
      String anAtt = getName(); 
      String aValue = getValue();
      try {
        //Check all attribute that require a boolean.
        if (URLCheck.getBooleanAttributes().contains(anAtt)) {
          if (!checkBoolean(aValue)) {
            errors.addElement(LocalizedResource.getMessage("TL_trueFalse"));
          }
        }
      }
      catch (Exception ex) {
        ex.printStackTrace();
      }
    }
	  public boolean checkBoolean(String aValue) {
		  if (aValue == null)
			  return false;
		  return aValue.toLowerCase(Locale.ENGLISH).equals("true") || 
			  aValue.toLowerCase(Locale.ENGLISH).equals("false");
	  }
    public void display(String aString) {
		LocalizedResource.OutputWriter().println(aString);
    }
    public void displayIndented(String aString) {
		LocalizedResource.OutputWriter().println("   " + aString);
    }
  }

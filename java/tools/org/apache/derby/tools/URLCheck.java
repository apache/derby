/*

   Derby - Class org.apache.derby.tools.URLCheck

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

package org.apache.derby.tools;

import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.tools.i18n.LocalizedResource;
import org.apache.derby.impl.tools.ij.AttributeHolder;
import java.util.Vector;
import java.util.Properties;
import java.util.Enumeration;
import java.util.StringTokenizer;
import java.lang.reflect.Field;
import java.sql.SQLException;

/**
 * This class takes a string used for a connection URL and checks for
 * correctness.
 * To turn off output in ij, use the command line
 * property of -DURLCheck=false.
 *
 * param anURL	 The URL used to connect to a database.
 *
 */

public class URLCheck {

  public Vector attributes;
  public static Vector booleanAttributes;
  //Need so that AppUI class does not get garbage collected
  LocalizedResource langUtil = LocalizedResource.getInstance();
  Vector validProps;

  public URLCheck(String anURL) {

    try {
      //Initialize the AppUI class

      //Parse the URL string into properties.
      Properties props = getAttributes(anURL, new Properties());
      check();
    }
    catch (Exception ex) {
      ex.printStackTrace();
    }
  }
 

  public static void main(String[] args) {
    if (args.length > 0) {
      //Get the first argument passed in.
      URLCheck aCheck = new URLCheck(args[0]);
    }
  }
  public void check(){
    Enumeration enum = attributes.elements();
    while (enum.hasMoreElements()) {
      AttributeHolder anAttribute = (AttributeHolder)enum.nextElement();
      //The check for duplicate must be done at the URLCheck level
      //and not by each specific attribute.  Only URLCheck knowns about
      //all of the attributes and names.
      checkForDuplicate(anAttribute);
      //Have each attribute check as much about themself as possible.
      anAttribute.check( validProps);
    }
  }
  public void checkForDuplicate(AttributeHolder anAttribute){
    Enumeration enum = attributes.elements();
    while (enum.hasMoreElements()) {
      AttributeHolder aHolder = (AttributeHolder)enum.nextElement();
      //If a duplicate is found, make sure that the message is only shown
      //once for each attribute.
      if (anAttribute != aHolder && anAttribute.getName().equals(aHolder.getName())) {
        anAttribute.addError(langUtil.getTextMessage("TL_dupAtt"));
      }
    }

  }
	public Properties getAttributes(String url, Properties props) throws Exception {
		
		String protocol = "";

        if( url.startsWith( "jdbc:derby:net:"))
		{
            validProps = null;
		}
        else if( url.startsWith( "jdbc:derby:"))
		{
			protocol = "jdbc:derby:";
            validProps = getValidDerbyProps();
		}
        else
            validProps = null;

		
		//Parse the url into attributes and put them in a Properties object.
		StringTokenizer st = new StringTokenizer(url.substring(protocol.length()), ";:\"");
		attributes = new Vector();
		while (st.hasMoreTokens()) {
      AttributeHolder anAttribute = new AttributeHolder();
      String anAtt = "";
      String aValue = "";
	  String aToken = st.nextToken();
      //The "=" is the seperator between key and value.
	  int eqPos = aToken.indexOf('=');
	  if (eqPos == -1) {
		  //If there is no "=" this is not an attribute
		  continue;
      }
      else {
        anAtt = (aToken.substring(0, eqPos)).trim();
        aValue = (aToken.substring(eqPos + 1)).trim();

      }
      anAttribute.setName(anAtt);
      anAttribute.setValue(aValue);
      anAttribute.setToken(aToken);
      attributes.addElement(anAttribute);
      props.put(anAtt, aToken);
	}
		return props;
	}

  public static Vector getBooleanAttributes(){
    if (booleanAttributes == null) {
      booleanAttributes = new Vector();
		  booleanAttributes.addElement(Attribute.DATA_ENCRYPTION);
		  booleanAttributes.addElement(Attribute.CREATE_ATTR);
		  booleanAttributes.addElement(Attribute.SHUTDOWN_ATTR);
		  booleanAttributes.addElement(Attribute.UPGRADE_ATTR);
    }
    return booleanAttributes;
  }

    private static Vector validDerbyProps;
    private Vector getValidDerbyProps()
    {
        if( validDerbyProps == null)
        {
            try
            {
                Vector props = new Vector();
                Class att = Attribute.class;
                //Use reflection to get the list of valid keys from the Attribute class.
                //The Attribute class is an interface and therefore all the field
                //for it are public.
                Field[] fields = att.getFields();
                for (int i = 0; i < fields.length; i++)
                {
                    Field aField = (Field)fields[i];
                    props.addElement(aField.get(att));
                }
                validDerbyProps = props;
            }
            catch (Exception ex)
            {
                ex.printStackTrace();
            }
        }
        return validDerbyProps;
    } // end of getValidDerbyProps

}

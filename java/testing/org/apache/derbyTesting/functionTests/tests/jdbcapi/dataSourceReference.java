/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.dataSourcePermissions

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;

import javax.naming.*;
import javax.naming.spi.ObjectFactory;

/**
 * Test obtaining a javax.naming.Reference from a Derby data source
 * and recreating a Derby data source from it. Tests that the recreated
 * value has the same value for all the properties the data source supports.
 * The list of properties is obtained dynamically from the getXXX methods
 * that return int, String, boolean, short, long. Should Derby data sources
 * support any other bean property types then this test should be modified
 * to pick them up and handle them. Hopefully the test should fail when such
 * a property is added.
 * 
 * At no point does this test attempt to connect using these data sources.
 */
public class dataSourceReference
{
	public static void main(String[] args) throws Exception {

		System.out.println("Starting dataSourceReference");
		
		testDSReference("org.apache.derby.jdbc.EmbeddedDataSource");
		testDSReference("org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource");
		testDSReference("org.apache.derby.jdbc.EmbeddedXADataSource");
		
		
		testDSReference("org.apache.derby.jdbc.ClientDataSource");
		testDSReference("org.apache.derby.jdbc.ClientConnectionPoolDataSource");
		testDSReference("org.apache.derby.jdbc.ClientXADataSource");
		
		System.out.println("Completed dataSourceReference");

	}
	
	/**
	 * Test a data source
	 * <OL>
	 * <LI> Create an empty one from the class name
	 * <LI> Discover the property list
	 * <LI> Create a reference and recreate a data source
	 * <LI> Compare the two
	 * <LI> Serialize athe data source and recreate
	 * <LI> Compare the two
	 * <LI> Set every property for the data source
	 * <LI> Create a reference and recreate a data source
	 * <LI>  Compare the two
	 * </OL>
	 * @param dsName
	 * @throws Exception
	 */
	private static void testDSReference(String dsName) throws Exception
	{
		Object ds = Class.forName(dsName).newInstance();
		
		System.out.println("DataSource class " + dsName);
		String[] properties = getPropertyBeanList(ds);
		System.out.println(" property list");
		for (int i = 0; i < properties.length; i++)
		{
			System.out.println("  " + properties[i]);
		}
		
		Referenceable refDS = (Referenceable) ds;
		
		Reference dsAsReference = refDS.getReference();
		
		String factoryClassName = dsAsReference.getFactoryClassName();
		
		ObjectFactory factory = (ObjectFactory) Class.forName(factoryClassName).newInstance();	
		
		Object recreatedDS = factory.getObjectInstance(dsAsReference, null, null, null);
		
		System.out.println(" empty DataSource recreated using Reference as " + recreatedDS.getClass().getName());
		if (recreatedDS == ds)
			System.out.println("FAIL recreated as same instance!");
		
		compareDS(properties, ds, recreatedDS);
		
		// now serialize and recreate
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);	
		oos.writeObject(ds);
		oos.flush();
		oos.close();
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		ObjectInputStream ois = new ObjectInputStream(bais);
		recreatedDS = ois.readObject();
		System.out.println(" empty DataSource recreated using serialization");
		compareDS(properties, ds, recreatedDS);
		
		// now populate the data source
		for (int i = 0; i < properties.length; i++)
		{
			String property = properties[i];
			Method getMethod = getGet(property, ds);
			
			Method setMethod = getSet(getMethod, ds);
			
			Class pt = getMethod.getReturnType();
			
			// generate a somewhat unique value for a property
			int val = 0;
			for (int j = 0; j < property.length(); j++)
				val += property.charAt(j);
			
			if (pt.equals(Integer.TYPE))
			{
				setMethod.invoke(ds, new Object[] {new Integer(val)});
				continue;
			}
			if (pt.equals(String.class))
			{
				String value;
				if (property.equals("createDatabase"))
					value = "create";
				else if (property.equals("shutdownDatabase"))
					value = "shutdown";
				else
					value = "XX_" + property + "_" + val;
					
				setMethod.invoke(ds, new Object[] {value});
				continue;
			}
			if (pt.equals(Boolean.TYPE))
			{
				// set the opposite value
				Object gbv = getMethod.invoke(ds, null);
				Boolean sbv = Boolean.FALSE.equals(gbv) ? Boolean.TRUE : Boolean.FALSE;
				setMethod.invoke(ds, new Object[] {sbv});
				continue;
			}			
			if (pt.equals(Short.TYPE))
			{
				setMethod.invoke(ds, new Object[] {new Short((short)val)});
				continue;
			}
			if (pt.equals(Long.TYPE))
			{
				setMethod.invoke(ds, new Object[] {new Long(val)});
				continue;
			}
			System.out.println("FAIL " + property + " not settable - uhpdate test!!");
		}
		
		dsAsReference = refDS.getReference();
		recreatedDS = factory.getObjectInstance(dsAsReference, null, null, null);
		System.out.println(" populated DataSource recreated using Reference as " + recreatedDS.getClass().getName());
		if (recreatedDS == ds)
			System.out.println("FAIL recreated as same instance!");
		
		compareDS(properties, ds, recreatedDS);		

		// now serialize and recreate
		 baos = new ByteArrayOutputStream();
		oos = new ObjectOutputStream(baos);	
		oos.writeObject(ds);
		oos.flush();
		oos.close();
		bais = new ByteArrayInputStream(baos.toByteArray());
		ois = new ObjectInputStream(bais);
		recreatedDS = ois.readObject();
		System.out.println(" populated DataSource recreated using serialization");
		compareDS(properties, ds, recreatedDS);
	}
	
	private static String[] getPropertyBeanList(Object ds) throws Exception
	{
		Method[] allMethods = ds.getClass().getMethods();
		
		ArrayList properties = new ArrayList();
		for (int i = 0; i < allMethods.length; i++)
		{
			Method m = allMethods[i];
			String methodName = m.getName();
			// Need at least getXX
			if (methodName.length() < 5)
				continue;
			if (!methodName.startsWith("get"))
				continue;
			if (m.getParameterTypes().length != 0)
				continue;

			Class rt = m.getReturnType();
			
			if (rt.equals(Integer.TYPE) || rt.equals(String.class) || rt.equals(Boolean.TYPE)
					|| rt.equals(Short.TYPE) || rt.equals(Long.TYPE))
			{
				// valid Java Bean property
				 String beanName = methodName.substring(3,4).toLowerCase() + methodName.substring(4);

				properties.add(beanName);
				continue;
			}
			
			if (rt.isPrimitive())
				System.out.println("FAIL " + methodName + " not supported - update test!!");

		}
		
		String[] propertyList = (String[]) properties.toArray(new String[0]);
		
		Arrays.sort(propertyList);
		
		return propertyList;
	}
	
	private static Method getGet(String property, Object ds) throws Exception
	{
		String methodName =
			"get" + property.substring(0,1).toUpperCase()
			+ property.substring(1);
		Method m = ds.getClass().getMethod(methodName, null);
		return m;
	}
	private static Method getSet(Method getMethod, Object ds) throws Exception
	{
		String methodName = "s" + getMethod.getName().substring(1);
		Method m = ds.getClass().getMethod(methodName, new Class[] {getMethod.getReturnType()});
		return m;
	}	
	private static void compareDS(String[] properties, Object ds, Object rds) throws Exception
	{
		System.out.println(" Start compare recreated");
		for (int i = 0; i < properties.length; i++)
		{
			Method getMethod = getGet(properties[i], ds);
			
			Object dsValue = getMethod.invoke(ds, null);
			Object rdsValue = getMethod.invoke(rds, null);
			
			if (dsValue == null)
			{
				if (rdsValue != null)
				{
				    System.out.println("  FAIL: " + properties[i] + " originally null, recreated as " + rdsValue);
				    continue;
				}
			}
			else
			{
				if (!dsValue.equals(rdsValue)) {
					System.out.println("  FAIL: " + properties[i] + " originally " + dsValue + ", recreated as " + rdsValue);
					continue;
				}
				
				
			}
			if (dsValue != null)
				System.out.println("  " + properties[i] + "=" + dsValue);
		
		}
		System.out.println(" Completed compare recreated");

	}

}

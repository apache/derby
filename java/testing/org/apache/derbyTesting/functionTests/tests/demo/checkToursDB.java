/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.demo.checkToursDB

   Copyright 1999, 2006 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.demo;

import org.apache.derby.tools.ij;
import toursdb.insertMaps;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;

public class checkToursDB { 

	public static void main(String args[]) {

		String[] dbfiles = {"ToursDB_schema.sql","loadTables.sql"};
		try {	
			System.setProperty("ij.database","jdbc:derby:toursDB;create=true");
			for (int i = 0 ; i < dbfiles.length ; i++)
			{
				String[] ijArgs = {dbfiles[i]};
				ij.main(ijArgs);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		PreparedStatement ps = null;
		ResultSet rs = null;
		Connection connCS = null;

		// now populate the map table
		try {
			insertMaps.main(args);
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// first get connection...
		try {
			ij.getPropertyArg(args);
			connCS = ij.startJBMS();	
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// now ensure we can select from all the tables
		doSelect(connCS);

		// we've inserted, selected, now update a row in each table
		try {
			ps = connCS.prepareStatement 
				("select ECONOMY_SEATS from AIRLINES where AIRLINE = 'AA'");
			rs = ps.executeQuery();
			if (rs.next())
				System.out.print("ECONOMY_SEATS is first: " + rs.getInt(1));
			Statement stmt  = connCS.createStatement();
			stmt.execute("update AIRLINES set ECONOMY_SEATS=108 where AIRLINE = 'AA'");
			rs = ps.executeQuery();
			if (rs.next())
				System.out.println(", ECONOMY_SEATS is then: " + rs.getString(1));
		
			ps = connCS.prepareStatement ("select COUNTRY from COUNTRIES where COUNTRY_ISO_CODE = 'US'" );
			rs = ps.executeQuery();
			if (rs.next())
				System.out.print("COUNTRY is first: " + rs.getString(1));
			stmt  = connCS.createStatement();
			stmt.execute("update COUNTRIES set COUNTRY='United States of America' where COUNTRY_ISO_CODE = 'US'");
			rs = ps.executeQuery();
			if (rs.next())
				System.out.println(", COUNTRY is then: " + rs.getString(1));
		
			ps = connCS.prepareStatement ("select COUNTRY from CITIES where CITY_ID = 52" );
			rs = ps.executeQuery();
			if (rs.next())
				System.out.print("COUNTRY is first: " + rs.getString(1));
			stmt  = connCS.createStatement();
			stmt.execute("update CITIES set COUNTRY='United States of America' where COUNTRY='United States'");
			rs = ps.executeQuery();
			if (rs.next())
				System.out.println(", COUNTRY is then: " + rs.getString(1));
		
			ps = connCS.prepareStatement ("select ECONOMY_SEATS_TAKEN from FLIGHTAVAILABILITY where FLIGHT_ID = 'AA1134' and FLIGHT_DATE='2004-03-31'" );
			rs = ps.executeQuery();
			if (rs.next())
				System.out.print("ECONOMY_SEATS_TAKEN is first: " + rs.getInt(1));
			stmt  = connCS.createStatement();
			stmt.execute("update FLIGHTAVAILABILITY set ECONOMY_SEATS_TAKEN=20 where FLIGHT_ID = 'AA1134' and FLIGHT_DATE='2004-03-31'");
			rs = ps.executeQuery();
			if (rs.next())
				System.out.println(", ECONOMY_SEATS_TAKEN is then: " + rs.getString(1));
		
			ps = connCS.prepareStatement ("select AIRCRAFT from FLIGHTS where FLIGHT_ID = 'AA1183'" );
			rs = ps.executeQuery();
			if (rs.next())
				System.out.print("AIRCRAFT is first: " + rs.getString(1));
			stmt  = connCS.createStatement();
			stmt.execute("update FLIGHTS set AIRCRAFT='B777' where FLIGHT_ID = 'AA1134'");
			rs = ps.executeQuery();
			if (rs.next())
				System.out.println(", AIRCRAFT is then: " + rs.getString(1));
		
			ps = connCS.prepareStatement ("select REGION from MAPS where MAP_NAME = 'BART'" );
			rs = ps.executeQuery();
			if (rs.next())
				System.out.print("REGION is first: " + rs.getString(1));
			stmt  = connCS.createStatement();
			stmt.execute("update MAPS set REGION='San Francisco Bay Area' where MAP_NAME = 'BART'");
			rs = ps.executeQuery();
			if (rs.next())
				System.out.println(", REGION is then: " + rs.getString(1));
	
			// Flight_history is now has 1 row, because of TRIG1
			stmt = connCS.createStatement();
			ps = connCS.prepareStatement ("select STATUS from FLIGHTS_HISTORY where FLIGHT_ID = 'AA1134'"  );
			rs = ps.executeQuery();
			if (rs.next())
				System.out.print("STATUS is first: " + rs.getString(1));
			stmt  = connCS.createStatement();
			stmt.execute("update FLIGHTS_HISTORY set STATUS='over' where FLIGHT_ID='AA1134'");
			rs = ps.executeQuery();
			if (rs.next())
				System.out.println(", STATUS is then: " + rs.getString(1));

		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}	

		// now delete....
		try {
			ps = null;

			String tableName[] = {"AIRLINES","CITIES","COUNTRIES","FLIGHTAVAILABILITY","FLIGHTS","MAPS"};
			for (int i = 0 ; i < 6; i++) {
				Statement stmt = connCS.createStatement();
				stmt.execute("delete from " + tableName[i]);
				System.out.println("deleted all from table " + tableName[i]);
			}
			// now quickly checking FLIGHTS_HISTORY - 
			// should now have a 2nd row because of trigger2
			Statement stmt  = connCS.createStatement();
			rs = stmt.executeQuery("select STATUS from FLIGHTS_HISTORY where FLIGHT_ID IS NULL and STATUS <> 'over'");
			// don't care if there are more than 1 rows...
			if (rs.next())
				System.out.println("STATUS is here: " + rs.getString(1));
			// now delete this one too
			stmt.execute("delete from FLIGHTS_HISTORY");
			System.out.println("deleted all from table FLIGHTS_HISTORY");
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}	
	
		//select again
		doSelect(connCS);

	}

	private static void doSelect(Connection connCS)
	{
		// now ensure we can select from all the tables
		try {
			PreparedStatement ps = null;

			String tableName[] = {"AIRLINES","COUNTRIES","CITIES","FLIGHTAVAILABILITY","FLIGHTS","MAPS","FLIGHTS_HISTORY"};
			for (int i = 0 ; i < 7; i++) {
				ps = connCS.prepareStatement ("select count(*) from " + tableName[i]);
				System.out.print("count for select * from table " + tableName[i]);
				ResultSet rs = ps.executeQuery();
				if (rs.next())
					System.out.println(": " + rs.getInt(1));
				else System.out.println(": 0");
			}
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}	
	}

}

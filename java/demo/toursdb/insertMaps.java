/*

   Derby - Class SimpleApp

   Copyright 2001, 2006 The Apache Software Foundation or its licensors, as applicable.

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

package toursdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.SQLException;


public class insertMaps {

	public static final String CSdriver = new String("org.apache.derby.jdbc.EmbeddedDriver");
	public static final String dbURLCS = new String("jdbc:derby:toursDB");

	public static void main(String[] args) throws Exception {

		try {
			Connection connCS = null;

			System.out.println("Loading the Cloudscape jdbc driver...");
			Class.forName(CSdriver).newInstance();
	
			System.out.println("Getting Cloudscape database connection...");
			connCS = DriverManager.getConnection(dbURLCS);
			System.out.println("Successfully got the Cloudscape database connection...");

			PreparedStatement ps = null;

			ps = connCS.prepareStatement
			("insert into maps (map_name, region, area, photo_format, picture) values (?,?,?,?,?)");
	
			ps.setString(1,"BART");
			ps.setString(2,"Bay Area");
			ps.setBigDecimal(3, new BigDecimal("1776.11"));
			ps.setString(4,"gif");
			File file = new File ("BART.gif");
			InputStream fileIn = new FileInputStream(file);
			ps.setBinaryStream(5, fileIn, (int)file.length());
			int numrows = ps.executeUpdate();

			ps.setString(1,"Caltrain");
			ps.setString(2,"West Bay");
			ps.setBigDecimal(3, new BigDecimal("1166.77"));
			ps.setString(4,"gif");
			file = new File ("Caltrain.gif");
			fileIn = new FileInputStream(file);
			ps.setBinaryStream(5, fileIn, (int)file.length());
			numrows = numrows + ps.executeUpdate();

			ps.setString(1,"Light Rail");
			ps.setString(2,"Santa Clara Valley");
			ps.setBigDecimal(3, new BigDecimal("9117.90"));
			ps.setString(4,"gif");
			file = new File ("BART.gif");
			fileIn = new FileInputStream(file);
			ps.setBinaryStream(5, fileIn, (int)file.length());
			numrows = numrows + ps.executeUpdate();

			System.out.println("Inserted " + numrows + " rows into the ToursDB");

			ps.close();
	
			connCS.close();

		} catch (SQLException e) {
			System.out.println ("FAIL -- unexpected exception: " + e.toString());
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println ("FAIL -- unexpected exception: " + e.toString());
			e.printStackTrace();
		}

	}

}

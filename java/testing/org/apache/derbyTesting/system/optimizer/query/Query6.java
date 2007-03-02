/*
 
 Derby - Class org.apache.derbyTesting.system.langtest.query.Query6
 
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
package org.apache.derbyTesting.system.optimizer.query;

/*
 * Class Query6: Returns a list of queries that Selects from multiple
 * views with combination of nested views and aggregate views
 */

public class Query6 extends GenericQuery {

	public Query6() {
		description = "Select from multiple views with combination of nested views and aggregate views";
		generateQueries();
	}

	/**
	 * @param args
	 */
	public void generateQueries() {
		queries
				.add("select col1, max_view_8bc1 from sum_view_8a right join max_view_8b on col1=max_view_8bc1 where col1 <100 union all select col1, min_view_8bc1 from sum_view_8a right join min_view_8b on col1=min_view_8bc1 where col1 <100");
		queries
				.add("select col1, max_view_8bc1 from sum_view_8a  join max_view_8b on col1=max_view_8bc1 where col1 <100 union all select col1, min_view_8bc1 from sum_view_8a  join min_view_8b on col1=min_view_8bc1 where col1 <100");
		queries
				.add("select col1, max_view_8bc1 from sum_view_8a  inner join max_view_8b on col1=max_view_8bc1 where col1 <100 union all select col1, min_view_8bc1 from sum_view_8a  inner join min_view_8b on col1=min_view_8bc1 where col1 <100");
		queries
				.add("select col1, max_view_8bc1 from sum_view_8a right join max_view_8b on col1=max_view_8bc1 where col1 <100 union all select col1, min_view_8bc1 from sum_view_8a right join min_view_8b on col1=min_view_8bc1 where col1 <100");
		queries
				.add("select col1, max_view_8bc1 from sum_view_8a inner join max_view_8b on col1=max_view_8bc1 where col1 <100 union all select col1, min_view_8bc1 from sum_view_8a inner join min_view_8b on col1=min_view_8bc1 where col1 <100");
		queries
				.add("select col1, max_view_8bc1 from sum_view_8a join max_view_8b on col1=max_view_8bc1 where col1 <100 union all select col1, min_view_8bc1 from sum_view_8a join min_view_8b on col1=min_view_8bc1 where col1 <100");
		queries
				.add("select col1, max_view_8bc1 from avg_view_8a  join max_view_8b on col1=1 where (col1 >0 and col1 <10) union all select col1, min_view_8bc1 from avg_view_8a  join min_view_8b on col1=2 where (col1 >0 and col1 <10)");
		queries
				.add("select col1, max_view_8bc1 from avg_view_8a  join max_view_8b on col1=101 where (col1 >100 and col1 <200) union all select col1, min_view_8bc1 from avg_view_8a  join min_view_8b on col1=min_view_8bc1 where (col1 >100 and col1 <200)");
		queries
				.add("select col1, count_view_8bc1 from avg_view_8a  join count_view_8b on col1=23 where col1 <100 union all select col1, min_view_8bc1 from avg_view_8a  join min_view_8b on col1=20 where col1 <100");
		queries
				.add("select col1, count_view_8bc1 from max_view_8a  join count_view_8b on col1=145 where (col1 >50 and col1<300) union all select col1, max_view_8bc1 from avg_view_8a  join max_view_8b on col1=123 where (col1 >50 and col1<510)");
		queries
				.add("select col1,sum_view_8bc1 from avg_view_8a  join sum_view_8b on col1<10 where col1 is not null union all select col1, count_view_8bc1 from avg_view_8a  join count_view_8b on col1<10 where col1 is null");
		queries
				.add("select sum_view_8bc1,avg_view_8bc1,max_view_8bc1 from sum_view_8b,avg_view_8b,max_view_8b where (sum_view_8bc1>avg_view_8bc1 and avg_view_8bc1<max_view_8bc1) union all select count_view_8bc1,min_view_8bc1,max_view_8bc1 from count_view_8b,min_view_8b,max_view_8b where (count_view_8bc1=min_view_8bc1=max_view_8bc1)");
		queries
				.add("select sum_view_8bc1,avg_view_8bc1,max_view_8bc1 from sum_view_8b,avg_view_8b,max_view_8b where (sum_view_8bc1>avg_view_8bc1 and avg_view_8bc1< max_view_8bc1) union select min_view_8bc1,avg_view_8bc1,max_view_8bc1 from min_view_8b,avg_view_8b,max_view_8b where (min_view_8bc1>avg_view_8bc1 and avg_view_8bc1<max_view_8bc1)");
		queries
				.add("select sum_view_8bc1,avg_view_8bc1,max_view_8bc1 from sum_view_8b,avg_view_8b,max_view_8b where (sum_view_8bc1=40200 and avg_view_8bc1<1255 and max_view_8bc1=400) union all select count_view_8bc1,min_view_8bc1,max_view_8bc1 from count_view_8b,min_view_8b,max_view_8b where (count_view_8bc1=40200 and min_view_8bc1<1255 and max_view_8bc1=400)");
		queries
				.add("select sum_view_8bc1,avg_view_8bc1,min_view_8bc1 from sum_view_8b,avg_view_8b,min_view_8b where (min_view_8bc1=(select col8 from mytable8 where col8=2)) union all select count_view_8bc1,min_view_8bc1,max_view_8bc1 from count_view_8b,min_view_8b,max_view_8b where (count_view_8bc1=200 and (min_view_8bc1=2) or (max_view_8bc1=1245))");
		queries
				.add("select v_level7c2, v_level7c4, sum_view_8bc1 from v_level7  join sum_view_8b on sum_view_8bc1 is not null union all select v_level7c2, v_level7c3 , sum_view_8bc1 from v_level7  join sum_view_8b on v_level7c1=sum_view_8bc1 where ((v_level7c1>2308 and v_level7c1<2310) and v_level7c4 is not null and sum_view_8bc1 is not null)");
		queries
				.add("select v_level8c2, v_level8c4, count_view_8bc1 from v_level8  join count_view_8b on count_view_8bc1=v_level8c1 where ((v_level8c1>108 and v_level8c1<201) and v_level8c4 is not null and v_level8c7 is not null) union all select v_level8c2, v_level8c3 , avg_view_8bc1 from v_level8  join avg_view_8b on v_level8c1=avg_view_8bc1 where ((v_level8c1=201 ) and v_level8c4 = 'String value for the varchar column in Table MYTABLE2: 197' and avg_view_8bc1 =97)");
		queries
				.add("select v_level6c2, v_level6c4, min_view_8bc1 from v_level6  join min_view_8b on min_view_8bc1=v_level6c1 where (v_level6c2='String value for the varchar column in Table MYTABLE9: 2') union all select v_level5c2, v_level5c3 , min_view_8bc1 from v_level5  join min_view_8b on v_level5c1=min_view_8bc1 where ((v_level5c1>2308 and v_level5c1<2310) and v_level5c4 is not null and min_view_8bc1 is not null)");
		queries
				.add("select a1,a2 from (select v_level8c1,v_level8c7 from v_level8 union all select col1,col7 from mytable2) as A(a1,a2),mytable1 where mytable1.col1=A.a1 and mytable1.col7=A.a2");
		queries
				.add("select a1,a2 from (select v_level8c1,v_level8c7 from v_level8 where (v_level8c1 < 1000 ) union all select col1,col7 from mytable2 where (col1 < 1000 )) as A(a1,a2),mytable1 where mytable1.col1=A.a1 and mytable1.col7=A.a2");
		queries
				.add("select a1,a2 from (select v_level8c1,v_level8c7 from v_level8 where (v_level8c1 < 1000 ) union all select col1,col7 from mytable2 where (col1 < 1000 )) as A(a1,a2),mytable1 where mytable1.col1=A.a1 and mytable1.col7=A.a2 union all select a3,a4 from (select v_level8c1,v_level8c7 from v_level8 where (v_level8c1 < 1000 ) union all select col1,col7 from mytable2 where (col1 < 1000 )) as A(a3,a4),mytable1 where mytable1.col1=A.a3 and mytable1.col7=A.a4");
		queries
				.add("select a1,a2 from (select v_level8c1,v_level8c7 from v_level8 where (v_level8c1 < 1000 ) union all select col1,col7 from mytable2 where (col1 < 1000 )) as A(a1,a2),mytable1 where mytable1.col1=A.a1 and mytable1.col7=A.a2 union all select a3,a4 from (select v_level8c1,v_level8c7 from v_level8 where (v_level8c1 < 1000 ) union all select col1,col7 from mytable2 where (col1 < 1000 )) as A(a3,a4),mytable1 where mytable1.col1=A.a3 and mytable1.col7=A.a4 union all select a5,a6 from (select v_level8c1,v_level8c7 from v_level8 where (v_level8c1 < 100 ) union all select col1,col7 from mytable2 where (col1 < 1000 )) as A(a5,a6),mytable8 where mytable8.col1=A.a5 and mytable8.col7=A.a6 union all select a7,a8 from (select col1,col7 from mytable55 where ( col1< 1000 ) union all select col1,col7 from mytable10 where (col1 < 1000 )) as A(a7,a8),mytable1 where mytable1.col1=A.a7 and mytable1.col7=A.a8");
		queries
				.add("select a1,a2 from (select v_level8c1,v_level8c7 from v_level8 where (v_level8c1 < 1000 ) union all select col1,col7 from mytable2 where (col1 < 1000 )) as A(a1,a2),mytable1 where mytable1.col1=A.a1 and mytable1.col7=A.a2 union all select a3,a4 from (select v_level8c1,v_level8c7 from v_level8 where (v_level8c1 < 1000 ) union all select col1,col7 from mytable2 where (col1 < 1000 )) as A(a3,a4),mytable1 where mytable1.col1=A.a3 and mytable1.col7=A.a4 union all select a5,a6 from (select v_level8c1,v_level8c7 from v_level8 where (v_level8c1 < 100 ) union all select col1,col7 from mytable2 where (col1 < 1000 )) as A(a5,a6),mytable8 where mytable8.col1=A.a5 and mytable8.col7=A.a6 union all select a7,a8 from (select col1,col7 from mytable55 where ( col1< 1000 ) union all select col1,col7 from mytable10 where (col1 < 1000 )) as A(a7,a8),mytable1 where mytable1.col1=A.a7 and mytable1.col7=A.a8 union all select a5,a6 from (select v_level8c1,v_level8c7 from v_level8 where (v_level8c1 < 100 ) union all select col1,col7 from mytable2 where (col1 < 1000 )) as A(a5,a6),mytable8 where mytable8.col1=A.a5 and mytable8.col7=A.a6 union all select a7,a8 from (select col1,col7 from mytable55 where ( col1< 1000 ) union all select col1,col7 from mytable10 where (col1 < 1000 )) as A(a7,a8),mytable1 where mytable1.col1=A.a7 and mytable1.col7=A.a8");
		queries
				.add("select a1,a2 from (select v_level8c1,v_level8c7 from v_level8 where (v_level8c1 > 1000 and v_level8c1 < 1010 ) union all select col1,col7 from mytable2 where (col1 > 1000 and col1 <1010 )) as A(a1,a2),mytable1 where mytable1.col1=A.a1 and mytable1.col7=A.a2 union all select a3,a4 from (select v_level8c1,v_level8c7 from v_level8 where (v_level8c1 < 1000 ) union all select col1,col7 from mytable2 where (col1 < 1000 )) as A(a3,a4),mytable1 where mytable1.col1=A.a3 and mytable1.col7=A.a4 union all select a5,a6 from (select v_level8c1,v_level8c7 from v_level8 where (v_level8c1 < 100 ) union all select col1,col7 from mytable2 where (col1 < 1000 )) as A(a5,a6),mytable8 where mytable8.col1=A.a5 and mytable8.col7=A.a6 union all select a7,a8 from (select col1,col7 from mytable55 where ( col1< 1000 ) union all select col1,col7 from mytable10 where (col1 < 1000 )) as A(a7,a8),mytable1 where mytable1.col1=A.a7 and mytable1.col7=A.a8");

	}

}

/*

   Derby - Class org.apache.derby.iapi.reference.ClassName

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


package org.apache.derby.iapi.reference;


/**
	List of strings representing class names, which are typically found
    for classes with implement the Formatable interface.
    These strings are removed from the code to separate them from the
    strings which need to be internationalized. It also reduces footprint.
    <P>
	This class has no methods, all it contains are String's which by default
	are public, static and final since they are declared in an interface.
*/

public interface ClassName
{

	String STORE_CONGLOMDIR =
		"org.apache.derby.impl.store.access.ConglomerateDirectory";

	String STORE_PCXENA =
		"org.apache.derby.impl.store.access.PC_XenaVersion";


	String DataValueFactory = "org.apache.derby.iapi.types.DataValueFactory";
	String DataValueDescriptor = "org.apache.derby.iapi.types.DataValueDescriptor";

	String BooleanDataValue = "org.apache.derby.iapi.types.BooleanDataValue";

 	String BitDataValue = "org.apache.derby.iapi.types.BitDataValue";
	String StringDataValue = "org.apache.derby.iapi.types.StringDataValue";
	String DateTimeDataValue = "org.apache.derby.iapi.types.DateTimeDataValue";
	String NumberDataValue = "org.apache.derby.iapi.types.NumberDataValue";
	String RefDataValue = "org.apache.derby.iapi.types.RefDataValue";
	String UserDataValue = "org.apache.derby.iapi.types.UserDataValue";
	String ConcatableDataValue  = "org.apache.derby.iapi.types.ConcatableDataValue";

	String FormatableBitSet = "org.apache.derby.iapi.services.io.FormatableBitSet";

	String BaseActivation = "org.apache.derby.impl.sql.execute.BaseActivation";
	String BaseExpressionActivation = "org.apache.derby.impl.sql.execute.BaseExpressionActivation";

	String CursorActivation = "org.apache.derby.impl.sql.execute.CursorActivation";

	String Row = "org.apache.derby.iapi.sql.Row";
	String Qualifier = "org.apache.derby.iapi.store.access.Qualifier";

	String RunTimeStatistics = "org.apache.derby.iapi.sql.execute.RunTimeStatistics";

	String Storable = "org.apache.derby.iapi.services.io.Storable";
	String StandardException = "org.apache.derby.iapi.error.StandardException";

	String LanguageConnectionContext = "org.apache.derby.iapi.sql.conn.LanguageConnectionContext";
	String ConstantAction = "org.apache.derby.iapi.sql.execute.ConstantAction";
	String DataDictionary = "org.apache.derby.iapi.sql.dictionary.DataDictionary";

	String CursorResultSet = "org.apache.derby.iapi.sql.execute.CursorResultSet";

	String ExecIndexRow = "org.apache.derby.iapi.sql.execute.ExecIndexRow";

	String ExecPreparedStatement = "org.apache.derby.iapi.sql.execute.ExecPreparedStatement";

	String ExecRow = "org.apache.derby.iapi.sql.execute.ExecRow";
	String Activation = "org.apache.derby.iapi.sql.Activation";

	String ResultSet = "org.apache.derby.iapi.sql.ResultSet";

	String FileMonitor = "org.apache.derby.impl.services.monitor.FileMonitor";

	String GeneratedClass = "org.apache.derby.iapi.services.loader.GeneratedClass";
	String GeneratedMethod = "org.apache.derby.iapi.services.loader.GeneratedMethod";
	String GeneratedByteCode = "org.apache.derby.iapi.services.loader.GeneratedByteCode";

	String Context = "org.apache.derby.iapi.services.context.Context";

	String NoPutResultSet = "org.apache.derby.iapi.sql.execute.NoPutResultSet";

	String ResultSetFactory = "org.apache.derby.iapi.sql.execute.ResultSetFactory";
	String RowFactory = "org.apache.derby.iapi.sql.execute.RowFactory";

	String RowLocation = "org.apache.derby.iapi.types.RowLocation";

	String VariableSizeDataValue = "org.apache.derby.iapi.types.VariableSizeDataValue";
	String ParameterValueSet = "org.apache.derby.iapi.sql.ParameterValueSet";


	String CurrentDatetime = "org.apache.derby.impl.sql.execute.CurrentDatetime";

	String MaxMinAggregator = "org.apache.derby.impl.sql.execute.MaxMinAggregator";
	String SumAggregator = "org.apache.derby.impl.sql.execute.SumAggregator";
	String CountAggregator = "org.apache.derby.impl.sql.execute.CountAggregator";
	String AvgAggregator = "org.apache.derby.impl.sql.execute.AvgAggregator";

	String ExecutionFactory = "org.apache.derby.iapi.sql.execute.ExecutionFactory";
	String LanguageFactory ="org.apache.derby.iapi.sql.LanguageFactory";
	String ParameterValueSetFactory ="org.apache.derby.iapi.sql.ParameterValueSetFactory";

	String TriggerNewTransitionRows = "org.apache.derby.catalog.TriggerNewTransitionRows";
	String TriggerOldTransitionRows = "org.apache.derby.catalog.TriggerOldTransitionRows";
	String VTICosting = "org.apache.derby.vti.VTICosting";

	String Authorizer = "org.apache.derby.iapi.sql.conn.Authorizer";
}

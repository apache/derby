/*

   Derby - Class org.apache.derby.iapi.services.io.RegisteredFormatIds

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

package org.apache.derby.iapi.services.io;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.info.JVMInfo;

/**
        Registration of TypedFormat classes.

        <P>
        A TypedFormat is registered by placing a class name at the
        correct place in the correct array, driven by the base format number:
        <UL>
        <LI>2 byte - MIN_TWO_BYTE_FORMAT_ID - TwoByte
        </UL>
        The offset from the base format number (0 based) gives the offset in the array.
        <P>
        The class name is either:
        <UL>
        <LI> The actual class name of the TypeFormat.
        <LI> The name of a class that extends org.apache.derby.iapi.services.io.FormatableInstanceGetter.
             In this case the monitor will register an instance of the class after calling its
                 setFormatId() method with format id it is registered as.
        </UL>
*/

public interface RegisteredFormatIds {

/* one byte  format identifiers never used
String[] OneByte = {
};
*/

String[] TwoByte = {
        /* 0 */         null, // null marker
        /* 1 */         null, // String marker
        /* 2 */         null, // Serializable marker
        /* 3 */         null,
        /* 4 */         "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 5 */         "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 6 */         "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 7 */         "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 8 */         "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 9 */         "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 10 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 11 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 12 */        null,
        /* 13 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 14 */        "org.apache.derby.catalog.types.TypeDescriptorImpl",
        /* 15 */        "org.apache.derby.impl.store.access.PC_XenaVersion",
        /* 16 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 17 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 18 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 19 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 20 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 21 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 22 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 23 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 24 */        null,
        /* 25 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 26 */        null,
        /* 27 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 28 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 29 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 30 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 31 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 32 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 33 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 34 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 35 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 36 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 37 */        "org.apache.derby.impl.sql.execute.DeleteConstantAction",
        /* 38 */        "org.apache.derby.impl.sql.execute.InsertConstantAction",
        /* 39 */        "org.apache.derby.impl.sql.execute.UpdateConstantAction",
        /* 40 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 41 */        null,
        /* 42 */        null,
        /* 43 */        null,
        /* 44 */        null,
        /* 45 */        null,
        /* 46 */        null,
        /* 47 */        null,
        /* 48 */        null,
        /* 49 */        null,
        /* 50 */        null,
        /* 51 */        null,
        /* 52 */        null,
        /* 53 */        null,
        /* 54 */        null,
        /* 55 */        null,
        /* 56 */        null,
        /* 57 */        null,
        /* 58 */        null,
        /* 59 */        null,
        /* 60 */        null,
        /* 61 */        null,
        /* 62 */        null,
        /* 63 */        null,
        /* 64 */        null,
        /* 65 */        null,
        /* 66 */        null,
        /* 67 */        null,
        /* 68 */        null,
        /* 69 */        null,
        /* 70 */        null,
        /* 71 */        null,
        /* 72 */        null,
        /* 73 */        null, 
        /* 74 */        null,
        /* 75 */        null,
        /* 76 */        null,
        /* 77 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 78 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 79 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 80 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 81 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 82 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 83 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 84 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 85 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 86 */        null,
        /* 87 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 88 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 89 */        "org.apache.derby.iapi.types.SQLLongint",
        /* 90 */        "org.apache.derby.impl.store.access.heap.HeapClassInfo",
        /* 91 */        "org.apache.derby.impl.store.access.heap.Heap",
        /* 92 */        null,
        /* 93 */        "org.apache.derby.impl.store.access.StorableFormatId",
        /* 94 */        null,
        /* 95 */        "org.apache.derby.impl.store.access.btree.index.B2IUndo",
        /* 96 */        null,
        /* 97 */        "org.apache.derby.impl.store.raw.data.ChainAllocPageOperation",
        /* 98 */        null,
        /* 99 */        null,
        /* 100 */       null,
        /* 101 */       "org.apache.derby.impl.store.raw.data.DeleteOperation",
        /* 102 */       "org.apache.derby.impl.store.raw.xact.EndXact",
        /* 103 */       "org.apache.derby.impl.store.raw.data.InsertOperation",
        /* 104 */       "org.apache.derby.impl.store.raw.data.LogicalUndoOperation",
        /* 105 */       "org.apache.derby.impl.store.raw.data.PhysicalUndoOperation",
        /* 106 */       "org.apache.derby.impl.store.raw.data.PurgeOperation",
        /* 107 */       "org.apache.derby.impl.store.raw.data.ContainerUndoOperation",
        /* 108 */       "org.apache.derby.impl.store.raw.data.UpdateOperation",
        /* 109 */       "org.apache.derby.impl.store.raw.data.UpdateFieldOperation",
        /* 110 */       null,
        /* 111 */       "org.apache.derby.impl.store.raw.data.AllocPageOperation",
        /* 112 */       null,
        /* 113 */       "org.apache.derby.impl.store.raw.data.InvalidatePageOperation",
        /* 114 */       "org.apache.derby.impl.store.raw.log.SaveLWMOperation",
        /* 115 */       null, 
        /* 116 */       null,
        /* 117 */       "org.apache.derby.impl.store.raw.data.StoredPage",
        /* 118 */       "org.apache.derby.impl.store.raw.data.AllocPage",
        /* 119 */       null,
        /* 120 */       null,
        /* 121 */       null,
        /* 122 */       null,
        /* 123 */       null,
        /* 124 */       null,
        /* 125 */       null,
        /* 126 */       null,
        /* 127 */       null,
        /* 128 */       null,
        /* 129 */       "org.apache.derby.impl.store.raw.log.LogRecord",
        /* 130 */       "org.apache.derby.impl.store.raw.log.LogCounter",
        /* 131 */       "org.apache.derby.impl.services.uuid.BasicUUIDGetter",           // InstanceGetter
        /* 132 */       null,
        /* 133 */       "org.apache.derby.impl.store.access.btree.LeafControlRow",
        /* 134 */       "org.apache.derby.impl.store.access.btree.BranchControlRow",
        /* 135 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 136 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 137 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 138 */       null,
        /* 139 */       null,
        /* 140 */       null,
        /* 141 */       null,
        /* 142 */       null,
        /* 143 */       null,
        /* 144 */       null,
        /* 145 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 146 */       null,
        /* 147 */       "org.apache.derby.impl.store.raw.xact.XactId",
        /* 148 */       null,
        /* 149 */       "org.apache.derby.impl.sql.execute.AvgAggregator",
        /* 150 */       null,
        /* 151 */       "org.apache.derby.impl.sql.execute.CountAggregator",
        /* 152 */       "org.apache.derby.impl.sql.execute.MaxMinAggregator",
        /* 153 */       null,
        /* 154 */       "org.apache.derby.impl.sql.execute.SumAggregator",
        /* 155 */       null,
        /* 156 */       null, 
        /* 157 */       null, 
        /* 158 */       null, 
        /* 159 */       null, 
        /* 160 */       null, 
        /* 161 */       null, 
        /* 162 */       null, 
        /* 163 */       null, 
        /* 164 */       null, 
        /* 165 */       null, 
        /* 166 */       null, 
        /* 167 */       null, 
        /* 168 */       null, 
        /* 169 */       "org.apache.derby.impl.store.raw.xact.BeginXact",
        /* 170 */       null, 
        /* 171 */       null, 
        /* 172 */       null,
        /* 173 */       "org.apache.derby.impl.sql.execute.rts.RunTimeStatisticsImpl",
        /* 174 */       null,
        /* 175 */       null,
        /* 176 */       null,
        /* 177 */       "org.apache.derby.impl.sql.execute.rts.RealProjectRestrictStatistics",
        /* 178 */       "org.apache.derby.impl.sql.execute.rts.RealSortStatistics",
        /* 179 */       "org.apache.derby.impl.sql.execute.rts.RealTableScanStatistics",
        /* 180 */       "org.apache.derby.impl.sql.execute.rts.RealNestedLoopJoinStatistics",
        /* 181 */       "org.apache.derby.impl.sql.execute.rts.RealIndexRowToBaseRowStatistics",
        /* 182 */       "org.apache.derby.impl.sql.execute.rts.RealAnyResultSetStatistics",
        /* 183 */       "org.apache.derby.impl.sql.execute.rts.RealOnceResultSetStatistics",
        /* 184 */       "org.apache.derby.impl.sql.execute.rts.RealCurrentOfStatistics",
        /* 185 */       "org.apache.derby.impl.sql.execute.rts.RealRowResultSetStatistics",
        /* 186 */       "org.apache.derby.impl.sql.execute.rts.RealUnionResultSetStatistics",
        /* 187 */       "org.apache.derby.impl.sql.execute.rts.RealNestedLoopLeftOuterJoinStatistics",
        /* 188 */       "org.apache.derby.impl.sql.execute.rts.RealNormalizeResultSetStatistics",
        /* 189 */       "org.apache.derby.impl.sql.execute.rts.RealInsertResultSetStatistics",
        /* 190 */       "org.apache.derby.impl.sql.execute.rts.RealUpdateResultSetStatistics",
        /* 191 */       "org.apache.derby.impl.sql.execute.rts.RealDeleteResultSetStatistics",
        /* 192 */       null,
        /* 193 */       null,
        /* 194 */       null,
        /* 195 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 196 */       "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 197 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 198 */       "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 199 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter,
        /* 200 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter,
        /* 201 */       null,
        /* 202 */       "org.apache.derby.iapi.types.UserType",
        /* 203 */       "org.apache.derby.impl.sql.execute.rts.RealHashScanStatistics",
        /* 204 */       null,
        /* 205 */       "org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl",
        /* 206 */       null,
        /* 207 */       null,
        /* 208 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 209 */       null,
        /* 210 */       "org.apache.derby.impl.store.raw.data.CopyRowsOperation",
        /* 211 */       null,
        /* 212 */       null,
        /* 213 */       "org.apache.derby.impl.sql.execute.ReplaceJarConstantAction",
        /* 214 */       "org.apache.derby.impl.sql.execute.rts.RealVTIStatistics",
        /* 215 */       null,
        /* 216 */       null,
        /* 217 */       null,
        /* 218 */       "org.apache.derby.impl.sql.execute.IndexColumnOrder",
        /* 219 */       "org.apache.derby.iapi.util.ByteArray",
        /* 220 */       null,
        /* 221 */       null,
        /* 222 */       null,
        /* 223 */       "org.apache.derby.impl.sql.execute.AggregatorInfo",
        /* 224 */       "org.apache.derby.impl.sql.execute.AggregatorInfoList",
        /* 225 */       "org.apache.derby.impl.sql.GenericStorablePreparedStatement",
        /* 226 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 227 */       null,
        /* 228 */       "org.apache.derby.impl.sql.GenericResultDescription",
        /* 229 */       null,
        /* 230 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 231 */       "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 232 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 233 */       "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 234 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter,
        /* 235 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter,
        /* 236 */       null,
        /* 237 */       "org.apache.derby.impl.sql.execute.ValueRow",
        /* 238 */       "org.apache.derby.impl.sql.execute.IndexRow",
        /* 239 */       "org.apache.derby.iapi.sql.dictionary.RowList",
        /* 240 */       null,
        /* 241 */       "org.apache.derby.impl.store.raw.data.InitPageOperation",
        /* 242 */       "org.apache.derby.impl.store.raw.data.ContainerOperation",
        /* 243 */       "org.apache.derby.iapi.sql.depend.DependableList",
        /* 244 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 245 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 246 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 247 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 248 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 249 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 250 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 251 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 252 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 253 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 254 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 255 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 256 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 257 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 258 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 259 */       "org.apache.derby.iapi.types.DataTypeDescriptor",
        /* 260 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 261 */       "org.apache.derby.impl.store.raw.xact.TransactionTableEntry",
        /* 262 */       "org.apache.derby.impl.store.raw.xact.TransactionTable",
        /* 263 */       "org.apache.derby.impl.store.raw.log.CheckpointOperation",
        /* 264 */       "org.apache.derby.catalog.types.UserDefinedTypeIdImpl",
        /* 265 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 266 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 267 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 268 */       "org.apache.derby.iapi.sql.dictionary.IndexRowGenerator",
        /* 269 */       "org.apache.derby.iapi.services.io.FormatableBitSet",
        /* 270 */       "org.apache.derby.iapi.services.io.FormatableArrayHolder",
        /* 271 */       "org.apache.derby.iapi.services.io.FormatableProperties",
        /* 272 */       null,
        /* 273 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 274 */       null,
        /* 275 */       null,
        /* 276 */       null,
        /* 277 */       null,
        /* 278 */       "org.apache.derby.impl.sql.execute.ConstraintInfo",
        /* 279 */       null,
        /* 280 */       null,
        /* 281 */       null,
        /* 282 */       "org.apache.derby.impl.sql.execute.FKInfo",
        /* 283 */       "org.apache.derby.impl.sql.execute.rts.RealScalarAggregateStatistics",
        /* 284 */       "org.apache.derby.impl.sql.execute.rts.RealDistinctScalarAggregateStatistics",
        /* 285 */       "org.apache.derby.impl.sql.execute.rts.RealGroupedAggregateStatistics",
        /* 286 */       null,
        /* 287 */       "org.apache.derby.impl.store.raw.data.SetReservedSpaceOperation",
        /* 288 */    null,
        /* 289 */       null,
        /* 290 */       null,
        /* 291 */       "org.apache.derby.impl.store.raw.data.RemoveFileOperation",
        /* 292 */       null,
        /* 293 */       null,
        /* 294 */       null,
        /* 295 */       null,
        /* 296 */       "org.apache.derby.impl.sql.CursorTableReference",
        /* 297 */       "org.apache.derby.impl.sql.CursorInfo",
        /* 298 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 299 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 300 */       null,
        /* 301 */       null,
        /* 302 */       null,
        /* 303 */       "org.apache.derby.iapi.services.io.FormatableIntHolder",
        /* 304 */       "org.apache.derby.impl.sql.execute.rts.RealHashJoinStatistics",
        /* 305 */       "org.apache.derby.impl.sql.execute.rts.RealHashLeftOuterJoinStatistics",
        /* 306 */       "org.apache.derby.impl.sql.execute.rts.RealHashTableStatistics",
        /* 307 */       "org.apache.derby.iapi.types.JSQLType",
        /* 308 */       "org.apache.derby.impl.sql.execute.rts.RealMaterializedResultSetStatistics",
        /* 309 */       null,
        /* 310 */       null,
        /* 311 */       null,
        /* 312 */       "org.apache.derby.catalog.types.MethodAliasInfo",
        /* 313 */       "org.apache.derby.iapi.services.io.FormatableHashtable",
        /* 314 */       null,
        /* 315 */       null,
        /* 316 */       "org.apache.derby.iapi.sql.dictionary.TriggerDescriptor",
        /* 317 */       "org.apache.derby.impl.sql.execute.TriggerInfo",
        /* 318 */       null,
        /* 319 */       null,
        /* 320 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 321 */       null,
        /* 322 */       null,
        /* 323 */       null,
        /* 324 */       null,
        /* 325 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 326 */       "org.apache.derby.catalog.types.DefaultInfoImpl",
        /* 327 */       "org.apache.derby.impl.sql.execute.rts.RealLastIndexKeyScanStatistics",
        /* 328 */       "org.apache.derby.impl.store.raw.xact.GlobalXactId",
        /* 329 */       "org.apache.derby.iapi.services.io.FormatableLongHolder",
        /* 330 */       "org.apache.derby.impl.sql.execute.rts.RealScrollInsensitiveResultSetStatistics",
        /* 331 */       null,
        /* 332 */       null,
        /* 333 */       null,
        /* 334 */       "org.apache.derby.impl.sql.execute.rts.RealDistinctScanStatistics",
        /* 335 */       "org.apache.derby.impl.sql.execute.AlterSPSConstantAction",
        /* 336 */       null,
        /* 337 */       null,
        /* 338 */       null,
        /* 339 */       null,
        /* 340 */       null,
        /* 341 */       null,
        /* 342 */       null,
        /* 343 */       null,
        /* 344 */       null,
        /* 345 */       null,
        /* 346 */       null,
        /* 347 */       null,
        /* 348 */       null,
        /* 349 */       null,
        /* 350 */       null,
        /* 351 */       null,
        /* 352 */       null,
        /* 353 */       null,
        /* 354 */       null,
        /* 355 */       null,
        /* 356 */       null,
        /* 357 */       null,
        /* 358 */       "org.apache.derby.impl.sql.execute.ColumnInfo",
        /* 359 */       "org.apache.derby.impl.sql.depend.DepClassInfo",
        /* 360 */       "org.apache.derby.impl.store.access.btree.index.B2IStaticCompiledInfo",
        /* 361 */       null, // SQLData marker
        /* 362 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 363 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter,
        /* 364 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter,
        /* 365 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter,
        /* 366 */       "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 367 */       "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 368 */       "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 369 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 370 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 371 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 372 */       null,
        /* 373 */       null,
        /* 374 */       null,
        /* 375 */       "org.apache.derby.impl.sql.execute.UpdatableVTIConstantAction",
        /* 376 */       null,
        /* 377 */       null,
        /* 378 */       null,
        /* 379 */       "org.apache.derby.impl.sql.execute.rts.RealInsertVTIResultSetStatistics",
        /* 380 */       "org.apache.derby.impl.sql.execute.rts.RealDeleteVTIResultSetStatistics",
        /* 381 */       null, // Unused,
        /* 382 */       null, // Unused
        /* 383 */   "org.apache.derby.impl.sql.GenericColumnDescriptor",
        /* 384 */   null, // Unused,
        /* 385 */   null,
        /* 386 */   null,
        /* 387 */       "org.apache.derby.catalog.types.IndexDescriptorImpl",
        /* 388 */       "org.apache.derby.impl.store.access.btree.index.B2I",
        /* 389 */   null,
        /* 390 */   null,
        /* 391 */   null,
        /* 392 */   null,
        /* 393 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 394 */   null,
        /* 395 */   null,
        /* 396 */       null, // Unused
        /* 397 */   "org.apache.derby.catalog.types.StatisticsImpl",
        /* 398 */       null,
        /* 399 */       null,
        /* 400 */   null,
        /* 401 */       "org.apache.derby.impl.sql.catalog.DD_Version",
        /* 402 */       "org.apache.derby.impl.sql.catalog.DD_Version",
        /* 403 + 0 */           null,
        /* 1 */         null,
        /* 2 */         null,
        /* 3 */         null,
        /* 4 */         null,
        /* 5 */         null,
        /* 6 */         null,
        /* 7 */         null,
        /* 8 */         null,
        /* 9 */         null,
        /* 10 */        null,
        /* 11 */        null,
        /* 12 */        null,
        /* 13 */        null,
        /* 14 */        null,
        /* 15 */        null,
        /* 16 */        null,
        /* 17 */        null,
        /* 18 */        null,
        /* 19 */        null,
        /* 20 */        null,
        /* 21 */        null,
        /* 22 */        null,
        /* 23 */        null,
        /* 24 */        null,
        /* 25 */        null,
        /* 26 */        null,
        /* 27 */        null,
        /* 28 */        null,
        /* 29 */        null,
        /* 30 */        null,
        /* 31 */        null,
        /* 32 */        null,
        /* 33 */        null,
        /* 403 + 34 */  null,
    /* 438 */   null,
        /* 439 */   "org.apache.derby.impl.sql.execute.rts.RealDeleteCascadeResultSetStatistics",    

    /// --- BLOB is copying LONGVARBIT in implementation
        /* 440 */   "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter, BLOB_TYPE_ID
        /* 441 */   "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter, BLOB_COMPILATION_TYPE_ID
        /* 442 */   "org.apache.derby.catalog.types.TypesImplInstanceGetter", // BLOB_TYPE_ID_IMPL
        /* 443 */   "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter, SQL_BLOB_ID

    /// --- CLOB is copying LONGVARCHAR in implementation
        /* 444 */   "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 445 */   "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 446 */   "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 447 */   "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter,
           

    /// --- NLOB is copying NATIONAL LONGVARCHAR in implementation
        
        /* 448 */   "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 449 */       "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 450 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter,

 
        /* 451 */   "org.apache.derby.catalog.types.RoutineAliasInfo"


};
}

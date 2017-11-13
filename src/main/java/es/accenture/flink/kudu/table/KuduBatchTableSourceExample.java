/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package es.accenture.flink.kudu.table;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connectors.kudu.KuduBatchTableSource;
import org.apache.flink.connectors.kudu.KuduInputFormat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.*;

import java.util.concurrent.TimeUnit;


public class KuduBatchTableSourceExample {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        KuduInputFormat.Conf conf = KuduInputFormat.Conf
                .builder()
                .masterAddress("quickstart.cloudera")
                .tableName("impala::default.sfmta")
                .build();

        KuduBatchTableSource kuduTableSource = new KuduBatchTableSource(conf, new TupleTypeInfo<>(
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.FLOAT_TYPE_INFO,
                BasicTypeInfo.FLOAT_TYPE_INFO,
                BasicTypeInfo.FLOAT_TYPE_INFO,
                BasicTypeInfo.FLOAT_TYPE_INFO
        ));


        tEnv.registerTableSource("Taxis", kuduTableSource);

        Table result = tEnv
                .sql("SELECT f1, AVG(f4), COUNT(*) from Taxis group by f1")
                .as("vehicle,avgSpeed,totalMeasures");


        tEnv.toDataSet(result, new TupleTypeInfo<Tuple3>(
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.FLOAT_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO
        ))
                .output(new PrintingOutputFormat<Tuple3>());

        env.execute();
    }
}

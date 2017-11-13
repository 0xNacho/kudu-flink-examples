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

package es.accenture.flink.kudu;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connectors.kudu.KuduInputFormat;
import org.apache.flink.connectors.kudu.KuduOutputFormat;

import java.util.Date;

import static org.apache.flink.connectors.kudu.KuduOutputFormat.Conf.WriteMode.UPSERT;

public class KuduOutputFormatTest {

    public static void main(String[] args) throws Exception {
        KuduInputFormat.Conf inputConfig = KuduInputFormat.Conf
                .builder()
                .masterAddress("quickstart.cloudera")
                .tableName("impala::default.sfmta")
                .build();

        KuduOutputFormat.Conf outputConfig = KuduOutputFormat.Conf
                .builder()
                .masterAddress("quickstart.cloudera")
                .tableName("impala::default.sfmta2")
                .writeMode(UPSERT)
                .build();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(7);
        env.createInput(new KuduInputFormat<>(inputConfig), new TupleTypeInfo<Tuple>(
                        BasicTypeInfo.LONG_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.FLOAT_TYPE_INFO,
                        BasicTypeInfo.FLOAT_TYPE_INFO,
                        BasicTypeInfo.FLOAT_TYPE_INFO,
                        BasicTypeInfo.FLOAT_TYPE_INFO
                )
        )

                .output(new KuduOutputFormat(outputConfig));

        env.execute();
    }
}

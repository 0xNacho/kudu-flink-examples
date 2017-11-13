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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.kudu.KuduInputFormat;
import org.apache.flink.connectors.kudu.internal.Predicate;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class FilterVsPredicateExample {

    public static void main(String[] args) throws Exception {

        ParameterTool parameters = ParameterTool.fromArgs(args);

        KuduInputFormat.Conf inputConfig = KuduInputFormat.Conf
                .builder()
                .masterAddress("quickstart.cloudera")
                .tableName("impala::default.sfmta")
                .addPredicate(new Predicate.PredicateBuilder("vehicle_tag").isEqualTo().val(6288))
                .build();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.createInput(new KuduInputFormat<>(inputConfig), new TupleTypeInfo<>(
                        BasicTypeInfo.LONG_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.FLOAT_TYPE_INFO,
                        BasicTypeInfo.FLOAT_TYPE_INFO,
                        BasicTypeInfo.FLOAT_TYPE_INFO,
                        BasicTypeInfo.FLOAT_TYPE_INFO
                )
        ).output(new OutputFormat<Tuple>() {
            @Override
            public void configure(Configuration parameters) {

            }

            @Override
            public void open(int taskNumber, int numTasks) throws IOException {

            }

            @Override
            public void writeRecord(Tuple record) throws IOException {

            }

            @Override
            public void close() throws IOException {

            }
        });


        JobExecutionResult result1 = env.execute("PredicateJob");


        System.out.println("Job with predicates time: " + result1.getNetRuntime(TimeUnit.MILLISECONDS));

        // ----------------------------------------------------------------------------------------
        // ----------------------------------------------------------------------------------------
        // ----------------------------------------------------------------------------------------

        ExecutionEnvironment env2 = ExecutionEnvironment.getExecutionEnvironment();

        env2.createInput(new KuduInputFormat<>(inputConfig), new TupleTypeInfo<>(
                        BasicTypeInfo.LONG_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.FLOAT_TYPE_INFO,
                        BasicTypeInfo.FLOAT_TYPE_INFO,
                        BasicTypeInfo.FLOAT_TYPE_INFO,
                        BasicTypeInfo.FLOAT_TYPE_INFO
                )
        )
        .output(new OutputFormat<Tuple>() {
            @Override
            public void configure(Configuration parameters) {

            }

            @Override
            public void open(int taskNumber, int numTasks) throws IOException {

            }

            @Override
            public void writeRecord(Tuple record) throws IOException {

            }

            @Override
            public void close() throws IOException {

            }
        });

        JobExecutionResult result2 = env2.execute("FilteringJob");

        System.out.println("Job with filters time: " + result2.getNetRuntime(TimeUnit.MILLISECONDS));


    }
}

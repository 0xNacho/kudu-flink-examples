package es.accenture.flink.kudu.table;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connectors.kudu.KuduBatchTableSource;
import org.apache.flink.connectors.kudu.KuduInputFormat;
import org.apache.flink.connectors.kudu.KuduOutputFormat;
import org.apache.flink.connectors.kudu.KuduTableSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class KuduTableSinkExample {


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

        KuduOutputFormat.Conf outputConf = KuduOutputFormat.Conf
                .builder()
                .masterAddress("quickstart.cloudera")
                .tableName("impala::default.speed_aggregation_sfmta")
                .build();

        result.writeToSink(new KuduTableSink<>(outputConf, new TupleTypeInfo<>(
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO
        )));

        env.execute();
    }
}

package org.shivram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;

import java.io.IOException;


public class OrcReader {
    private final static String CONF_PATH = "/Users/shivram/work/singlecluster-PHD/hadoop/etc/hadoop";
    private static Configuration conf;

    static {
        conf = new Configuration();
        conf.addResource(new Path(CONF_PATH + "core-site.xml"));
        conf.addResource(new Path(CONF_PATH + "hdfs-site.xml"));
    }

    public static long testWithoutFilter(Path path) throws IOException {
        long startTime = System.nanoTime();
        OrcFile.ReaderOptions options = OrcFile.readerOptions(conf);
        Reader reader = OrcFile.createReader(path, options);
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        RecordReader rows = reader.rows(new Reader.Options());
        int count = 0;
        while (rows.nextBatch(batch)) {
            for (int r = 0; r < batch.size; ++r) {
                count = count+1;
            }
        }

        rows.close();
        return System.nanoTime() - startTime;
    }

    public static long testWithFilter(Path path) throws IOException {
        long startTime = System.nanoTime();
        OrcFile.ReaderOptions options = OrcFile.readerOptions(conf);
        Reader reader = OrcFile.createReader(path, options);
        SearchArgument sarg = SearchArgumentFactory.newBuilder().between("year", PredicateLeaf.Type.STRING, new String("1941"), new String("1942")).build();
        RecordReader rows = reader.rows(new Reader.Options().searchArgument(sarg, new String[]{null, "year", "name"}));
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        int count = 0;
        while (rows.nextBatch(batch)) {
            for (int r = 0; r < batch.size; ++r) {
                count = count+1;
            }
        }
        rows.close();
        return System.nanoTime() - startTime;
    }

    public static long testWithFilterProjectColumn(Path path) throws IOException {
        long startTime = System.nanoTime();
        OrcFile.ReaderOptions options = OrcFile.readerOptions(conf);
        Reader reader = OrcFile.createReader(path, options);
        SearchArgument sarg = SearchArgumentFactory.newBuilder().between("year", PredicateLeaf.Type.STRING, new String("1941"), new String("1942")).build();

        TypeDescription schema = reader.getSchema();
        boolean [] include = new boolean[schema.getMaximumId() + 1]; // + 1 because col ids start at 1
        TypeDescription col0 = schema.getChildren().get(0);
        TypeDescription col1 = schema.getChildren().get(1);
        include[col0.getId()] = true;
        include[col1.getId()] = true;


        RecordReader rows = reader.rows(new Reader.Options().searchArgument(sarg, new String[]{null, "year", "name"}).include(include));
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        int count = 0;
        while (rows.nextBatch(batch)) {
            for (int r = 0; r < batch.size; ++r) {
                count = count+1;
            }
        }
        rows.close();
        return System.nanoTime() - startTime;
    }

    public static long testWithoutFilterProjectColumn(Path path) throws IOException {
        long startTime = System.nanoTime();
        OrcFile.ReaderOptions options = OrcFile.readerOptions(conf);
        Reader reader = OrcFile.createReader(path, options);

        TypeDescription schema = reader.getSchema();
        boolean [] include = new boolean[schema.getMaximumId() + 1]; // + 1 because col ids start at 1
        TypeDescription col0 = schema.getChildren().get(0);
        TypeDescription col1 = schema.getChildren().get(1);
        include[col0.getId()] = true;
        include[col1.getId()] = true;

        RecordReader rows = reader.rows(new Reader.Options().include(include));
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        int count = 0;
        while (rows.nextBatch(batch)) {
            for (int r = 0; r < batch.size; ++r) {
                count = count+1;
            }
        }
        rows.close();
        return System.nanoTime() - startTime;
    }


    public static void main(String []args) throws IOException {

        Path path = new Path("file:////Users/shivram/work/000000_0");
        System.out.println(testWithFilter(path));
        System.out.println(testWithoutFilter(path));
        System.out.println(testWithFilterProjectColumn(path));
        System.out.println(testWithoutFilterProjectColumn(path));

    }
}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.Integer;
import java.util.*;
import java.util.ArrayList;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "League Count");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LeagueCountMap.class);
        jobA.setReducerClass(LeagueCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(PopularityLeague.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Leagues");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(IntArrayWritable.class);

        jobB.setMapperClass(TopLeagueMap.class);
        jobB.setReducerClass(TopLeagueReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(PopularityLeague.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }

    public static class LeagueCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        List<String> league;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {

            Configuration conf = context.getConfiguration();
            String leaguePath = conf.get("league");
            this.league = Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"));
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer lineTokenizer = new StringTokenizer(line, ":");
            String srcPage = lineTokenizer.nextToken();
            Integer srcPg = Integer.parseInt(srcPage);
//            context.write(new IntWritable(srcPg), new IntWritable(0));

            String destPages = lineTokenizer.nextToken();
            StringTokenizer tokenizer = new StringTokenizer(destPages, " ");
            while (tokenizer.hasMoreTokens()){
                String landingPage = tokenizer.nextToken();
                Integer landingPg = Integer.parseInt(landingPage);
                context.write(new IntWritable(landingPg), new IntWritable(1));
            }
        }
    }

    public static class LeagueCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class TopLeagueMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        private TreeSet<Pair<Integer, Integer>> countTopLeagueMap = new TreeSet<Pair<Integer, Integer>>();
        List<String> league;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String leaguePath = conf.get("league");
            this.league = Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"));
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Integer count = Integer.parseInt(value.toString());
            Integer word = Integer.parseInt(key.toString());
            if (this.league.contains(word)) {
                countTopLeagueMap.add(new Pair<Integer, Integer>(count, word));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Pair<Integer, Integer> item : countTopLeagueMap) {
                Integer[] strings = {item.second, item.first};
                IntArrayWritable val = new IntArrayWritable(strings);
                context.write(NullWritable.get(), val);
            }

        }
    }

    public static class TopLeagueReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
            private TreeSet<Pair<Integer, Integer>> countTopLeagueMap = new TreeSet<Pair<Integer, Integer>>();

            @Override
            protected void setup(Context context) throws IOException,InterruptedException {
                Configuration conf = context.getConfiguration();
            }

            @Override
            public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
                for (IntArrayWritable val: values) {
                    IntWritable[] pair= (IntWritable[]) val.toArray();
                    Integer word = pair[0].get();
                    Integer count = pair[1].get();
                    countTopLeagueMap.add(new Pair<Integer, Integer>(count, word));
                }

    //        Integer init = lsize-1;
            Integer repeatCount = 0;
            Integer lsize = countTopLeagueMap.size();
            Integer previousWordCount= -1;
            Iterator iterator = countTopLeagueMap.descendingIterator();
//                Iterator iterator = countTopLeagueMap.iterator();
             int i = lsize-1;
             while (iterator.hasNext()){
                Pair<Integer, Integer> item = (Pair<Integer, Integer>)iterator.next();
                Integer word = item.second;
                Integer value = item.first;
                    if(value.intValue() == previousWordCount) {
                        repeatCount++;
                    }else {
                        repeatCount=0;
                    }
                   Integer rankCnt = (i + repeatCount);
                   context.write(new IntWritable(word), new IntWritable(rankCnt));
//                 context.write( new IntWritable(rankCnt), new IntWritable(word));
                   previousWordCount = value;
                     i--;
                  }
                }
            }

        public static class IntArrayWritable extends ArrayWritable {
            public IntArrayWritable() {
                super(IntWritable.class);
            }

            public IntArrayWritable(Integer[] numbers) {
                super(IntWritable.class);
                IntWritable[] ints = new IntWritable[numbers.length];
                for (int i = 0; i < numbers.length; i++) {
                    ints[i] = new IntWritable(numbers[i]);
                }
                set(ints);
            }
        }
}


class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
package com.datapipeline;

import com.datapipeline.model.ApacheLogEvent;
import com.datapipeline.model.PageViewCount;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;
import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

public class HotPages {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        URL resource = HotPages.class.getResource("/apache.log");
        DataStream<String> inputStream = senv.readTextFile(resource.toString());
        DataStream<ApacheLogEvent> mapStream = inputStream.map(new MapFunction<String, ApacheLogEvent>() {
            @Override
            public ApacheLogEvent map(String line) throws Exception {
                String[] fields = line.split(" ");
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
                return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.minutes(1)) {
            @Override
            public long extractTimestamp(ApacheLogEvent apacheLogEvent) {
                return apacheLogEvent.getTimestamp();
            }
        });

        DataStream<PageViewCount> windAggreStream = mapStream.filter(line->"GET".equals(line.getMethod())).
                keyBy(ApacheLogEvent::getUrl).
                timeWindow(Time.of(10, TimeUnit.MINUTES),Time.of(5,TimeUnit.SECONDS)).
                aggregate(new HotPageAggregate(),new HotPageWindowFunction());

        DataStream<String> topN  = windAggreStream.keyBy(PageViewCount::getWindowEnd).process(new HotPageTopN(3));
//
        topN.print();
        senv.execute("HotPages");
    }

    public static class HotPageAggregate implements AggregateFunction<ApacheLogEvent,Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
            return aLong+1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }

    public static class HotPageWindowFunction implements WindowFunction<Long,PageViewCount,String, TimeWindow>{

        @Override
        public void apply(String url, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
                collector.collect(new PageViewCount(url,timeWindow.getEnd(),iterable.iterator().next()));
        }
    }

    private static class HotPageTopN extends KeyedProcessFunction<Long,PageViewCount, String> {
            int topN;
        public HotPageTopN(int topN) {
            this.topN = topN;
        }

        private transient ListState<PageViewCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState =   getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("listState", TypeInformation.of(PageViewCount.class)));
        }



        @Override
        public void processElement(PageViewCount pageViewCount, KeyedProcessFunction<Long, PageViewCount, String>.Context context, Collector<String> collector) throws Exception {
            listState.add(pageViewCount);
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
          ArrayList<PageViewCount> pageViewCounts = Lists.newArrayList(listState.get());
            pageViewCounts.sort(new Comparator<PageViewCount>() {
                @Override
                public int compare(PageViewCount o1, PageViewCount o2) {
                    if(o1.getCount()>o2.getCount()){
                        return -1;
                    }else if(o1.getCount()<o2.getCount()){
                        return 1;
                    }else{
                        return 0;
                    }
                }
            });


            // 格式化成String输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===================================\n");
            resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表，取top n输出
            for (int i = 0; i < Math.min(topN, pageViewCounts.size()); i++) {
                PageViewCount currentItemViewCount = pageViewCounts.get(i);
                resultBuilder.append("NO ").append(i + 1).append(":")
                        .append(" 页面URL = ").append(currentItemViewCount.getUrl())
                        .append(" 浏览量 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            resultBuilder.append("===============================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());

        }
    }
}

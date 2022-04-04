package com.dinglicom;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dinglicom.model.ItemViewCount;
import com.dinglicom.model.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.*;

/**
 * @author ly
 * @Date Create in 10:29 2021/4/15 0015
 * @Description
 * 通过控制台传参的方式，将路径传入
 * 形式： --application.properties /Users/sll/develop/workspace_idea/flink/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/application.properties
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        String path = tool.getRequired("application.properties");
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(path);
        // 参数获取
        // checkpoint参数
        String checkpointDirectory = parameterTool.getRequired("checkpointDirectory");
        long checkpointSecondInterval = parameterTool.getLong("checkpointSecondInterval");

        //Kafka参数（consumerKafka）
        String fromKafkaBootstrapServers = parameterTool.getRequired("bootstrap.servers");
        String fromKafkaGroupID = parameterTool.getRequired("group.id");
        String fromKafkaAutoOffsetReset= parameterTool.getRequired("auto.offset.reset");


        // 1，创建执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        senv.getConfig().setAutoWatermarkInterval(200) ;    //设置水印生成频率，默认200毫秒
        //设置StateBackend     true：表示开启异步快照
        senv.setStateBackend((StateBackend)new FsStateBackend(checkpointDirectory, true));

        //设置Checkpoint
        CheckpointConfig checkpointConfig = senv.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(checkpointSecondInterval*10);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        /*
         *  ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：
         *     取消作业时保留检查点。请注意，在这种情况下，您必须在取消后手动清理检查点状态。
         *  ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：
         *     取消作业时删除检查点。只有在作业失败时，检查点状态才可用。
         */
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        checkpointConfig.setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        checkpointConfig.setMaxConcurrentCheckpoints(2);



        // 2,读取数据，创建DataStream流
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,fromKafkaBootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,fromKafkaGroupID);
        // 这个参数貌似没有起作用？？？
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty("auto.offset.reset", "earliest");

        String topic = "hotitems";  //hotitems

//        FlinkKafkaConsumer flinkKafkaConsumer = new FlinkKafkaConsumer(topic, new CustomerDeserializationSchema(), properties);

        FlinkKafkaConsumer flinkKafkaConsumer = new FlinkKafkaConsumer(topic, new SimpleStringSchema(), properties);


//        flinkKafkaConsumer.setStartFromLatest();

        // 从头开始消费
        flinkKafkaConsumer.setStartFromEarliest();

        // 从指定offset开始消费
//        Map<KafkaTopicPartition, Long> offset = new HashMap<>();
//        offset.put(new KafkaTopicPartition("hotitems",0),6807384L);
//        flinkKafkaConsumer.setStartFromSpecificOffsets(offset);


        DataStream<String> inputStream = senv.addSource(flinkKafkaConsumer);

//        inputStream.print();

//        // 3.转换为Pojo，分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] split = line.split(",");
            return new UserBehavior(new Long(split[0]), new Long(split[1]), new Integer(split[2]), split[3], new Long(split[4]));
                        // AscendingTimestampExtractor 这个时间提取器适用于数据已经排序的
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000;
            }
        });
//        dataStream.print();

//        // 4,分组开窗聚合，得到每个窗口内各个商品的count值
        DataStream<ItemViewCount> windowStream = dataStream.filter(data ->
                "pv".equals(data.getBehavior())) // 过滤pv行为
                .keyBy("itemId")     // 根据商品ID进行分组
                .timeWindow(Time.hours(1), Time.minutes(5))  //开滑动窗口
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());
//        windowStream.print();

//        // 4,收集同一个窗口内的所有商品的count数据，排序输出top n
        DataStream<String> windowEnd = windowStream.keyBy("windowEnd")
                .process(new TopNHotItems(5));// 用自定义处理函数排序取前5

        windowEnd.print();

        senv.execute("hot items analysis");
    }

    // 自定义增量聚合函数
    private static class ItemCountAgg implements AggregateFunction<UserBehavior,Integer,Long>{

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(UserBehavior userBehavior, Integer accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Integer accumulator) {
            return accumulator+0L;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            System.out.println("a+b->"+a+b);
            return a+b;  // 这一步其实用不到
        }
    }

    // 自定义全窗口函数
    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            Long itemId = tuple.getField(0);
            Long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }


    // 自定义全窗口函数
    private static class WindowItemCountResult1 extends ProcessWindowFunction<Long,ItemViewCount, Tuple, TimeWindow>{

        @Override
        public void process(Tuple tuple, Context context, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            collector.collect(new ItemViewCount(tuple.getField(0),context.window().getEnd(),iterable.iterator().next()));
        }
    }

    private static class TopNHotItems extends KeyedProcessFunction<Tuple,ItemViewCount,String>{
        private int topN;

        public TopNHotItems(int topN) {
            this.topN = topN;
        }

        ListState<ItemViewCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list",ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            // 每来一条数据，存入list状态中，并注册定时器
            listState.add(itemViewCount);
            // 这里需要注意，我们注册定时器时虽然是针对来的每一条数据，但是很多数据的WindowEnd是一样的，所以相同的窗口结束时间只会有一个定时器存在
            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd()+1);
        }

        // 定时器触发时需要干的事情
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，当前已收集到所有数据，排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(listState.get().iterator());
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue()-o1.getCount().intValue();  // 使用intValue方法的前提条件是Long类型的最大值不能超过Integer.MAX_VALUE，否则会出问题
                }
            });
            // 将排名信息格式化成String，方便打印输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===================================\n");
            resultBuilder.append("窗口结束时间：").append( new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表，取top n输出
            for( int i = 0; i < Math.min(topN, itemViewCounts.size()); i++ ){   // 这里注意Math.min函數的使用
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append("NO ").append(i+1).append(":")
                        .append(" 商品ID = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            resultBuilder.append("===============================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }

    private static class MyKafkaDeserializationSchema implements DeserializationSchema<String>, SerializationSchema<String>
    {

        @Override
        public String deserialize(byte[] bytes) throws IOException {
            String value = new String(bytes,Charset.forName("UTF-8"));
            return value;
        }

        @Override
        public boolean isEndOfStream(String s) {
            return false;
        }


        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }

        @Override
        public byte[] serialize(String s) {
            return new byte[0];
        }
    }

    private static class CustomerDeserializationSchema implements KafkaDeserializationSchema<String>{

        @Override
        public boolean isEndOfStream(String s) {
            return false;
        }

        @Override
        public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
            String value = new String(consumerRecord.value(), Charset.forName("UTF-8"));
//            int partition = consumerRecord.partition();
            long offset = consumerRecord.offset();
            System.out.println("offset: "+offset);
//            JSONObject jsonObject = JSON.parseObject(value);
//            System.out.println("jsonObject: "+jsonObject);
//            jsonObject.put("partition",partition);
//            jsonObject.put("offset",offset);
//            System.out.println("value: "+value);
            return value+" : "+offset;
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

}

package com.flink.project;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import scala.Tuple4;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

public class PVAndUVExample {

    public static void main(String[] args) {

        // 初始化kafka的topic,node等信息

        // 获取命令行参数
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String kafkaTopic = parameterTool.get("kafka-topic", "test");

        String brokers = parameterTool.get("brokers", "localhost:9092");
        System.out.printf("Reading from kafka topic %s @ %s", kafkaTopic, brokers);
        System.out.println();

        Properties properties = new Properties();


        FlinkKafkaConsumer010<UserBean> kafka = new FlinkKafkaConsumer010<UserBean>(kafkaTopic, new UserKafkaDeserializationSchema<UserBean>(), properties);
        kafka.setStartFromEarliest();
        kafka.setCommitOffsetsOnCheckpoints(false);
        // 获取StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从kafka获取数据源
        DataStreamSource<UserBean> dataStream = env.addSource(kafka);

        // 设置watermark
        // 设置water生成器和时间戳转换器
        dataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                        .withTimestampAssigner((ctx) -> new TimeStampExtractor()))
                // 设置滑动窗口
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                .allowedLateness(Time.minutes(5))
                .process(new ProcessAllWindowFunction<UserBean, Tuple4<Long, Long, Long, Integer>, TimeWindow>() {

                    @Override
                    public void process(Context context, Iterable<UserBean> iterable, Collector<Tuple4<Long, Long, Long, Integer>> collector) throws Exception {

                        // 点击数量即userBean的个数
                        Long pv = 0l;
                        // 每次访问生成一个userBean，所以需要用set去重
                        Set<Integer> userIds = new HashSet<>();
                        Iterator<UserBean> iterator = iterable.iterator();
                        while(iterator.hasNext()){

                            UserBean next = iterator.next();
                            pv++;
                            userIds.add(next.getUserId());
                        }
                        TimeWindow window = context.window();
                        collector.collect(new Tuple4<>(window.getStart(), window.getEnd(), pv, userIds.size()));
                    }
                });



        // 设置窗口大小
        // 设置窗口延迟时间
        // 设置具体执行逻辑
        // 设置sink位置
    }

    private static class PeriodicWatermarkGenerator implements WatermarkGenerator<UserBean>, Serializable{

        private long currentWatermark = Long.MIN_VALUE;

        @Override
        public void onEvent(UserBean userBean, long eventTimestamp, WatermarkOutput watermarkOutput) {

            this.currentWatermark = eventTimestamp;
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            long effectiveWatermark = currentWatermark == Long.MIN_VALUE ? Long.MIN_VALUE : currentWatermark - 1;
        }
    }

    private static class TimeStampExtractor implements TimestampAssigner<UserBean> {
        @Override
        public long extractTimestamp(UserBean element, long recordTimestamp) {
            return element.getTs();
        }
    }
}

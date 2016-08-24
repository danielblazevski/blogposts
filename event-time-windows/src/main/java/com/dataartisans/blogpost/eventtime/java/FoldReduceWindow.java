/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.blogpost.eventtime.java;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import sun.management.Sensor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Main class of the sample application.
 * This class constructs and runs the data stream program.
 */
public class FoldReduceWindow {

    /**
     * Main entry point.
     */
    public static void main(String[] args) throws Exception {

        // create environment and configure it
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.registerType(Statistic.class);
        env.registerType(SensorReading.class);

        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // create a stream of sensor readings, assign timestamps, and create watermarks
        DataStream<SensorReading> readings = env
                .addSource(new SampleDataGenerator())
                .assignTimestamps(new ReadingsTimestampAssigner());

        // Example of Fold + Window function
        readings
                .keyBy(reading -> reading.sensorId() )
                .timeWindow(Time.minutes(1), Time.seconds(10))
                .apply(new Tuple3<String, Long, Integer>("",0L, 0), new MyFoldFunction(),
                        new MyWindowFunction())
                .print();

        env.execute("Event time example");

        // Example of Reduce + Window function
        readings
                .keyBy(reading -> reading.sensorId() )
                .timeWindow(Time.minutes(1), Time.seconds(10))
                .apply(new MyReduceFunction(), new MyOtherWindowFunction())
                .print();

    }

    private static class MyFoldFunction implements FoldFunction<SensorReading,
            Tuple3<String, Long, Integer> > {

        public Tuple3<String, Long, Integer> fold(Tuple3<String, Long, Integer> acc, SensorReading s) {
            Integer cur = acc.getField(2);
            acc.setField(2, cur + 1);
            return acc;
        }
    }

    private static class MyWindowFunction implements WindowFunction<Tuple3<String, Long, Integer>,
            Tuple3<String, Long, Integer>, String, TimeWindow> {
        public void apply(String s,
                          TimeWindow window,
                          Iterable<Tuple3<String, Long, Integer>> counts,
                          Collector<Tuple3<String, Long, Integer>> out) {
            Integer count = counts.iterator().next().getField(2);
            out.collect(new Tuple3<String, Long, Integer>(s, window.getEnd(),count));
        }
    }

    private static class MyReduceFunction implements ReduceFunction<SensorReading> {

        public SensorReading reduce(SensorReading reading1, SensorReading reading2) {

            return reading1.reading() > reading2.reading() ? reading2 : reading1;
        }
    }

    private static class MyOtherWindowFunction implements WindowFunction<SensorReading,
            Tuple2<Long, SensorReading>, String, TimeWindow> {
        public void apply(String s,
                          TimeWindow window,
                          Iterable<SensorReading> minReadings,
                          Collector<Tuple2<Long, SensorReading>> out) {
            SensorReading min = minReadings.iterator().next();
            out.collect(new Tuple2<Long, SensorReading>(window.getStart(),min));
        }
    }

    private static class ReadingsTimestampAssigner implements TimestampExtractor<SensorReading> {

        private static final long MAX_DELAY = 12000;

        private long maxTimestamp;

        @Override
        public long extractTimestamp(SensorReading element, long currentTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.timestamp());
            return element.timestamp();
        }

        @Override
        public long extractWatermark(SensorReading element, long currentTimestamp) {
            return Long.MIN_VALUE;
        }

        @Override
        public long getCurrentWatermark() {
            return maxTimestamp - MAX_DELAY;
        }
    }
}

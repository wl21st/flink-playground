/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package galiglobal.flink.eventTime;

import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class CustomStrategyJob {

    private static final Logger LOG = LoggerFactory.getLogger(CustomStrategyJob.class);

    private SourceFunction<SensorData> source;
    private SinkFunction<SensorData> sink;

    public CustomStrategyJob(SourceFunction<SensorData> source, SinkFunction<SensorData> sink) {
        this.source = source;
        this.sink = sink;
    }

    public static void main(String[] args) throws Exception {
        CustomStrategyJob job = new CustomStrategyJob(new RandomSensorSource(), new PrintSinkFunction<>());
        job.execute();
    }

    public void execute() throws Exception {

        Properties props = new Properties();
        props.put("metrics.reporter.jmx.factory.class", "org.apache.flink.metrics.jmx.JMXReporterFactory");
        Configuration conf = ConfigurationUtils.createConfiguration(props);
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // https://issues.apache.org/jira/browse/FLINK-19317
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        LOG.debug("Start Flink example job");

        DataStream<SensorData> sensorStream =
                env.addSource(source)
                        .returns(TypeInformation.of(SensorData.class));


        var sensorEventTimeStream =
                sensorStream
                        .assignTimestampsAndWatermarks(
                                new WatermarkStrategy<SensorData>() {
                                    @Override
                                    public WatermarkGenerator<SensorData> createWatermarkGenerator(
                                            WatermarkGeneratorSupplier.Context context) {
                                        return new BoundedOutOfOrdernessWatermarks<>(
                                                Duration.ofMillis(0)
                                        ) {
                                            @Override
                                            public void onEvent(
                                                    SensorData event,
                                                    long eventTimestamp,
                                                    WatermarkOutput output) {
                                                super.onEvent(event, eventTimestamp, output);
                                                super.onPeriodicEmit(output);
                                            }
                                        };
                                    }
                                }
                                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                        );

        sensorEventTimeStream
                .transform("debugFilter", sensorEventTimeStream.getType(), new StreamWatermarkDebugFilter<>())
                .keyBy((event) -> event.getId())
                .process(new TimeoutFunction())
                .addSink(sink);

        LOG.debug("Stop Flink example job");
        env.execute();
    }

}
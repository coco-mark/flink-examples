package coco.demo.questions;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * 本示例用于展示使用 checkpoint 前后在容灾时对于数据计算结果的影响。
 * <p>
 * 通过添加无值的参数 --useCheckpoint 来控制是否使用 checkpoint。
 *
 * @author coco
 */
public class Question1_useCheckpointOrNot {
    public static final File OFFSET_FILE = new File(Question1_useCheckpointOrNot.class.getResource("").getFile(), "offsets.txt");

    public static void main(String[] args) throws Exception {
        FileUtils.deleteFileOrDirectory(OFFSET_FILE);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));
        env.setParallelism(1);

        ParameterTool params = ParameterTool.fromArgs(args);
        String useCp = params.get("useCheckpoint");

        if (useCp != null) {
            // 使用 checkpoint 进行恢复
            env.enableCheckpointing(500);
            env.addSource(
                    // 使用 element emit source 模拟 kafka source
                    new ElementEmitSource<>(1L, 2L, 3L, 4L, 5L, 6L, 7L)).returns(Long.class)
                    .keyBy((KeySelector<Long, Long>) value -> value % 2)
                    .process(new KeyedProcessFunction<Long, Long, Long>() {
                        transient ValueState<Long> sum;

                        @Override
                        public void open(Configuration parameters) throws Exception {
                            ValueStateDescriptor<Long> desc = new ValueStateDescriptor<Long>("sum", BasicTypeInfo.LONG_TYPE_INFO);
                            sum = getRuntimeContext().getState(desc);
                        }

                        @Override
                        public void processElement(Long value, Context context, Collector<Long> collector) throws Exception {
                            Long state = sum.value();
                            if (state == null) {
                                state = value;
                            } else {
                                state += value;
                            }

                            sum.update(state);

                            if (context.getCurrentKey() == 0) {
                                System.out.println(">> result: sum even " + sum.value());
                            } else {
                                System.out.println(">> result: sum odd " + sum.value());
                            }
                        }
                    });
        } else {
            // 不使用 checkpoint
            env.addSource(
                    // 使用 element emit source 模拟 kafka source
                    new ElementEmitSource<>(1L, 2L, 3L, 4L, 5L, 6L, 7L)).returns(Long.class)
                    .keyBy((KeySelector<Long, Long>) value -> value % 2)
                    .process(new KeyedProcessFunction<Long, Long, Long>() {
                        Map<Long, Long> sumMap = new HashMap<>();

                        @Override
                        public void processElement(Long value, Context context, Collector<Long> collector) throws Exception {
                            long sum = sumMap.getOrDefault(context.getCurrentKey(), 0L);
                            sum += value;
                            if (context.getCurrentKey() == 0) {
                                System.out.println(">> result: sum even " + sum);
                            } else {
                                System.out.println(">> result: sum odd " + sum);
                            }
                            sumMap.put(context.getCurrentKey(), sum);
                        }
                    });
        }

        // execute program
        env.execute("Flink: Question1_useCheckpointOrNot");
    }
}

package coco.demo.questions;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Iterator;

import static coco.demo.questions.Question1_useCheckpointOrNot.OFFSET_FILE;

/**
 * @author coco
 */
public class ElementEmitSource<T> implements SourceFunction<T>, CheckpointedFunction {

    private final T[] elements;

    private transient ListState<Long> offsetsState;

    private Long currOffset = null;
    private boolean hasBreak;


    public ElementEmitSource(T... elements) {
        this.elements = elements;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {

        for (int i = Math.toIntExact(currOffset); i < elements.length; i++) {
            ctx.collect(elements[i]);
            currOffset = (long) (i + 1);
            System.out.println(">> source: emit " + elements[i]);
            Thread.sleep(1000);

            if (!hasBreak && currOffset == 4) {
                FileUtils.write(OFFSET_FILE, currOffset + " break", "UTF-8");
                System.err.println(">> source: break! currOffset: " + currOffset);
                throw new RuntimeException("break!");
            } else {
                FileUtils.write(OFFSET_FILE, String.valueOf(currOffset), "UTF-8");
            }
        }
    }

    @Override
    public void cancel() {

    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        offsetsState.clear();
        offsetsState.add(currOffset);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Long> descriptor =
                new ListStateDescriptor<>(
                        "offsetsState",
                        Long.class);

        offsetsState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            Iterator<Long> it = offsetsState.get().iterator();
            if (it.hasNext()) {
                this.currOffset = it.next();
                System.out.println(">> source(with-checkpoint): recover from checkpoint, currOffset: " + this.currOffset);
            }
        } else {
            // restore from offset file
            try {
                String offsets = FileUtils.readFileToString(OFFSET_FILE, "UTF-8");
                if (offsets.contains("break")) {
                    this.hasBreak = true;
                    currOffset = Long.valueOf(offsets.split(" ")[0]);
                } else {
                    currOffset = Long.valueOf(offsets);
                }
                System.out.println(">> source(no-checkpoint): recover from local file, currOffset: " + this.currOffset);
            } catch (Exception e) {
                currOffset = 0L;
            }
        }
    }
}

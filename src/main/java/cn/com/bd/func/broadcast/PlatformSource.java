package cn.com.bd.func.broadcast;

import cn.com.bd.pojo.Platform;
import cn.com.bluemoon.bd.utils.ThreadUtils;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;

public class PlatformSource implements SourceFunction<Platform> {
    private volatile boolean isRun;
    private List<Platform> platforms = Lists.newArrayList();
    private int count;

    {
        platforms.add(new Platform("京东", "B2C"));
        platforms.add(new Platform("天猫", "B2C"));
        platforms.add(new Platform("维品会", "F2C"));
        platforms.add(new Platform("网易考拉", "F2C"));
        platforms.add(new Platform("拼多多", "F2C"));
        platforms.add(new Platform("云集", "社交电商"));
        platforms.add(new Platform("贝店", "社交电商"));
    }


    @Override
    public void run(SourceContext<Platform> context) throws Exception {
        this.isRun = true;
        ThreadUtils.sleepInMillSeconds(20000);

        while (this.isRun) {
            int index = count % this.platforms.size();
            context.collect(platforms.get(index));
            count++;
            ThreadUtils.sleepInMillSeconds(10000);
        }
    }

    @Override
    public void cancel() {
        this.isRun = false;
    }
}

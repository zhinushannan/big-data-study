package club.kwcoder.covid;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CovidCombiner extends Reducer<CovidWritable, NullWritable, CovidWritable, NullWritable> {

    private final CovidWritable.Builder outKeyBuild = new CovidWritable.Builder();

    @Override
    protected void reduce(CovidWritable key, Iterable<NullWritable> values, Reducer<CovidWritable, NullWritable, CovidWritable, NullWritable>.Context context) throws IOException, InterruptedException {
        String state = "", county = "";
        Integer cases = 0, deaths = 0;

        // 累加确诊数、死亡数
        for (NullWritable ignored : values) {
            if (state.equals("")) {
                state = key.getState();
                county = key.getCounty();
            }
            cases += key.getCases();
            deaths += key.getDeaths();
        }

        // 构建对象并输出
        CovidWritable build = outKeyBuild
                .setState(state)
                .setCounty(county)
                .setCases(cases)
                .setDeaths(deaths)
                .build();

        context.write(build, NullWritable.get());

    }
}

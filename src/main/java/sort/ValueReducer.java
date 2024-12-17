package sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ValueReducer extends Reducer<DoubleWritable, Value, Text, Text> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<Value> values, Context context) throws IOException, InterruptedException {
        for (Value value : values) {
            Text category = new Text(value.getCategory());
            context.write(category, new Text(String.format("%.4f\t%d", -1 * key.get(), value.getQuantity())));
        }
    }
}
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SalesReducer extends Reducer<Text, Sales, Text, Sales> {
    @Override
    protected void reduce(Text key, Iterable<Sales> values, Context context) throws IOException, InterruptedException {
        double totalRevenue = 0.0;
        int totalQuantity = 0;
        for (Sales value : values) {
            double revenue = value.getRevenue();
            int quantity = value.getQuantity();
            totalRevenue += revenue;
            totalQuantity += quantity;
        }
        Sales result = new Sales(totalRevenue, totalQuantity);
        context.write(key, result);
    }
}
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SalesMapper extends Mapper<LongWritable, Text, Text, Sales> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("transaction_id")) {
            return;
        }
        String[] fields = value.toString().split(",");
        if (fields.length == 5) {
            String categoryName = fields[2];
            double price = Double.parseDouble(fields[3]);
            int quantity = Integer.parseInt(fields[4]);
            double revenue = price * quantity;
            context.write(new Text(categoryName), new Sales(revenue, quantity));
        }
    }
}
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TestCodeSample {
    @Test
    public void 두번째() throws IOException, InterruptedException {
        Count.HelloCountMapper mapper = new Count.HelloCountMapper();
        List<String> filter = Arrays.asList("hello");
        mapper.setFilterString(filter.iterator());
        IntWritable key = null;
        Text value = new Text("Hello hello Bon jure");
        Mapper.Context context = mock(Mapper.Context.class);

        mapper.map(key, value, context);
        verify(context, times(1)).write(new Text("hello"), new IntWritable(1));
    }

    @Test
    public void 첫번째() throws IOException, InterruptedException {
        Count.HelloCountMapper mapper = new Count.HelloCountMapper();
        IntWritable key = null;
        Text value = new Text("Hello Bon hllo hello");
        Mapper.Context context = mock(Mapper.Context.class);

        mapper.map(key, value, context);
        verify(context, times(1)).write(new Text("hello"), new IntWritable(1));
    }
}

package br.com.gt.training.section4;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

//STEP 2
class StringToKv extends DoFn<String, KV<String, Integer>>{
    @ProcessElement
    public void processElement(ProcessContext c){
        String input = c.element();
        String arr[] = input.split(",");
        c.output(KV.of(arr[0], Integer.valueOf(arr[3])));
    }
}

//step4
class KVToString extends DoFn<KV<String, Iterable<Integer>>, String> {

    @ProcessElement
    public void processElement(ProcessContext c){
        String stringKey = c.element().getKey();
        Iterable<Integer> vals = c.element().getValue();

        Integer sum = 0;
        for(Integer integer : vals){
            sum = sum + integer;
        }
        c.output(stringKey + ", " + sum.toString());
    }
}
public class GroupByKeyExample {


    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();

        //step 1 -> Read file
        PCollection<String> pCustOrderList = pipeline.apply(TextIO
                .read()
                .from("src/main/resources/files/GroupByKey_data.csv"));

        //step 2 -> Convert String to Kv
        PCollection<KV<String, Integer>> kvOrder =  pCustOrderList.apply(ParDo.of(new StringToKv()));

        //step 3 -> Apply groupKey and build KV<String, Iterable<integer>>
        PCollection<KV<String, Iterable<Integer>>> kvOrder2 = kvOrder.apply(GroupByKey.<String, Integer>create());

        //step4 -> Convert KV<String, Iterable<integer>> to string and write
        PCollection<String> output = kvOrder2.apply(ParDo.of(new KVToString()));

        output.apply(TextIO.write().to("src/main/resources/files/group_by_output.csv")
                .withHeader("ID, Amount")
                .withNumShards(1)
                .withSuffix(".csv")
        );

        pipeline.run();

    }
}

package br.com.gt.training.section4;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.grpc.v1p48p1.io.opencensus.stats.Aggregation;

public class CountExample {

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> pCount = pipeline.apply(TextIO.read().from("src/main/resources/files/Count.csv"));

        PCollection<Long> pLong = pCount.apply(Count.globally());

        pLong.apply(ParDo.of(new DoFn<Long, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                System.out.println(c.element());
            }
        }));

        pipeline.run();
    }
}

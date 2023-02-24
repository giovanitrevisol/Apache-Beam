//package br.com.gt.training.section3;
//
//import org.apache.beam.sdk.Pipeline;
//import org.apache.beam.sdk.io.TextIO;
//import org.apache.beam.sdk.transforms.DoFn;
//import org.apache.beam.sdk.transforms.ParDo;
//import org.apache.beam.sdk.transforms.View;
//import org.apache.beam.sdk.values.KV;
//import org.apache.beam.sdk.values.PCollection;
//import org.apache.beam.sdk.values.PCollectionView;
//
//import java.util.Map;
//
//public class SIdeInputExample {
//
//    public static void main(String[] args) {
//        Pipeline pipeline = Pipeline.create();
//
//        PCollection<String> pReturn = pipeline.apply(TextIO.read().from("src/main/resources/files/return.csv"))
//                .apply(ParDo.of(new DoFn<String, String>() {
//
//                    @ProcessElement
//                    public void process(ProcessContext c) {
//                        String arr[] = c.element().split(",");
//                        c.output(String.valueOf(KV.of(arr[0], arr[1])
//                                )
//                        );
//                    }
//                }));
//
//        PCollectionView<Map<String, String>> pMap = pReturn.apply(View.asMap());
//
//        PCollection<String> pCustList = pipeline.apply(TextIO.read().from("src/main/resources/files/cust_order.csv"));
//
//        pCustList.apply(ParDo.of(new DoFn<String, Object>() {
//            @ProcessElement
//            public void process(ProcessContext c) {
//                Map<String, String> psideInputView = c.sideInput(pMap);
//                String arr[] = c.element().split(",");
//
//                String custName = psideInputView.get(arr[0]);
//
//                if(custName == null){
//                    System.out.println(custName);
//                }
//            }
//        }).withSideInput(pMap));
//
//
//    }
//}

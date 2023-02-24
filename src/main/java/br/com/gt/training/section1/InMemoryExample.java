package br.com.gt.training;

import br.com.gt.training.entity.CustomerEntity;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;
import java.util.List;

public class InMemoryExample {

    public static void main(String[] args) {

        Pipeline p = Pipeline.create();

        PCollection<CustomerEntity> pList = p.apply(Create.of(getCustomers()));

        PCollection<String> pStringList = pList
                .apply(MapElements.into(TypeDescriptors.strings()).via((cust -> cust.getName())));

        pStringList.apply(TextIO.write().to("src/main/resources/files/customer.csv")
                .withNumShards(1)
                .withSuffix(".csv"));

        p.run();

    }


    static List<CustomerEntity> getCustomers(){

        CustomerEntity c1 = new CustomerEntity("1001", "Giovani");
        CustomerEntity c2 = new CustomerEntity("1002", "Lara");

        List<CustomerEntity> list = new ArrayList<>();
        list.add(c1);
        list.add(c2);

        return list;
    }
}

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class IotDeserializer implements Deserializer<IotEvent> {

    @Override
    public void close() {
        Deserializer.super.close();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public IotEvent deserialize(String args, byte[] args1) {

        ObjectMapper om = new ObjectMapper();
        IotEvent iotEvent = null;
        try {
            iotEvent = om.readValue(args1, IotEvent.class);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return iotEvent;
    }

}
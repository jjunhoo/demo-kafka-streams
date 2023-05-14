import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

public class KStreamJoinGlobalKTable {

    private static String APPLICATION_NAME = "global-table-join-application";
    private static String BOOTSTRAP_SERVERS = "localhost:9092";
    private static String ADDRESS_GLOBAL_TABLE = "address_v2"; // 파티션 2개
    private static String ORDER_STREAM = "order"; // 파티션 3개
    private static String ORDER_JOIN_STREAM = "order_join";

    public static void main(String[] args) {
        // 설정
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // GlobalKTable, KStream 설정
        StreamsBuilder builder = new StreamsBuilder();
        GlobalKTable<String, String> addressGlobalTable = builder.globalTable(ADDRESS_GLOBAL_TABLE); // GlobalKTable
        KStream<String, String> orderStream = builder.stream(ORDER_STREAM); // KStream

        // GlobalKTable, KStream 조인 및 결과 데이터를 특정 토픽으로 전송
        orderStream.join(addressGlobalTable,
                        (orderKey, orderValue) -> orderKey,
                        (order, address) -> order + " send to " + address)
                   .to(ORDER_JOIN_STREAM);

        KafkaStreams streams;
        streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}

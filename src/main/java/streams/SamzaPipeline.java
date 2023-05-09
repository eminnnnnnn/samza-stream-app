package streams;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.LongSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;

import streams.data.User;
import streams.data.UniqueStats;

public class SamzaPipeline implements StreamApplication, Serializable {
    private static final String KAFKA_SYSTEM_NAME = "kafka";
    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT =
            ImmutableList.of("localhost:2181");
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS =
            ImmutableList.of("localhost:9092");
    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS =
            ImmutableMap.of("replication.factor", "1");

    public static final String INPUT_STREAM_ID = "users-input";
    public static final String STATS_OUTPUT_STREAM_ID = "unique-stats-output";
    private static final Duration WINDOW_INTERVAL = Duration.ofMillis(100);

    static class StatsJoiner implements JoinFunction<String, KV<String, Long>,
                                              KV<String, Long>, UniqueStats> {

        @Override
        public UniqueStats apply(KV<String, Long> nameCnt, KV<String, Long> countryCnt) {
            return UniqueStats
                    .builder()
                    .numCountries(countryCnt.value)
                    .numNames(nameCnt.value)
                    .build();
        }

        @Override
        public String getFirstKey(KV<String, Long> nameCnt) {
            return nameCnt.getKey();
        }

        @Override
        public String getSecondKey(KV<String, Long> countryCnt) {
            return countryCnt.getKey();
        }
    }

    @Override
    public void describe(StreamApplicationDescriptor appDesc) {
        KafkaSystemDescriptor kafkaSysDesc = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
            .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
            .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
            .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

        // Serdes
        StringSerde stringSerde = new StringSerde();
        JsonSerdeV2<User> userSerde = new JsonSerdeV2<>(User.class);
        KVSerde<String, Long> stringLongKVSerde =
                KVSerde.of(new StringSerde(), new LongSerde());
        JsonSerdeV2<UniqueStats> uniqueStatsSerde = new JsonSerdeV2<>(UniqueStats.class);

        // Input Descriptors
        KafkaInputDescriptor<User> inUsersDesc =
            kafkaSysDesc.getInputDescriptor(INPUT_STREAM_ID, userSerde);

        // Output Descriptors
        KafkaOutputDescriptor<UniqueStats> outUniqueStatsDesc =
            kafkaSysDesc.getOutputDescriptor(STATS_OUTPUT_STREAM_ID, uniqueStatsSerde);

        appDesc.withDefaultSystem(kafkaSysDesc);

        // Input Streams
        MessageStream<User> usersInStream =
            appDesc.getInputStream(inUsersDesc);

        // Output Streams
        OutputStream<UniqueStats> uniqueStatsOutStream =
            appDesc.getOutputStream(outUniqueStatsDesc);

        // Processing
        MessageStream<WindowPane<Void, Collection<User>>> windowInStream =
            usersInStream
                .window(Windows.tumblingWindow(
                            WINDOW_INTERVAL,
                            userSerde),
                        "win");

        MessageStream<KV<String, Collection<User>>> windowUserNames =
            windowInStream
                .map(w -> KV.of(w.getKey().getPaneId(), w.getMessage()));

        MessageStream<KV<String, Collection<User>>> windowCountries =
            windowInStream
                .map(w -> KV.of(w.getKey().getPaneId(), w.getMessage()));

        MessageStream<KV<String, Long>> userStat =
            windowUserNames
                .map(wp -> {
                    Set<String> uniques = new HashSet<>();
                    wp.value.forEach(u -> uniques.add(u.getName()));
                    return KV.of(wp.key, (long)uniques.size());
                });

        MessageStream<KV<String, Long>> countryStat =
            windowCountries
                .map(wp -> {
                    Set<String> uniques = new HashSet<>();
                    wp.value.forEach(u -> uniques.add(u.getCountry().toLowerCase()));
                    return KV.of(wp.key, (long)uniques.size());
                });

        userStat
            .join(countryStat, new StatsJoiner(),
                  stringSerde,
                  stringLongKVSerde,
                  stringLongKVSerde,
                  Duration.ofSeconds(5), "join")
            .sendTo(uniqueStatsOutStream);
    }
}

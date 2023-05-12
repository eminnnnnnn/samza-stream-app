package streams.tests;

import java.time.Duration;
import java.util.List;

import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.junit.Assert;
import org.junit.Test;

import streams.SamzaPipeline;
import streams.data.User;
import streams.data.UniqueStats;
import streams.tests.utils.TestDataGenerator;

public class SamzaPipelineTest {
    @Test
    public void testTumblingWindowExample() {
        List<User> users = TestDataGenerator.genUsers();

        InMemorySystemDescriptor inMemorySystem =
                new InMemorySystemDescriptor("test-kafka");

        InMemoryInputDescriptor<User> userInputDescriptor =
                inMemorySystem.getInputDescriptor(
                        SamzaPipeline.INPUT_STREAM_ID,
                        new NoOpSerde<>()
                );

        InMemoryOutputDescriptor<UniqueStats> uniqueStatsOutDesc =
                inMemorySystem.getOutputDescriptor(
                        SamzaPipeline.STATS_OUTPUT_STREAM_ID,
                        new NoOpSerde<>()
                );

        TestRunner
                .of(new SamzaPipeline())
                .addInputStream(userInputDescriptor, users)
                .addOutputStream(uniqueStatsOutDesc, 1)
                .run(Duration.ofMinutes(1));

        TestRunner
                .<UniqueStats>consumeStream(uniqueStatsOutDesc,
                                     Duration.ofMillis(1000))
                .get(0)
                .forEach(uniqStat -> {
//                    System.out.println("names: " + uniqStat.getNumNames()
//                            + " countries:" + uniqStat.getNumCountries());
                    Assert.assertTrue(uniqStat.getNumNames() > 0);
                    Assert.assertTrue(uniqStat.getNumCountries() > 0);
                });
    }

}

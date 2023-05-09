package streams.tests.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.time.Instant;
import java.util.Random;

import org.apache.samza.operators.KV;
import scala.Int;
import streams.data.User;

public class TestDataGenerator {
    private static final String[] COUNTRIES = {
      "Russia", "Rhodesia", "Palestine", "Argentina", "USA",
      "France", "Germany", "China", "Japan"
    };
    public static List<KV<String, User>> genUsers() {
        Random random = new Random(12345678);
        List<KV<String, User>> users = new ArrayList<>();
        int MAX_USERS = 1000;
        for (int i = 0; i < MAX_USERS; i++) {
            User user = User.builder()
                    .name("user" + String.valueOf(random.nextInt(25)))
                    .country(COUNTRIES[random.nextInt(COUNTRIES.length)])
                    .date(Date.from(Instant.now()))
                    .build();
            users.add(KV.of(user.getName(), user));
        }
        return users;
    }
//    public static List<KV<String,User>> genUsers() {
//        return new ArrayList<>() {{
//            add(KV.of("user1",
//                      new User("user1", "Russia",
//                        Date.from(Instant.now()))));
//            add(KV.of("user1",
//                      new User("user1", "Russia",
//                        Date.from(Instant.now()))));
//            add(KV.of("user2",
//                      new User("user2", "USA",
//                        Date.from(Instant.now()))));
//            add(KV.of("user2",
//                      new User("user2", "USA",
//                        Date.from(Instant.now()))));
//            add(KV.of("user1",
//                      new User("user1", "Russia",
//                        Date.from(Instant.now()))));
//            add(KV.of("user2",
//                      new User("user2", "USA",
//                        Date.from(Instant.now()))));
//            add(KV.of("user3",
//                      new User("user3", "France",
//                        Date.from(Instant.now()))));
//            add(KV.of("user4",
//                      new User("user4", "Kazakhstan",
//                        Date.from(Instant.now()))));
//            add(KV.of("user2",
//                      new User("user2", "USA",
//                        Date.from(Instant.now()))));
//        }};
//    }
}

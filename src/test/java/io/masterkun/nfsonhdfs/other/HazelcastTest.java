package io.masterkun.nfsonhdfs.other;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.List;
import java.util.stream.Collectors;

public class HazelcastTest {
    public static void main(String[] args) {
        Config config = new Config();
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        List<String> collect = instance.getCluster().getMembers().stream()
                .filter(member -> !member.localMember())
                .map(member -> member.getAddress().getHost())
                .collect(Collectors.toList());
        System.out.println(collect);
    }
}

package io.masterkun.nfsonhdfs;

import org.junit.Ignore;
import org.junit.Test;

public class NfsServerAppStarterTest {

    public static void main(String[] args) {

        System.setProperty("java.security.krb5.conf", "src/test/resources/krb5.conf");
        NfsServerAppStarter.main(new String[]{"src/test/resources/nfs-server.yaml"});
    }
}

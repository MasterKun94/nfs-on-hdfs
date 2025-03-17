package io.masterkun.nfsonhdfs;

public class NfsServerAppStarterTest {

    public static void main(String[] args) {

        System.setProperty("java.security.krb5.conf", "src/test/resources/krb5.conf");
        NfsServerAppStarter.main(new String[]{"src/test/resources/nfs-server.yaml"});
    }
}

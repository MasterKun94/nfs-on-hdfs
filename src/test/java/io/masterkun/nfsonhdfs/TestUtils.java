package io.masterkun.nfsonhdfs;

import com.sun.security.auth.UnixNumericUserPrincipal;
import org.apache.hadoop.security.UserGroupInformation;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.CompoundContextBuilder;
import org.dcache.nfs.v4.NFSv41Session;
import org.dcache.nfs.v4.xdr.sessionid4;
import org.dcache.oncrpc4j.rpc.RpcAuthTypeNone;
import org.dcache.oncrpc4j.rpc.RpcCall;

import javax.security.auth.Subject;
import java.io.IOException;
import java.util.Collections;

public class TestUtils {

    public static void kerberosInit() throws IOException {
        System.setProperty("java.security.krb5.conf", "src/test/resources/krb5.conf");
        UserGroupInformation.loginUserFromKeytab(
                "admin/admin@HADOOP.COM",
                "src/test/resources/admin.keytab"
        );
    }

    public static void contextInit() {
        CompoundContextBuilder builder = new CompoundContextBuilder()
                .withCall(new RpcCall(0, 0, new RpcAuthTypeNone(), null, null));
        CompoundContext context = new CompoundContext(builder) {
            @Override
            public Subject getSubject() {
                return new Subject(false,
                        Collections.singleton(new UnixNumericUserPrincipal(0)),
                        Collections.emptySet(),
                        Collections.emptySet());
            }

            @Override
            public NFSv41Session getSession() {
                return new NFSv41Session(null,
                        new sessionid4("test".getBytes()),
                        0, 0, 0, 0);
            }
        };
        CallContext.setCtx(context);
    }
}

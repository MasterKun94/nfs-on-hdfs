package io.masterkun.nfsonhdfs;

import com.google.common.annotations.VisibleForTesting;
import com.sun.security.auth.UnixNumericUserPrincipal;
import io.masterkun.nfsonhdfs.util.Utils;
import io.masterkun.nfsonhdfs.vfs.FileHandle;
import org.dcache.nfs.v4.CompoundContext;

import javax.security.auth.Subject;
import java.io.IOException;
import java.net.InetAddress;
import java.security.Principal;
import java.util.NoSuchElementException;

public class CallContext {

    private static final ThreadLocal<CompoundContext> SUBJECT_TL = new ThreadLocal<>();
    private static final ThreadLocal<String> REAL_PRINCIPAL_TL = new ThreadLocal<>();

    @VisibleForTesting
    public static void setCtx(CompoundContext ctx) {
        SUBJECT_TL.set(ctx);
    }

    static void clearCtx() {
        SUBJECT_TL.remove();
    }

    public static void setRealPrincipal(String realPrincipal) {
        REAL_PRINCIPAL_TL.set(realPrincipal);
    }

    public static void clearRealPrincipal() {
        REAL_PRINCIPAL_TL.remove();
    }

    public static String getPrincipal() {
        String realPrincipal = REAL_PRINCIPAL_TL.get();
        return realPrincipal == null ?
                getPrincipal(SUBJECT_TL.get().getSubject()) :
                realPrincipal;
    }

    public static CompoundContext getCompoundContext() {
        return SUBJECT_TL.get();
    }

    public static int getExportIdx() throws IOException {
        CompoundContext ctx = SUBJECT_TL.get();
        return ctx.currentInode().exportIndex();
    }

    public static FileHandle getFileHandle(long fileId) {
        String realPrincipal = REAL_PRINCIPAL_TL.get();
        CompoundContext ctx = SUBJECT_TL.get();
        return new FileHandle(
                fileId,
                realPrincipal == null ? getPrincipal(ctx.getSubject()) : realPrincipal,
                ctx.getSession().id().value.clone()
        );
    }

    public static InetAddress getClientAddress() {
        CompoundContext ctx = SUBJECT_TL.get();
        return ctx == null ? null : ctx.getRemoteSocketAddress().getAddress();
    }

    private static String getPrincipal(Subject subject) {
        for (Principal principal : subject.getPrincipals()) {
            if (principal instanceof UnixNumericUserPrincipal unixNumericUserPrincipal) {
                return Utils.getNfsIdMapping().uidToPrincipal(Integer.parseInt(unixNumericUserPrincipal.getName()));
            }
        }
        throw new NoSuchElementException("no unix user for " + subject);
    }
}

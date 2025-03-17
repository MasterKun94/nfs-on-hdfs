package io.masterkun.nfsonhdfs.vfs;

import io.masterkun.nfsonhdfs.util.StringBuilderFormattable;

import java.util.Base64;
import java.util.Objects;

public record FileHandle(long fileId, String principal,
                         byte[] sessionId) implements StringBuilderFormattable {
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileHandle that = (FileHandle) o;
        return fileId == that.fileId && Objects.equals(principal, that.principal);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileId, principal);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        formatTo(builder);
        return builder.toString();
    }

    @Override
    public void formatTo(StringBuilder buffer) {
        buffer.append("FileHandle{fileId=")
                .append(fileId)
                .append(", principal=")
                .append(principal)
                .append(", sessionId=")
                .append(Base64.getEncoder().encodeToString(sessionId))
                .append('}');
    }
}

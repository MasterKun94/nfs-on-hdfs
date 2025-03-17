package io.masterkun.nfsonhdfs.util.cli;

import java.io.Serializable;

public sealed class CliEvent implements Serializable permits CleanCacheEvent, RefreshFsTableEvent
        , RefreshIdMappingEvent {
}

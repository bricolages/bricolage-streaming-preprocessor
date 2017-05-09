package org.bricolages.streaming.preflight.domains;

import lombok.*;

public class DomainDefaultValues {
    @Getter
    private DateDomain date;
    @Getter
    private LogTimeDomain logTime;
    @Getter
    private StringDomain string;
    @Getter
    private UnixtimeDomain unixtime;
}

package org.bricolages.streaming.preflight.domains;

import java.io.IOException;
import java.io.Reader;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

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

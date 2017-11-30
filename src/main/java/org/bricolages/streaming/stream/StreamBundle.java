package org.bricolages.streaming.stream;
import javax.persistence.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@Slf4j
@Entity
@Table(name="strload_stream_bundles")
public class StreamBundle {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Column(name="stream_bundle_id")
    @Getter
    long id;

    @ManyToOne
    @JoinColumn(name="stream_id")
    @Getter
    DataStream stream;

    @Column(name="s3_bucket", nullable=false)
    @Getter
    String bucket;

    @Column(name="s3_prefix", nullable=false)
    @Getter
    String prefix;

    @Column(name="dest_bucket", nullable=false)
    @Getter
    String destBucket;

    @Column(name="dest_prefix", nullable=false)
    @Getter
    String destPrefix;

    public StreamBundle(DataStream stream, String bucket, String prefix, String destBucket, String destPrefix) {
        this.stream = stream;
        this.bucket = bucket;
        this.prefix = prefix;
        this.destBucket = destBucket;
        this.destPrefix = destPrefix;
    }
}

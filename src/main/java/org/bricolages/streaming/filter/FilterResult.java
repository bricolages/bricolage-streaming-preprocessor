package org.bricolages.streaming.filter;
import javax.persistence.*;
import java.util.Date;
import java.sql.Timestamp;
import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@Entity
@Table(name="preproc_log")
public class FilterResult {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    long id;

    @Column(name="src_data_file")
    String srcDataFile;

    @Column(name="dest_data_file")
    String destDataFile;

    @Column(name="input_rows")
    public long inputRows = 0;

    @Column(name="output_rows")
    public long outputRows = 0;

    @Column(name="error_rows")
    public long errorRows = 0;

    @Column(name="status")
    String status = STATUS_STARTED;

    static final String STATUS_STARTED = "started";
    static final String STATUS_SUCCESS = "success";
    static final String STATUS_FAILURE = "failure";

    @Column(name="start_time")
    Timestamp startTime = null;

    @Column(name="end_time")
    Timestamp endTime = null;

    @Column(name="message")
    String message;

    @Column(name="dispatched")
    boolean dispatched;

    public FilterResult(String src, String dest) {
        this.srcDataFile = src;
        this.destDataFile = dest;
        this.startTime = currentTimestamp();
    }

    public void succeeded() {
        this.status = STATUS_SUCCESS;
        this.endTime = currentTimestamp();
    }

    public void failed(String msg) {
        this.status = STATUS_FAILURE;
        this.endTime = currentTimestamp();
        this.message = msg;
    }

    Timestamp currentTimestamp() {
        return new Timestamp(System.currentTimeMillis());
    }

    public void dispatched() {
        this.dispatched = true;
    }
}

package org.bricolages.streaming.stream.op;
import javax.persistence.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@AllArgsConstructor
@Slf4j
@Entity
@Table(name="strload_sequence")
public class SequencialNumber {
    @Column(name="last_value")
    @Getter
    long lastValue;

    @Id
    @Column(name="nextval")
    @Getter
    long nextValue;

    // This depends on sequence object definition, but we cannot get
    // nextval and last_value at the same time (atomically).
    // So we use constant here as the second best.
    static final long BATCH_SIZE = 10000;

    void setLastValueFromNextValue() {
        this.lastValue = nextValue - BATCH_SIZE;
    }
}

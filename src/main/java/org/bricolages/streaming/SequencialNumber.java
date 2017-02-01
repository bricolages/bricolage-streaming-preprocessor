package org.bricolages.streaming;
import javax.persistence.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@Slf4j
@Entity
@Table(name="strload_sequence")
public class SequencialNumber {
    // HACK: JPA Repository needs PK
    @Id
    @GeneratedValue
    @Column(name="sequence_name")
    String name;

    @Column(name="last_value")
    @Getter
    long lastValue;

    @Column(name="nextval")
    @Getter
    long nextValue;
}

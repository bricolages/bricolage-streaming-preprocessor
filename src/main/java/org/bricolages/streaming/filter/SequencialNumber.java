package org.bricolages.streaming.filter;
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
    @GeneratedValue
    @Column(name="nextval")
    @Getter
    long nextValue;
}

package org.bricolages.streaming;
import javax.persistence.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@Slf4j
@Entity
@Table(name="preproc_sequence")
public class SequencialNumber {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Getter
    long id;

    @Column(name="value")
    @Getter
    @Setter
    long value;
}

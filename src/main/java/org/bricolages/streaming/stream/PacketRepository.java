package org.bricolages.streaming.stream;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PacketRepository extends JpaRepository<Packet, Long> {
}

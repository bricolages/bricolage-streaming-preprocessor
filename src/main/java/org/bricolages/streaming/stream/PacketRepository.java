package org.bricolages.streaming.stream;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.dao.DataIntegrityViolationException;
import lombok.*;

public interface PacketRepository extends JpaRepository<Packet, Long> {
    Packet findByObjectUrl(String url);

    default public Packet upsert(Packet packet) {
        val curr = findByObjectUrl(packet.getObjectUrl());
        if (curr != null) {
            curr.merge(packet);
            return save(curr);
        }
        try {
            return save(packet);
        }
        catch (DataIntegrityViolationException ex) {
            val curr2 = findByObjectUrl(packet.getObjectUrl());
            curr2.merge(packet);
            return save(curr2);
        }
    }
}

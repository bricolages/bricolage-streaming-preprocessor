package org.bricolages.streaming.stream;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.dao.DataIntegrityViolationException;
import lombok.*;

public interface PacketRepository extends JpaRepository<Packet, Long> {
    Packet findByObjectUrl(String url);

    default public Packet upsert(Packet packet) {
        try {
            return save(packet);
        }
        catch (DataIntegrityViolationException ex) {
            val newPacket = packet;
            val oldPacket = findByObjectUrl(newPacket.getObjectUrl());
            oldPacket.merge(newPacket);
            return save(oldPacket);
        }
    }
}

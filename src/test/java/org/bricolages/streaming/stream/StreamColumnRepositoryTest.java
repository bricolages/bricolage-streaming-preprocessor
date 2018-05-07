package org.bricolages.streaming.stream;
import org.bricolages.streaming.exception.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.boot.test.autoconfigure.orm.jpa.*;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import java.util.HashSet;
import java.util.Set;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.sql.Timestamp;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;
import lombok.*;
import org.springframework.transaction.annotation.Transactional;
import org.hibernate.Hibernate;

@DataJpaTest
@RunWith(SpringJUnit4ClassRunner.class)
public class StreamColumnRepositoryTest {
    @Autowired TestEntityManager entityManager;
    @Autowired PacketStreamRepository streamRepos;
    @Autowired StreamColumnRepository columnRepos;

    @Test
    public void saveUnknownColumns() throws Exception {
        val s = new PacketStream("schema.table");
        s.initialized = true;
        s.columnInitialized = true;
        val stream = entityManager.persist(s);
        columnRepos.saveUnknownColumns(stream, nameSet("a", "b", "c"));

        val columns = columnRepos.findColumns(stream);
        assertEquals(3, columns.size());
        assertEquals("a", columns.get(0).getName());
        assertEquals("b", columns.get(1).getName());
        assertEquals("c", columns.get(2).getName());
    }

    @Test
    public void saveUnknownColumns_dup() throws Exception {
        val s = new PacketStream("schema.table");
        s.initialized = true;
        s.columnInitialized = true;
        val stream = entityManager.persist(s);

        columnRepos.saveUnknownColumns(stream, nameSet("a", "b"));
        columnRepos.saveUnknownColumns(stream, nameSet("b", "c"));

        entityManager.clear();
        val columns = columnRepos.findColumns(stream);
        assertEquals(3, columns.size());
        assertEquals("a", columns.get(0).getName());
        assertEquals("b", columns.get(1).getName());
        assertEquals("c", columns.get(2).getName());
    }

    Set<String> nameSet(String... names) {
        val set = new HashSet<String>();
        for (val n : names) {
            set.add(n);
        }
        return set;
    }
}

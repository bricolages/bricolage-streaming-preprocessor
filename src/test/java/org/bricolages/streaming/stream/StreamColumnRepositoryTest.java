package org.bricolages.streaming.stream;
import org.bricolages.streaming.exception.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.*;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;
import lombok.val;

@DataJpaTest
@RunWith(SpringJUnit4ClassRunner.class)
public class StreamColumnRepositoryTest {
    @Autowired TestEntityManager entityManager;
    @Autowired PacketStreamRepository streamRepos;
    @Autowired StreamColumnRepository columnRepos;

    @Test
    public void saveUnknownColumn() throws Exception {
        val s = new PacketStream("schema.table");
        s.initialized = true;
        s.columnInitialized = true;
        val stream = entityManager.persist(s);

        columnRepos.saveUnknownColumn(stream, "a");
        columnRepos.saveUnknownColumn(stream, "b");
        columnRepos.saveUnknownColumn(stream, "c");

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

        columnRepos.saveUnknownColumn(stream, "a");
        columnRepos.saveUnknownColumn(stream, "b");
        columnRepos.saveUnknownColumn(stream, "b");
        columnRepos.saveUnknownColumn(stream, "c");

        entityManager.clear();
        val columns = columnRepos.findColumns(stream);
        assertEquals(3, columns.size());
        assertEquals("a", columns.get(0).getName());
        assertEquals("b", columns.get(1).getName());
        assertEquals("c", columns.get(2).getName());
    }
}

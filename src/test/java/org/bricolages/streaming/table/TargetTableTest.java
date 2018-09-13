package org.bricolages.streaming.table;
import org.bricolages.streaming.exception.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.*;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;
import lombok.*;

@DataJpaTest
@RunWith(SpringJUnit4ClassRunner.class)
public class TargetTableTest {
    @Autowired TestEntityManager entityManager;
    @Autowired TargetTableRepository tableRepos;

    @Test
    public void _ctor() throws Exception {
        TargetTable table = new TargetTable("schema", "table", "schema.table", "bucket", "prefix");
        table = entityManager.persist(table);
        assertEquals("bucket", table.getBucket());
        assertEquals("prefix", table.getPrefix());
        assertEquals(TargetTable.DEFAULT_LOAD_BATCH_SIZE, table.loadBatchSize);
        assertTrue(
            (table.loadInterval >= TargetTable.DEFAULT_LOAD_INTERVAL_MIN) &&
            (table.loadInterval <= TargetTable.DEFAULT_LOAD_INTERVAL_MAX)
        );
    }
}

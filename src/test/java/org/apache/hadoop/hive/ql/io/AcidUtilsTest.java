package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static com.google.common.io.Files.createTempDir;
import static org.junit.Assert.assertEquals;

public class AcidUtilsTest
{
    private FileSystem fileSystem;
    private File rootDir;

    @Before
    public void setUp()
            throws Exception
    {
        fileSystem = FileSystem.get(new Configuration(false));
        rootDir = createTempDir();
    }

    @Test
    public void testParseBase()
    {
        assertEquals(123, AcidUtils.parseBase(new Path("some/dir/base_123")));
        // v456 VISIBILITY part should be ignored
        assertEquals(123, AcidUtils.parseBase(new Path("some/dir/base_123_v456")));
    }

    @Test
    public void testParseDelta()
            throws IOException
    {
        AcidUtils.ParsedDelta parsedDelta;
        parsedDelta = parseDelta("/some/dir/delta_12_34");
        assertEquals(12, parsedDelta.getMinWriteId());
        assertEquals(34, parsedDelta.getMaxWriteId());
        assertEquals(0, parsedDelta.getStatementId());

        parsedDelta = parseDelta("/some/dir/delete_delta_12_34");
        assertEquals(12, parsedDelta.getMinWriteId());
        assertEquals(34, parsedDelta.getMaxWriteId());
        assertEquals(0, parsedDelta.getStatementId());

        parsedDelta = parseDelta("/some/dir/delta_12_34_56");
        assertEquals(12, parsedDelta.getMinWriteId());
        assertEquals(34, parsedDelta.getMaxWriteId());
        assertEquals(56, parsedDelta.getStatementId());

        parsedDelta = parseDelta("/some/dir/delete_delta_12_34_56");
        assertEquals(12, parsedDelta.getMinWriteId());
        assertEquals(34, parsedDelta.getMaxWriteId());
        assertEquals(56, parsedDelta.getStatementId());

        // v78 VISIBILITY part should be ignored

        parsedDelta = parseDelta("/some/dir/delta_12_34_v78");
        assertEquals(12, parsedDelta.getMinWriteId());
        assertEquals(34, parsedDelta.getMaxWriteId());
        assertEquals(0, parsedDelta.getStatementId());

        parsedDelta = parseDelta("/some/dir/delete_delta_12_34_v78");
        assertEquals(12, parsedDelta.getMinWriteId());
        assertEquals(34, parsedDelta.getMaxWriteId());
        assertEquals(0, parsedDelta.getStatementId());

        parsedDelta = parseDelta("/some/dir/delta_12_34_56_v78");
        assertEquals(12, parsedDelta.getMinWriteId());
        assertEquals(34, parsedDelta.getMaxWriteId());
        assertEquals(56, parsedDelta.getStatementId());

        parsedDelta = parseDelta("/some/dir/delete_delta_12_34_56_v78");
        assertEquals(12, parsedDelta.getMinWriteId());
        assertEquals(34, parsedDelta.getMaxWriteId());
        assertEquals(56, parsedDelta.getStatementId());
    }

    private AcidUtils.ParsedDelta parseDelta(String path)
            throws IOException
    {
        fileSystem.create(new Path(rootDir.getAbsolutePath() + path));
        return AcidUtils.parsedDelta(new Path(rootDir.getAbsolutePath() + path), fileSystem);
    }
}

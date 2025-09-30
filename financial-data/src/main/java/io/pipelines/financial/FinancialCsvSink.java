package io.pipelines.financial;

import io.pipelines.core.BatchSink;
import io.pipelines.core.Record;
import io.pipelines.core.Sink;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * Writes CSV bytes to files named by the record's fileName.
 */
public class FinancialCsvSink implements Sink<FinancialCsv>, BatchSink<FinancialCsv> {
    private final Path outDir;

    public FinancialCsvSink(Path outDir) throws IOException {
        this.outDir = outDir;
        Files.createDirectories(outDir);
    }

    @Override
    public void accept(Record<FinancialCsv> record) throws IOException {
        FinancialCsv csv = record.payload();
        if (csv.data() == null || csv.data().length == 0) return; // nothing to write
        Path out = outDir.resolve(csv.fileName());
        boolean exists = Files.exists(out);
        if (!exists) {
            Files.createDirectories(out.getParent());
            // Write header for new files
            byte[] header = "date,open,high,low,close,volume\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            Files.write(out, header, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        }
        Files.write(out, csv.data(), StandardOpenOption.APPEND);
    }

    @Override
    public void acceptBatch(List<Record<FinancialCsv>> records) throws IOException {
        for (Record<FinancialCsv> r : records) {
            accept(r);
        }
    }
}

package org.apache.iceberg;

import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.common.DynFields.UnboundField;
import org.apache.iceberg.metrics.MetricsReporter;

public class AlluvialRowDelta extends BaseRowDelta {
    private static final UnboundField<Long> startingSnapshotIdField = DynFields.builder()
            .hiddenImpl(BaseRowDelta.class, "startingSnapshotId")
            .build();
    private static final UnboundField<MetricsReporter> reporterField = DynFields.builder()
            .hiddenImpl(BaseTable.class, "reporter")
            .build();

    public static AlluvialRowDelta of(Table table) {
        var ops = ((HasTableOperations) table).operations();
        var reporter = reporterField.get(table);

        var rd = new AlluvialRowDelta(table.name(), ops);
        rd.reportWith(reporter);
        return rd;
    }

    private boolean validateFromHead = false;

    AlluvialRowDelta(String tableName, TableOperations ops) {
        super(tableName, ops);
    }

    public RowDelta validateFromHead() {
        this.validateFromHead = true;
        return this;
    }

    @Override
    public Snapshot apply() {
        if (validateFromHead) {
            var base = refresh();
            var snapshotId = base.currentSnapshot() == null ? null : base.currentSnapshot().snapshotId();
            // validateFromSnapshot does NOT accept null input
            startingSnapshotIdField.set(this, snapshotId);
        }
        return super.apply();
    }
}

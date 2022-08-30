package org.apache.iceberg;

import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.common.DynFields.UnboundField;

public class AlluvialRowDelta extends BaseRowDelta {
    private static final UnboundField<Long> startingSnapshotIdField = DynFields.builder()
        .hiddenImpl(BaseRowDelta.class, "startingSnapshotId")
        .build();

    public static AlluvialRowDelta of(Table table) {
        var ops = ((HasTableOperations) table).operations();
        return new AlluvialRowDelta(table.name(), ops);
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

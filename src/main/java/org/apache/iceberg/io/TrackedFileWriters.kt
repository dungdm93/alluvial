package org.apache.iceberg.io

import dev.alluvial.sink.iceberg.io.PathOffset
import org.apache.iceberg.common.DynFields
import org.apache.iceberg.deletes.EqualityDeleteWriter
import org.apache.iceberg.deletes.PositionDeleteWriter


class RollingTrackedFileWriter<T, W : FileWriter<T, R>, R> internal constructor(
    private val rollingFileWriter: RollingFileWriter<T, W, R>
) : FileWriter<T, R> by rollingFileWriter,
    TrackedFileWriter<T, R> {
    override fun trackedWrite(row: T): PathOffset {
        val location = rollingFileWriter.currentFilePath()
        val offset = rollingFileWriter.currentFileRows()
        val pathOffset = PathOffset.of(location, offset)

        rollingFileWriter.write(row)
        return pathOffset
    }
}

class NormalTrackedFileWriter<T, R> internal constructor(
    private val fileWriter: FileWriter<T, R>
) : FileWriter<T, R> by fileWriter,
    TrackedFileWriter<T, R> {
    companion object {
        private val dataLocationGetter = DynFields.builder()
            .hiddenImpl(DataWriter::class.java, "location")
            .build<String>()

        private val eqDelLocationGetter = DynFields.builder()
            .hiddenImpl(EqualityDeleteWriter::class.java, "location")
            .build<String>()

        private val posDelLocationGetter = DynFields.builder()
            .hiddenImpl(PositionDeleteWriter::class.java, "location")
            .build<String>()
    }

    private val location = when (fileWriter) {
        is DataWriter<*> -> dataLocationGetter.get(fileWriter)
        is EqualityDeleteWriter<*> -> eqDelLocationGetter.get(fileWriter)
        is PositionDeleteWriter<*> -> posDelLocationGetter.get(fileWriter)
        else -> throw RuntimeException("Unsupported fileWriter class ${fileWriter.javaClass}")
    }
    private var offset = 0L

    override fun write(row: T) {
        fileWriter.write(row)
        offset++
    }

    override fun trackedWrite(row: T): PathOffset {
        val pathOffset = PathOffset.of(location, offset)

        fileWriter.write(row)
        return pathOffset
    }
}

package org.apache.iceberg.io

import dev.alluvial.sink.iceberg.io.PathOffset


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
    private val location = "" // TODO
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

@file:JvmName("Main")

package dev.alluvial.runtime

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import java.io.FileReader

val mapper: YAMLMapper = YAMLMapper.builder()
    .addModule(KotlinModule.Builder().build())
    .addModule(JavaTimeModule())
    .build()

fun parseConfig(args: Array<out String>): Config {
    if (args.isEmpty()) {
        throw RuntimeException("missing configuration file")
    }
    if (args.size != 1) {
        throw RuntimeException("require exact one argument, got ${args.size}")
    }
    val configFile = args[0]
    return mapper.readValue(FileReader(configFile))
}

private fun runStreamController(config: Config) {
    val alluvial = StreamController()
    alluvial.configure(config)
    alluvial.run()
}

fun main(vararg args: String) {
    val config = parseConfig(args)
    runStreamController(config)
}

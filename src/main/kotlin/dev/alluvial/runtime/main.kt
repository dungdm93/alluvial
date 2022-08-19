@file:JvmName("Main")

package dev.alluvial.runtime

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import java.io.FileReader
import kotlin.system.exitProcess

val mapper: YAMLMapper = YAMLMapper.builder()
    .addModule(KotlinModule.Builder().build())
    .addModule(JavaTimeModule())
    .build()


fun parseArgs(args: Array<out String>): Pair<String, String> {
    if (args.isEmpty()) {
        printUsage()
        throw RuntimeException("missing command")
    }

    val command = args[0]
    if (command == "help") {
        printUsage()
        exitProcess(0)
    }

    if (args.size != 2) {
        printUsage()
        val message = if (args.size < 2)
            "missing configuration file" else
            "too many arguments"
        throw RuntimeException(message)
    }
    val configFile = args[1]

    return Pair(command, configFile)
}

fun loadConfig(file: String): Config {
    return mapper.readValue(FileReader(file))
}

private fun printUsage() {
    val usage = """
        Usage: alluvial [COMMAND] [CONFIG_FILE]

        Commands:
          stream: run StreamController to CDC from Kafka topics to Iceberg tables
          manage: run TableManager to auto-manage Iceberg tables (compact, tagging, expiring snapshots,...)
          help:   display this help and exit
    """.trimIndent()
    println(usage)
}

private fun runStreamController(config: Config) {
    val controller = StreamController()
    controller.configure(config)
    controller.run()
}

private fun runTableManager(config: Config) {
    val manager = TableManager()
    manager.configure(config)
    manager.run()
}

fun main(vararg args: String) {
    val (command, configFile) = parseArgs(args)
    val config = loadConfig(configFile)

    when (command) {
        "stream" -> runStreamController(config)
        "manage" -> runTableManager(config)
        else -> {
            printUsage()
            throw RuntimeException("Unknown command $command")
        }
    }
}

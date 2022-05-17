@file:JvmName("Main")

package dev.alluvial.runtime

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import dev.alluvial.utils.MetricExporter
import dev.alluvial.utils.MetricProviderFactory
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import java.io.FileReader

val mapper = YAMLMapper().also {
    it.registerModule(KotlinModule())
}

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

fun configureMetrics(metricConfig: MetricConfig): MetricExporter {
    val provider = MetricProviderFactory.create(metricConfig)
    Metrics.globalRegistry.add(provider.registry)

    val tags = metricConfig.commonTags.map { Tag.of(it.key, it.value) }
    Metrics.globalRegistry.config().commonTags(tags)

    return provider
}

fun main(vararg args: String) {
    val config = parseConfig(args)
    val metricProvider = configureMetrics(config.metric)

    metricProvider.use {
        it.run()
        val alluvial = Alluvial()
        alluvial.configure(config)
        alluvial.run()
    }
}

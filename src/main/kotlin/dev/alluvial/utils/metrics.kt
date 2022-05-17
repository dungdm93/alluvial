package dev.alluvial.utils

import dev.alluvial.runtime.MetricConfig
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.config.validate.PropertyValidator.getBoolean
import io.micrometer.core.instrument.config.validate.PropertyValidator.getString
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import io.prometheus.client.exporter.HTTPServer
import java.io.Closeable
import java.net.InetAddress
import java.net.InetSocketAddress

abstract class MetricExporter : Runnable, Closeable {
    abstract val registry: MeterRegistry
}

class PrometheusMetricExporter(config: MetricConfig) : MetricExporter() {
    private val exporterConfig = PrometheusMetricExporterConfig(config.properties)
    override val registry = PrometheusMeterRegistry(exporterConfig)
    private var server: HTTPServer? = null

    override fun run() {
        val inetAddress = InetAddress.getByName(exporterConfig.serverAddress())
        val port = exporterConfig.serverPort()
        val inetSocketAddress = InetSocketAddress(inetAddress, port)
        val daemon = exporterConfig.serverDaemon()

        server = HTTPServer(inetSocketAddress, registry.prometheusRegistry, daemon)
    }

    override fun close() {
        server?.close()
    }

    private class PrometheusMetricExporterConfig(private val config: Map<String, String>) : PrometheusConfig {
        companion object {
            const val SERVER_BIND_DEFAULT = "127.0.0.1:9090"
        }

        override fun get(key: String): String? {
            return config[key]
        }

        fun serverDaemon(): Boolean = getBoolean(this, "daemon").orElse(true)

        fun serverBind(): String = getString(this, "bind").orElse(SERVER_BIND_DEFAULT)

        fun serverPort(): Int {
            val bind = serverBind()
            return bind.substring(bind.indexOf(":") + 1, bind.length).toInt()
        }

        fun serverAddress(): String {
            val bind = serverBind()
            return bind.substring(0, bind.indexOf(":"))
        }
    }
}

object MetricProviderFactory {
    fun create(config: MetricConfig): MetricExporter {
        return when (config.kind) {
            "prometheus" -> PrometheusMetricExporter(config)
            // Add new provider here (Elasticsearch, Datadog, etc)
            else -> throw UnsupportedOperationException("Meter registry \"${config.kind}\" not supported")
        }
    }
}

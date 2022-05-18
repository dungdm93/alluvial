package dev.alluvial.metric.exporters

import io.micrometer.core.instrument.config.validate.PropertyValidator.getBoolean
import io.micrometer.core.instrument.config.validate.PropertyValidator.getString
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import io.prometheus.client.exporter.HTTPServer
import java.net.InetSocketAddress

class PrometheusMetricExporter(config: Map<String, String>) : MetricExporter() {
    private val exporterConfig = PrometheusMetricExporterConfig(config)
    override val registry = PrometheusMeterRegistry(exporterConfig)
    private var server: HTTPServer? = null

    override fun run() {
        val host = exporterConfig.serverHost()
        val port = exporterConfig.serverPort()
        val address = InetSocketAddress(host, port)
        val daemon = exporterConfig.serverDaemon()

        server = HTTPServer(address, registry.prometheusRegistry, daemon)
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
            return bind.substring(bind.indexOf(":") + 1).toInt()
        }

        fun serverHost(): String {
            val bind = serverBind()
            return bind.substring(0, bind.indexOf(":"))
        }
    }
}

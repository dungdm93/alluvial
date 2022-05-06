package dev.alluvial.source.kafka.naming

class MappingSchemaNamingAdjuster : NamingAdjuster {
    override val name: String = "MappingSchema"
    private lateinit var map: Map<String, List<String>>

    @Suppress("UNCHECKED_CAST")
    override fun configure(config: Map<String, Any>) {
        val m = config["map"] as Map<String, String>
        map = m.mapValues { (_, v) ->
            v.split(".")
        }
    }

    override fun adjustNamespace(namespace: List<String>): List<String> {
        val key = namespace.joinToString(".")
        return map.getOrDefault(key, namespace)
    }
}

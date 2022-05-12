package dev.alluvial.source.kafka.naming

import org.apache.iceberg.catalog.TableIdentifier

class ChangingCaseNamingAdjuster : NamingAdjuster {
    enum class CaseFormatter {
        UPPER {
            override fun format(str: String): String = str.uppercase()
        },
        LOWER {
            override fun format(str: String): String = str.lowercase()
        };

        abstract fun format(str: String): String
    }

    override val name: String = "ChangingCase"
    private lateinit var caseFormatter: CaseFormatter

    override fun configure(config: Map<String, Any>) {
        val case = config["case"] as String
        this.caseFormatter = CaseFormatter.valueOf(case.uppercase())
    }

    override fun adjustNamespace(namespace: List<String>): List<String> {
        return namespace.map(caseFormatter::format)
    }

    override fun adjustTable(namespace: List<String>, table: String): String {
        return caseFormatter.format(table)
    }

    override fun adjustColumn(tableId: TableIdentifier, column: String): String {
        return caseFormatter.format(column)
    }
}

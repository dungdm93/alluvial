{{/*
Alluvial TableManager labels
*/}}
{{- define "alluvial.tableManager.labels" -}}
{{ include "alluvial.labels" . }}
app.kubernetes.io/component: table-manager
{{- end -}}

{{/*
Alluvial TableManager selector labels
*/}}
{{- define "alluvial.tableManager.selectorLabels" -}}
{{ include "alluvial.selectorLabels" . }}
app.kubernetes.io/component: table-manager
{{- end -}}

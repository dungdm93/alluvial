{{/*
Alluvial StreamController labels
*/}}
{{- define "alluvial.streamController.labels" -}}
{{ include "alluvial.labels" . }}
app.kubernetes.io/component: stream-controller
{{- end -}}

{{/*
Alluvial StreamController selector labels
*/}}
{{- define "alluvial.streamController.selectorLabels" -}}
{{ include "alluvial.selectorLabels" . }}
app.kubernetes.io/component: stream-controller
{{- end -}}

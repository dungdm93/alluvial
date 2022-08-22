{{/*
Expand the name of the chart.
*/}}
{{- define "alluvial.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "alluvial.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "alluvial.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "alluvial.labels" -}}
helm.sh/chart: {{ include "alluvial.chart" . }}
{{ include "alluvial.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "alluvial.selectorLabels" -}}
app.kubernetes.io/name: {{ include "alluvial.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "alluvial.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "alluvial.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Alluvial image
*/}}
{{- define "alluvial.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{/*
Alluvial volumeMounts
*/}}
{{- define "alluvial.volumeMounts" -}}
- name: alluvial-config
  mountPath: /etc/alluvial/
{{- end -}}

{{/*
Alluvial volumes
*/}}
{{- define "alluvial.volumes" -}}
- name: alluvial-config
  configMap:
    name: {{ include "alluvial.fullname" . }}
{{- end -}}

{{/*
Checksum pod annotations
*/}}
{{- define "alluvial.checksum" -}}
checksum/alluvial-config: {{ include (print $.Template.BasePath "/config-map.yaml") . | sha256sum }}
{{- end -}}

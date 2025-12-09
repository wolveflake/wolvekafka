{{- define "wks-prometheus.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "wks-prometheus.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "wks-prometheus.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "wks-prometheus.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion }}
{{- end -}}

{{- define "wks-prometheus.selectorLabels" -}}
app.kubernetes.io/name: {{ include "wks-prometheus.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "wks-prometheus.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "wks-prometheus.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "wks-prometheus.wksLabels" -}}
{{ include "wks-prometheus.labels" . }}
app.kubernetes.io/component: wks
{{- end -}}

{{- define "wks-prometheus.promLabels" -}}
{{ include "wks-prometheus.labels" . }}
app.kubernetes.io/component: prometheus
{{- end -}}

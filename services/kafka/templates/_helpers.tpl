{{- define "kafka-demo.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kafka-demo.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "kafka-demo.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "kafka-demo.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kafka-demo.kafkaServiceName" -}}
{{- printf "%s-kafka" (include "kafka-demo.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kafka-demo.kafkaHeadlessServiceName" -}}
{{- printf "%s-kafka-headless" (include "kafka-demo.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kafka-demo.zookeeperServiceName" -}}
{{- printf "%s-zookeeper" (include "kafka-demo.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kafka-demo.labels" -}}
helm.sh/chart: {{ include "kafka-demo.chart" . }}
app.kubernetes.io/name: {{ include "kafka-demo.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "kafka-demo.kafkaSelectorLabels" -}}
app.kubernetes.io/component: kafka
{{ include "kafka-demo.labels" . }}
{{- end -}}

{{- define "kafka-demo.zookeeperSelectorLabels" -}}
app.kubernetes.io/component: zookeeper
{{ include "kafka-demo.labels" . }}
{{- end -}}

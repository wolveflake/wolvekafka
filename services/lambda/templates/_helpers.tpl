{{- define "lambda.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "lambda.fullname" -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- $fullname := printf "%s-%s" .Release.Name $name -}}
{{- default $fullname .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "lambda.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "lambda.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion }}
{{- end -}}

{{- define "lambda.selectorLabels" -}}
app.kubernetes.io/name: {{ include "lambda.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "lambda.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "lambda.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

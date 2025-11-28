#!/usr/bin/env bash
set -o pipefail -o errexit -o nounset

readonly CRANE="{{crane_path}}"
readonly JQ="{{jq_path}}"
readonly IMAGE_DIR="{{image_dir}}"
readonly TAGS_FILE="{{tags}}"
readonly FIXED_ARGS=({{fixed_args}})
readonly REPOSITORY_FILE="{{repository_file}}"

REPOSITORY=""
if [ -f $REPOSITORY_FILE ] ; then
  REPOSITORY=$(tr -d '\n' < "$REPOSITORY_FILE")
fi

# set $@ to be FIXED_ARGS+$@
ALL_ARGS=(${FIXED_ARGS[@]+"${FIXED_ARGS[@]}"} $@)
if [[ ${#ALL_ARGS[@]} -gt 0 ]]; then
  set -- ${ALL_ARGS[@]}
fi

TAGS=()

# global crane flags to be passed with every crane invocation
GLOBAL_FLAGS=()

# this will hold args specific to `crane push``
ARGS=()

while (( $# > 0 )); do
  case $1 in
    (--allow-nondistributable-artifacts|--insecure|-v|--verbose)
      GLOBAL_FLAGS+=( "$1" )
      shift;;
    (--platform)
      GLOBAL_FLAGS+=( "--platform" "$2" )
      shift
      shift;;
    (-t|--tag)
      TAGS+=( "$2" )
      shift
      shift;;
    (--tag=*)
      TAGS+=( "${1#--tag=}" )
      shift;;
    (-r|--repository)
      REPOSITORY="$2"
      shift
      shift;;
    (--repository=*)
      REPOSITORY="${1#--repository=}"
      shift;;
    (*)
      ARGS+=( "$1" )
      shift;;
  esac
done

DIGEST=$("${JQ}" -r '.manifests[0].digest' "${IMAGE_DIR}/index.json")

REFS=$(mktemp)
"${CRANE}" push "${GLOBAL_FLAGS[@]+"${GLOBAL_FLAGS[@]}"}" "${IMAGE_DIR}" "${REPOSITORY}@${DIGEST}" "${ARGS[@]+"${ARGS[@]}"}" --image-refs "${REFS}"

push_tag() {
  local tag="${1}"
  if [[ -z "${tag}" ]]; then
    return
  fi
  # DigitalOcean's registry responds with mismatched digests when we try to
  # re-tag by digest via `crane tag`. Re-pushing by tag avoids the manifest
  # lookup that triggers the mismatch and is effectively a no-op once the
  # digest exists server side.
  "${CRANE}" push "${GLOBAL_FLAGS[@]+"${GLOBAL_FLAGS[@]}"}" "${IMAGE_DIR}" "${REPOSITORY}:${tag}" "${ARGS[@]+"${ARGS[@]}"}"
}

for tag in "${TAGS[@]+"${TAGS[@]}"}"
do
  push_tag "${tag}"
done

if [[ -e "${TAGS_FILE:-}" ]]; then
  while IFS= read -r tag || [[ -n "${tag}" ]]; do
    push_tag "${tag}"
  done < "${TAGS_FILE}"
fi

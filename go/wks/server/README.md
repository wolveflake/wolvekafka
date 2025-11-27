# WKS server build & push

Use the Docker-based Linux build environment to get consistent Bazel outputs and to push images to GHCR.

## 1. Build the tool image

```bash
docker build --platform=linux/amd64 -t wolvekafka-build -f Dockerfile.build .
```

## 2. Run Bazel inside the container

```bash
docker run --rm -it --platform=linux/amd64 \
  -v "$(pwd)":/workspace \
  -v wolvekafka-bazel-cache:/root/.cache/bazel \
  -w /workspace \
  wolvekafka-build bash
```

Inside the shell:

```bash
bazel build //go/wks/server:image
```

## 3. Authenticate to GHCR

Use the bundled crane binary in the container. Do **not** mount your macOS Docker config unless you strip out `"credsStore": "desktop"` first.

```bash
crane auth login ghcr.io -u <github-user> -p <personal-access-token>
```

## 4. Push images

```bash
bazel run //go/wks/server:image_push_ghcr_2       # linux/amd64
```

If you prefer macOS to handle the push directly, keep the existing `platform_transition_filegroup` targets and run the same commands outside the container; just ensure `crane auth login` has been run locally.

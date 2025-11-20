load("@aspect_bazel_lib//lib:utils.bzl", "propagate_common_rule_attributes")
load("@rules_oci//oci/private:util.bzl", "util")

def _emit_text_file(name, out, content, attrs):
    cmd = "cat <<'EOF' > $@\n{}\nEOF".format("\n".join(content))
    native.genrule(
        name = name,
        outs = [out],
        cmd = cmd,
        **attrs
    )

def _is_list(value):
    return type(value) == type([])

def _transition_to_target_impl(settings, _attr):
    return {
        "//command_line_option:extra_execution_platforms": [
            str(platform)
            for platform in settings["//command_line_option:platforms"]
        ],
    }

_transition_to_target = transition(
    implementation = _transition_to_target_impl,
    inputs = ["//command_line_option:platforms"],
    outputs = ["//command_line_option:extra_execution_platforms"],
)

def _do_push_impl(ctx):
    crane = ctx.attr._crane[0][platform_common.ToolchainInfo]
    jq = ctx.attr._jq[0][platform_common.ToolchainInfo]

    if ctx.attr.repository and ctx.attr.repository_file:
        fail("must specify exactly one of 'repository_file' or 'repository'")

    if not ctx.file.image.is_directory:
        fail("image attribute must be a oci_image or oci_image_index")

    _, _, _, maybe_digest, maybe_tag = util.parse_image(ctx.attr.repository)
    if maybe_digest or maybe_tag:
        fail("`repository` attribute should not contain digest or tag. got: {}".format(ctx.attr.repository))

    executable = ctx.actions.declare_file("push_%s.sh" % ctx.label.name)
    files = [ctx.file.image]
    substitutions = {
        "{{crane_path}}": crane.crane_info.binary.short_path,
        "{{jq_path}}": jq.jqinfo.bin.short_path,
        "{{image_dir}}": ctx.file.image.short_path,
        "{{fixed_args}}": "",
    }

    if ctx.attr.repository:
        substitutions["{{fixed_args}}"] += " ".join(["\"--repository\"", "\"{}\"".format(ctx.attr.repository)])
    elif ctx.attr.repository_file:
        files.append(ctx.file.repository_file)
        substitutions["{{repository_file}}"] = ctx.file.repository_file.short_path
    else:
        fail("must specify exactly one of 'repository_file' or 'repository'")

    if ctx.attr.remote_tags:
        files.append(ctx.file.remote_tags)
        substitutions["{{tags}}"] = ctx.file.remote_tags.short_path

    ctx.actions.expand_template(
        template = ctx.file.push_template,
        output = executable,
        is_executable = True,
        substitutions = substitutions,
    )
    runfiles = ctx.runfiles(files = files)
    runfiles = runfiles.merge(jq.default.default_runfiles)
    runfiles = runfiles.merge(ctx.attr.image[DefaultInfo].default_runfiles)
    runfiles = runfiles.merge(crane.default.default_runfiles)

    return DefaultInfo(executable = util.maybe_wrap_launcher_for_windows(ctx, executable), runfiles = runfiles)

_ATTRS = {
    "image": attr.label(
        allow_single_file = True,
        doc = "Label to an oci_image or oci_image_index",
        mandatory = True,
    ),
    "repository": attr.string(
        doc = """\
        Repository URL where the image will be signed at, e.g.: `index.docker.io/<user>/image`.
        Digests and tags are not allowed.
        """,
    ),
    "repository_file": attr.label(
        doc = """\
        The same as 'repository' but in a file. This allows pushing to different repositories based on stamping.
        """,
        allow_single_file = True,
    ),
    "remote_tags": attr.label(
        doc = """\
        a text file containing tags, one per line.
        """,
        allow_single_file = True,
    ),
    "push_template": attr.label(
        allow_single_file = True,
        default = ":do_push.sh.tpl",
    ),
    "_allowlist_function_transition": attr.label(
        default = "@bazel_tools//tools/allowlists/function_transition_allowlist",
    ),
    "_crane": attr.label(
        cfg = _transition_to_target,
        default = "@wolvekafka_crane_toolchains//:current_toolchain",
    ),
    "_windows_constraint": attr.label(default = "@platforms//os:windows"),
    "_jq": attr.label(
        cfg = _transition_to_target,
        default = "@jq_toolchains//:resolved_toolchain",
    ),
}

do_push_rule = rule(
    implementation = _do_push_impl,
    attrs = _ATTRS,
    toolchains = ["@bazel_tools//tools/sh:toolchain_type"],
    doc = "Push an OCI image while re-tagging by re-pushing tags to work around registry digest mismatches.",
    executable = True,
)

def oci_push_do(name, remote_tags = None, **kwargs):
    forwarded_kwargs = propagate_common_rule_attributes(kwargs)

    if _is_list(remote_tags):
        tags_label = "{}_write_tags".format(name)
        _emit_text_file(
            name = tags_label,
            out = "{}.tags.txt".format(name),
            content = remote_tags,
            attrs = forwarded_kwargs,
        )
        remote_tags = tags_label

    do_push_rule(
        name = name,
        remote_tags = remote_tags,
        **kwargs
    )

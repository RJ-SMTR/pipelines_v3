# Dataproc Python image

The workflow `.github/workflows/build-and-push-dataproc-image.yaml` publishes
`us-central1-docker.pkg.dev/rj-smtr/dataproc/dbt-python:latest`, used by the dbt
profiles. It runs daily at 06:23 UTC (03:23 in São Paulo), on changes to the image
inputs merged into `master`, and through **Run workflow**.

The default version comes from PyPI's `rio-rac-bus-subsidy` project metadata.
A manual run can specify `package_version`, including a published prerelease.
TestPyPI releases are not considered. A failed or incomplete publication fails
the check and leaves the current image unchanged.

The workflow fingerprints the package version, Dockerfile, Docker ignore file,
requirements and workflow. If the corresponding `build-<hash>` tag exists, it skips the build.
Otherwise it builds for `linux/amd64`, checks dependencies with `pip check`,
checks the package version and imports used by the dbt Python model, and pushes
the image. Only verified images are tagged `latest`. Existing verified images
are reused and promoted to `latest`, including after a manual version override.
The next daily check returns `latest` to PyPI's default version.

Use `force` to rebuild the same inputs, for example to refresh the base image or
unbounded dependencies. Fingerprinted tags identify build inputs, not immutable
image contents when `force` is used. The workflow does not execute Spark or
validate the remuneration calculation.

The existing `GKE_SA_KEY` secret authenticates the build. Its service account
needs Artifact Registry write access to the existing `dataproc` repository in
`rj-smtr`. This workflow does not provision the repository or grant IAM access.
Scheduled runs start once the workflow reaches the default branch. There is no
cross-repository token or required change to the PyPI publishing workflow.

The build context contains only `requirements-dataproc.txt`; repository code
and credentials are not copied into the image. The Dockerfile's default package
version preserves the source branch's pin for standalone builds; the workflow
overrides it with the resolved PyPI version.

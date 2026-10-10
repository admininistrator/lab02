# Kubernetes note

`curate-pod.yaml` is deliberately not included as a deployable file: the Lab 2 handout requires an **instructor-approved image pinned to a real digest**, Lab 1's hardened security context, quota-compliant resources, and an existing `s3-ingestor` Secret. Guessing an image or copying credentials into a manifest would give a broken/insecure Pod.

Once Lab 1 Kubernetes is healthy and the approved image/digest is known, create `k8s/curate-pod.yaml` using the Lab 2 Appendix A Pod example. Mount only `s3-ingestor`, configure `automountServiceAccountToken: false`, `S3_ENDPOINT=http://objects:8333`, writable `/work`, Pod label `access: s3`, CPU request/limit 500m/1 CPU, RAM 512Mi/1Gi. Preserve Lab 1 runAsNonRoot/seccomp/capabilities, never mount owner or analyst credentials in curator Pod.

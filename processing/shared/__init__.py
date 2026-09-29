"""Code imported by BOTH Lambda layers.

Named `shared` rather than `common` on purpose. `spark-submit` puts the submitted
script's own directory first on `sys.path`, so a `processing/common/` package
would shadow the project's top-level `common/` and every job would fail with
`No module named 'common.config'`.


This package is the concrete answer to Lambda's standard criticism -- that two
code paths drift apart. The event schema, the sim-day derivation and (from
Phase 3) the aggregation and billing rules live here once, and both the
streaming job and the batch job import them. What differs between the layers is
only the trigger and the sink, which is exactly what should differ.
"""

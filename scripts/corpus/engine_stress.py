"""The whole corpus through legend-engine as a Bazel test (//scripts/corpus:engine_stress): run.py in each shard, its
outputs into the test's outputs. A file of its own because two executables cannot share a main: :run's and this
test's precompiled mains would be the same output."""
import run

run.main()

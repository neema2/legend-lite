# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""//tools/guards:shrink_only_check's entry: the check itself (shrink_only.py, owned by :shrink_only_lib, so one target
compiles it -- two targets listing the same source made Bazel's precompile write one .pyc twice, a conflict that kept
//gates:local from building; found 2026-11-18). CI runs shrink_only.py directly."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
import shrink_only  # noqa: E402

sys.exit(shrink_only.main(sys.argv[1:]))

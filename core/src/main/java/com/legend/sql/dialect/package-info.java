/**
 * Phase J &mdash; SQL rendering. The ONLY place SQL text is produced: the IR is
 * data-only, calls carry semantic names, and each dialect owns its spellings,
 * quoting, literals, and feature set. An unknown semantic name is a loud error
 * (AGENTS.md invariant 4), never a pass-through.
 */
package com.legend.sql.dialect;

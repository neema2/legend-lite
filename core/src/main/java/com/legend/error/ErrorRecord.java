// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.error;

import com.legend.base.Nullable;

import java.util.List;

/**
 * AN ERROR, AS DATA (stage B's BE, the one error model, 2026-11-21; docs/records/compat-error-model-2026-11-21.md):
 * its {@link ErrorCode}, the values it is about (each as Pure spells it: a name, a type, a count), the driver's
 * {@code [line:col]} where it has one (placed before the whole sentence, frames included, as the driver places it), and
 * the frames it passed through, innermost first. No field holds prose: lite's
 * sentence is {@link #sentence}, from the codes' templates; another audience words the same record its way
 * ({@link #sentence(Words)}).
 */
public record ErrorRecord(ErrorCode code, List<String> values, @Nullable String position, List<Frame> frames) {

    /** A frame the error passed through: its kind and its values. */
    public record Frame(FrameKind kind, List<String> values) {
        public Frame {
            java.util.Objects.requireNonNull(kind, "kind");
            values = List.copyOf(values);
        }
    }

    /** An audience's words of an error's own code, inside its frames: the code and its values. */
    @FunctionalInterface
    public interface Words {
        String of(ErrorCode code, List<String> values);
    }

    public ErrorRecord {
        java.util.Objects.requireNonNull(code, "code");
        if (code instanceof FrameKind) {
            throw new IllegalArgumentException("an error's code is never a frame's: " + code);
        }
        values = List.copyOf(values);
        frames = List.copyOf(frames);
    }

    /** An error of {@code code} over {@code values}, at no position, in no frame. */
    public static ErrorRecord of(ErrorCode code, List<String> values) {
        return new ErrorRecord(code, values, null, List.of());
    }

    /** This error inside one more frame, the outermost. */
    public ErrorRecord framed(Frame frame) {
        List<Frame> out = new java.util.ArrayList<>(frames);
        out.add(frame);
        return new ErrorRecord(code, values, position, out);
    }

    /** Lite's sentence: the code's over its values, inside its frames, after its position. */
    public String sentence() {
        return sentence(ErrorCode::sentence);
    }

    /** The sentence {@code words} gives the error's own code, inside its frames as lite words them (the frames are
     *  lite's context: no upstream has them), after its position. */
    public String sentence(Words words) {
        String out = words.of(code, values);
        for (Frame f : frames) {
            out = f.kind().wrap(f.values(), out);
        }
        return position == null ? out : position + " " + out;
    }

    /** This error placed at {@code where} (the driver's {@code [line:col]}). */
    public ErrorRecord placed(String where) {
        return new ErrorRecord(code, values, where, frames);
    }
}

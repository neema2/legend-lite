// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.Compiler;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.error.NotImplementedException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A SERVICE EXECUTION ENVIRONMENT in a query (2026-11-14, the Studio roundtrip's DSL_ExecutionEnvironment cases):
 * {@code ->from(env->get($key))} types as legend-engine 4.145.0 types it -- the reference an
 * ExecutionEnvironmentInstance, {@code get}/{@code getWithRuntime} answering SingleExecutionParameters, the
 * service's {@code from} keeping the source's type -- so a model holding one compiles; and planning such a query is
 * refused by name at store resolution, because the environment's key is a service parameter the platform never
 * guesses.
 */
class ServiceExecutionEnvironmentTest {

    private static final String MODEL = """
            Class s::Person { name: String[1]; }
            ###Relational
            Database s::DB ( Table T ( NAME VARCHAR(20) PRIMARY KEY ) )
            ###Mapping
            Mapping s::M ( *s::Person: Relational { ~mainTable [s::DB] T name: [s::DB] T.NAME } )
            ###Connection
            RelationalDatabaseConnection s::Conn
            {
                store: s::DB;
                type: H2;
                specification: LocalH2 { };
                auth: DefaultH2;
            }
            ###Runtime
            Runtime s::RT { mappings: [s::M]; connections: [ s::DB: [ c1: s::Conn ] ]; }
            ###Service
            ExecutionEnvironment s::Env
            {
              executions:
              [
                QA:
                {
                  mapping: s::M;
                  runtime: s::RT;
                },
                PROD:
                {
                  mapping: s::M;
                  runtime: s::RT;
                }
              ];
            }
            ###Pure
            function s::byKey(env: String[1]): Any[*]
            {
              s::Person.all()->project([p|$p.name], ['name'])->from(s::Env->get($env))
            }
            """;

    /** The model compiles whole: the environment reference, bare get, and the service's from all type. */
    @Test
    void aModelWhoseQueryReadsAnEnvironmentCompiles() {
        assertEquals(List.of(), Compiler.compileErrors(MODEL));
    }

    /** The reference is an ExecutionEnvironmentInstance and every getter answers SingleExecutionParameters[1]. */
    @Test
    void theEnvironmentAndItsGettersTypeAsUpstreamDeclaresThem() {
        var ctx = Compiler.compileModel(MODEL);
        assertEquals(new Type.ClassType(PlatformTypes.EXECUTION_ENVIRONMENT_INSTANCE),
                Compiler.query(ctx, "|s::Env").resultType().type());
        for (String q : List.of("|s::Env->get('QA')", "|s::Env->get('UAT', 'abc')",
                "|s::Env->getWithRuntime('QA', 'data')")) {
            assertEquals(new Type.ClassType(PlatformTypes.SINGLE_EXECUTION_PARAMETERS),
                    Compiler.query(ctx, q).resultType().type(), q);
        }
    }

    /** Bare get keeps its collection meaning beside the service's: a map's get still resolves. */
    @Test
    void bareGetOnAMapIsStillTheCollectionGet() {
        var ctx = Compiler.compileModel(MODEL);
        assertEquals(Type.Primitive.STRING,
                Compiler.query(ctx, "|newMap([pair(1, 'a')])->get(1)").resultType().type());
    }

    /** Planning a query bound to an environment's key is refused by name: the key is the service's parameter. */
    @Test
    void planningAQueryBoundToAnEnvironmentKeyIsRefusedByName() {
        var ctx = Compiler.compileModel(MODEL);
        for (String q : List.of(
                "|s::Person.all()->from(s::Env->get('QA'))->project(~[name: p|$p.name])",
                "|s::Person.all()->project(~[name: p|$p.name])->from(s::Env->get('QA'))")) {
            var typed = Compiler.query(ctx, q);
            NotImplementedException e = assertThrows(NotImplementedException.class, () -> typed.plan(null), q);
            assertTrue(e.getMessage().contains("from(SingleExecutionParameters)")
                    && e.getMessage().contains("never guesses the key"), e.getMessage());
        }
    }
}

package com.gs.legend.server;
import com.gs.legend.ast.*;
import com.gs.legend.antlr.*;
import com.gs.legend.parser.*;
import com.gs.legend.compiler.*;
import com.gs.legend.model.*;
import com.gs.legend.model.def.*;
import com.gs.legend.model.m3.*;
import com.gs.legend.model.store.*;
import com.gs.legend.model.mapping.*;
import com.gs.legend.plan.*;
import com.gs.legend.exec.*;
import com.gs.legend.serial.*;
import com.gs.legend.sqlgen.*;
import com.gs.legend.service.*;
import com.gs.legend.plan.SingleExecutionPlan;
import com.gs.legend.plan.PlanGenerator;
import com.gs.legend.model.PureModelBuilder;
import com.gs.legend.model.def.ServiceDefinition;

import java.util.HashMap;
import java.util.Map;

/**
 * Stateful metadata service that holds the compiled Pure model.
 *
 * Use cases:
 * - Deploy model definitions (Classes, Mappings, Connections, Runtimes, Services)
 * - Generate and cache plans for known Services
 *
 * <pre>
 * MetadataService service = new MetadataService();
 * service.compile(classesSource);
 * service.compile(serviceSource);
 *
 * SingleExecutionPlan plan = service.generatePlan("app::PersonService", "app::Runtime");
 * </pre>
 */
public class MetadataService {

    private final PureModelBuilder model = new PureModelBuilder();
    private final Map<String, SingleExecutionPlan> planCache = new HashMap<>();

    /**
     * Compiles Pure source and registers definitions in the global model.
     */
    public void compile(String pureSource) {
        model.addSource(pureSource);
    }

    /**
     * Generates an execution plan for a known Service.
     */
    public SingleExecutionPlan generatePlan(String serviceName, String runtimeName) {
        String cacheKey = serviceName + "::" + runtimeName;

        if (planCache.containsKey(cacheKey)) {
            return planCache.get(cacheKey);
        }

        ServiceDefinition service = model.getService(serviceName);
        if (service == null) {
            throw new IllegalArgumentException("Service not found: " + serviceName);
        }

        SingleExecutionPlan plan = PlanGenerator.generate(model, service.functionBody(), runtimeName);
        planCache.put(cacheKey, plan);
        return plan;
    }

    /**
     * Generates an execution plan for an ad-hoc query.
     */
    public SingleExecutionPlan generateQueryPlan(String query, String runtimeName) {
        return PlanGenerator.generate(model, query, runtimeName);
    }
}

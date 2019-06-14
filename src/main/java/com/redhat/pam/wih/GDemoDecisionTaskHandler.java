package com.redhat.pam.wih;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.redhat.dm.dmn.listener.PrometheusMetricsGDListener;

import org.jbpm.process.workitem.bpmn2.AbstractRuleTaskHandler;
import org.kie.api.builder.ReleaseId;
import org.kie.dmn.api.core.DMNContext;
import org.kie.dmn.api.core.DMNMessage.Severity;
import org.kie.dmn.api.core.event.DMNRuntimeEventListener;
import org.kie.dmn.api.core.DMNModel;
import org.kie.dmn.api.core.DMNResult;
import org.kie.dmn.api.core.DMNRuntime;

/**
 * GDemoDecisionRuleTask
 */
public class GDemoDecisionTaskHandler extends AbstractRuleTaskHandler { 

    private final DMNRuntimeEventListener listener;

    public GDemoDecisionTaskHandler(String groupId,
                                   String artifactId,
                                   String version) {
        super(groupId, artifactId, version);
        ReleaseId releaseId = getKieContainer().getReleaseId();
        listener = new PrometheusMetricsGDListener(releaseId.getGroupId(), releaseId.getArtifactId(), releaseId.getVersion());                                    
    }

    public GDemoDecisionTaskHandler(String groupId,
                                   String artifactId,
                                   String version,
                                   long scannerInterval) {
        super(groupId, artifactId, version, scannerInterval);
        ReleaseId releaseId = getKieContainer().getReleaseId();
        listener = new PrometheusMetricsGDListener(releaseId.getGroupId(), releaseId.getArtifactId(), releaseId.getVersion());                                    
    }

    @Override
    public String getRuleLanguage() {
        return DMN_LANG;
    }

    @Override
    protected void handleDMN(Map<String, Object> parameters,
                             Map<String, Object> results) {
        String namespace = (String) parameters.remove("Namespace");
        String model = (String) parameters.remove("Model");
        String decision = (String) parameters.remove("Decision");

        DMNRuntime runtime = getKieContainer().newKieSession().getKieRuntime(DMNRuntime.class);

        //Decorate the runtime with our listener
        runtime.addListener(listener);

        DMNModel dmnModel = runtime.getModel(namespace,
                                             model);
        if (dmnModel == null) {
            throw new IllegalArgumentException("DMN model '" + model + "' not found with namespace '" + namespace + "'");
        }
        DMNResult dmnResult = null;
        DMNContext context = runtime.newContext();

        for (Entry<String, Object> entry : parameters.entrySet()) {
            context.set(entry.getKey(),
                        entry.getValue());
        }

        if (decision != null && !decision.isEmpty()) {
            //dmnResult = runtime.evaluateDecisionByName(dmnModel, decision, context);
            dmnResult = runtime.evaluateByName(dmnModel, context, decision);
        } else {
            dmnResult = runtime.evaluateAll(dmnModel,
                                            context);
        }

        if (dmnResult.hasErrors()) {
            String errors = dmnResult.getMessages(Severity.ERROR).stream()
                    .map(message -> message.toString())
                    .collect(Collectors.joining(", "));

            throw new RuntimeException("DMN result errors:: " + errors);
        }

        results.putAll(dmnResult.getContext().getAll());
    }
    
}
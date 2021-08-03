package com.hubspot.jinjava;

import com.hubspot.jinjava.interpret.*;

import java.util.HashMap;
import java.util.Map;

public class RecursiveJinjava extends Jinjava {
    public RenderResult renderForResult(
            String template,
            Map<String, ?> bindings,
            JinjavaConfig renderConfig
    ) {
        Context context;
        JinjavaInterpreter parentInterpreter = JinjavaInterpreter.getCurrent();
        if (parentInterpreter != null) {
            renderConfig = parentInterpreter.getConfig();
            Map<String, Object> bindingsWithParentContext = new HashMap<>(bindings);
            if (parentInterpreter.getContext() != null) {
                // removed this:  bindingsWithParentContext.putAll(parentInterpreter.getContext());
                for (Map.Entry<String, Object> entry : bindingsWithParentContext.entrySet()) {
                    bindingsWithParentContext.putIfAbsent(entry.getKey(), entry.getValue());
                }

            }
            context =
                    new Context(
                            copyGlobalContext(),
                            bindingsWithParentContext,
                            renderConfig.getDisabled()
                    );
        } else {
            context = new Context(copyGlobalContext(), bindings, renderConfig.getDisabled());
        }

        JinjavaInterpreter interpreter = getGlobalConfig()
                .getInterpreterFactory()
                .newInstance(this, context, renderConfig);
        JinjavaInterpreter.pushCurrent(interpreter);

        try {
            String result = interpreter.render(template);
            return new RenderResult(
                    result,
                    interpreter.getContext(),
                    interpreter.getErrorsCopy()
            );
        } catch (InterpretException e) {
            if (e instanceof TemplateSyntaxException) {
                return new RenderResult(
                        TemplateError.fromException((TemplateSyntaxException) e),
                        interpreter.getContext(),
                        interpreter.getErrorsCopy()
                );
            }
            return new RenderResult(
                    TemplateError.fromSyntaxError(e),
                    interpreter.getContext(),
                    interpreter.getErrorsCopy()
            );
        } catch (InvalidArgumentException e) {
            return new RenderResult(
                    TemplateError.fromInvalidArgumentException(e),
                    interpreter.getContext(),
                    interpreter.getErrorsCopy()
            );
        } catch (InvalidInputException e) {
            return new RenderResult(
                    TemplateError.fromInvalidInputException(e),
                    interpreter.getContext(),
                    interpreter.getErrorsCopy()
            );
        } catch (Exception e) {
            return new RenderResult(
                    TemplateError.fromException(e),
                    interpreter.getContext(),
                    interpreter.getErrorsCopy()
            );
        } finally {
            getGlobalContext().reset();
            JinjavaInterpreter.popCurrent();
        }
    }

    private Context copyGlobalContext() {
        Context globalContext = getGlobalContext();
        Context context = new Context(null, globalContext);
        // copy registered.
        globalContext.getAllExpTests().forEach(context::registerExpTest);
        globalContext.getAllFilters().forEach(context::registerFilter);
        globalContext.getAllFunctions().forEach(context::registerFunction);
        globalContext.getAllTags().forEach(context::registerTag);
        context.setDynamicVariableResolver(globalContext.getDynamicVariableResolver());
        return context;
    }
}

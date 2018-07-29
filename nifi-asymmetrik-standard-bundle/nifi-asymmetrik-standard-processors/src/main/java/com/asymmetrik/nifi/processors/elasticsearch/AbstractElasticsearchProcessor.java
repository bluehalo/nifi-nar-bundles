package com.asymmetrik.nifi.processors.elasticsearch;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StringUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A base class for all Elasticsearch processors
 */
public abstract class AbstractElasticsearchProcessor extends AbstractProcessor {

    public static final PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections. This service only applies if the Elasticsearch endpoint(s) have been secured with TLS/SSL.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    protected static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("Specifies the character set of the document data.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username to access the Elasticsearch cluster")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password to access the Elasticsearch cluster")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected abstract void createElasticsearchClient(ProcessContext context) throws ProcessException;

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();

        // Ensure that if username or password is set, then the other is too
        String userName = validationContext.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        String password = validationContext.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
        if (StringUtils.isEmpty(userName) != StringUtils.isEmpty(password)) {
            results.add(new ValidationResult.Builder().valid(false).explanation(
                    "If username or password is specified, then the other must be specified as well").build());
        }

        return results;
    }

    public void setup(ProcessContext context) {
        // Create the client if one does not already exist
        createElasticsearchClient(context);
    }

}
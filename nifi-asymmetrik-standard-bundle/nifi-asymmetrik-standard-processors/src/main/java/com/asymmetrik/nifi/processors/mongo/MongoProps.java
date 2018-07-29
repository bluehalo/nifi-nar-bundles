package com.asymmetrik.nifi.processors.mongo;

import com.asymmetrik.nifi.services.mongo.MongoClientService;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

public class MongoProps {

    private static final Validator JSON_LIST_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            String reason = null;
            try {
                Object root = JSON.parse(value);
                if (!(root instanceof BasicDBList)) {
                    reason = "not a valid JsonArray";
                }
            } catch (Exception e) {
//                LOGGER.debug("not a valid JSON list", e);
                reason = "unable to parse JSON";
            }
            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    public static final PropertyDescriptor MONGO_SERVICE = new PropertyDescriptor.Builder()
            .name("Mongo Service")
            .description("The instance of Mongo to connect to")
            .identifiesControllerService(MongoClientService.class)
            .required(true)
            .build();
    public static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder().name("Database").description("The database name to be used.").required(true)
            .expressionLanguageSupported(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor GRIDFS_TTL = new PropertyDescriptor.Builder().name("TTL")
            .description("How long should those documents live (in seconds).  A value of 0 stores documents forever").defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor GRIDFS_META_TTL = new PropertyDescriptor.Builder().name("Meta TTL")
            .description("How long should meta documents live (in seconds).  A value of 0 defers to GridFS TTL").defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor GRIDFS_UNIQUE_METADATA_ELEMENTS = new PropertyDescriptor.Builder().name("Unique METADATA Elements")
            .description("CSV of metadata attributes that need to be unique.  Duplicate values are transfered to 'duplicate' relationship")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor GRIDFS_META_ATTRIBUTES = new PropertyDescriptor.Builder().name("metadata")
            .description("CSV of metadata attributes to store.").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor COLLECTION = new PropertyDescriptor.Builder().name("Collection").description("The collection name to be used.").required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor WRITE_CONCERN = new PropertyDescriptor.Builder().name("Write Concern")
            .description("The write concern used for all writes.  It must be one of Acknowledged, Unacknowledged, Journaled, Replica Acknowledged, or Majority.")
            .defaultValue("Acknowledged").allowableValues("Acknowledged", "Unacknowledged", "Journaled", "Replica Acknowledged", "Majority")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Operation Size")
            .description(
                    "The number of operations to collect for a batch operation to MongoDB. The default value is 50, but any number up to 50 may be inserted/updated if fewer than 50 FlowFiles are available on the input queue.")
            .defaultValue("50").addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor ORDERED = new PropertyDescriptor.Builder()
            .name("Ordered Inserts")
            .required(true)
            .description(
                    "Whether order should to be maintained when doing batch inserts. If set to true and an error occurs with one insert, all remaining inserts in the batch will fail.")
            .defaultValue("false").addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();
    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder().name("Query")
            .description("Selection criteria.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor PROJECTION = new PropertyDescriptor.Builder().name("Projection")
            .description("The fields to be returned from the documents in the result set.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor SORT = new PropertyDescriptor.Builder().name("Sort")
            .description("The fields by which to sort.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor LIMIT = new PropertyDescriptor.Builder().name("Limit")
            .description("The maximum number of elements to return.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor UPDATE_OPERATOR = new PropertyDescriptor.Builder().name("Update Type")
            .description("The update operator to use.  Examples include '$set', '$unset', '$push'.")
            .defaultValue("$set")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor UPDATE_QUERY_KEYS = new PropertyDescriptor.Builder().name("Update Query Keys")
            .description("CSV of keys used to build update query")
            .defaultValue("_id")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor UPDATE_KEYS = new PropertyDescriptor.Builder().name("Keys to Update")
            .description("CSV of keys to update.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor UPSERT = new PropertyDescriptor.Builder().name("Upsert")
            .description("Whether to upsert (i.e. insert if update query has no match).")
            .required(true).defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();
    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("Secondary Index")
            .description("Defines a secondary Mongo index, if required. The value of this property must be a JSON array where the first element is a JSON object defining the index properties and the second, optional, element is a JSON object with properties to use for the index.")
            .addValidator(JSON_LIST_VALIDATOR)
            .build();
    // A template for a dynamic index property. You should add a name before returning it.
    public static final PropertyDescriptor.Builder DYNAMIC_INDEX = new PropertyDescriptor.Builder()
            .description("Defines a tertiary Mongo index, if required. The value of this property must be a JSON array where the first element is a JSON object defining the index properties and the second, optional, element is a JSON object with properties to use for the index.")
            .addValidator(JSON_LIST_VALIDATOR)
            .dynamic(true);
    //    private static Logger LOGGER = LoggerFactory.getLogger(MongoProps.class);
    public static final Validator JSON_OBJECT_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            String reason = null;
            try {
                Object root = JSON.parse(value);
                if (!(root instanceof BasicDBObject)) {
                    reason = "not a valid JSON object";
                }
            } catch (Exception e) {
//                LOGGER.debug("not a valid JSON object", e);
                reason = "unable to parse JSON";
            }
            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    private MongoProps() {
    }

}

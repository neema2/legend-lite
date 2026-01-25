// =============================================================================
// PureParser.g4 - Merged Pure Grammar Parser
// =============================================================================
// This file merges the following legend-engine grammar files:
//   - CoreParserGrammar.g4
//   - M3ParserGrammar.g4
//   - DomainParserGrammar.g4
//   - MappingParserGrammar.g4
//   - RuntimeParserGrammar.g4
//   - ConnectionParserGrammar.g4
//   - RelationalParserGrammar.g4
//   - RelationalDatabaseConnectionParserGrammar.g4
//
// Raw source files are in: src/main/antlr4-raw/
// =============================================================================

parser grammar PureParser;

options
{
    tokenVocab = PureLexer;
}


// =============================================================================
// IDENTIFIER (merged from all grammars)
// =============================================================================

identifier:                                     VALID_STRING | STRING
                                                // M3 keywords
                                                | ALL | LET | ALL_VERSIONS | ALL_VERSIONS_IN_RANGE
                                                | TO_BYTES_FUNCTION
                                                // Domain keywords
                                                | IMPORT
                                                | CLASS | FUNCTION | PROFILE | ASSOCIATION | ENUM | MEASURE
                                                | EXTENDS
                                                | STEREOTYPES | TAGS
                                                | NATIVE | PROJECTS | AS
                                                | CONSTRAINT_ENFORCEMENT_LEVEL_ERROR | CONSTRAINT_ENFORCEMENT_LEVEL_WARN
                                                | AGGREGATION_TYPE_COMPOSITE | AGGREGATION_TYPE_SHARED | AGGREGATION_TYPE_NONE
                                                // Mapping keywords
                                                | MAPPING | INCLUDE | TESTS
                                                // Mapping testable keywords (can be used as package names)
                                                | MAPPING_TESTABLE_DOC | MAPPING_TESTABLE_DATA | MAPPING_TESTABLE_ASSERT
                                                | MAPPING_TESTABLE_SUITES | MAPPING_TEST_ASSERTS | MAPPING_TESTS | MAPPING_TESTS_QUERY
                                                // Runtime keywords
                                                | RUNTIME | SINGLE_CONNECTION_RUNTIME | MAPPINGS | CONNECTIONS | CONNECTION | CONNECTIONSTORES
                                                // Relational keywords
                                                | DATABASE | TABLE | SCHEMA | VIEW | TABULAR_FUNC | FILTER | MULTIGRAIN_FILTER | JOIN
                                                | RELATIONAL_AND | RELATIONAL_OR
                                                | MILESTONING | BUSINESS_MILESTONING | BUSINESS_MILESTONING_FROM | BUSINESS_MILESTONING_THRU
                                                | THRU_IS_INCLUSIVE | BUS_SNAPSHOT_DATE
                                                | PROCESSING_MILESTONING | PROCESSING_MILESTONING_IN | PROCESSING_MILESTONING_OUT
                                                | OUT_IS_INCLUSIVE | INFINITY_DATE | PROCESSING_SNAPSHOT_DATE
                                                | ASSOCIATION_MAPPING | ENUMERATION_MAPPING | OTHERWISE | INLINE | BINDING | SCOPE
                                                // Connection keywords
                                                | STORE | TYPE | MODE | RELATIONAL_DATASOURCE_SPEC | RELATIONAL_AUTH_STRATEGY
                                                | DB_TIMEZONE | QUOTE_IDENTIFIERS | QUERY_GENERATION_CONFIGS
;


// =============================================================================
// CORE RULES (from CoreParserGrammar)
// =============================================================================

qualifiedName:                                  (packagePath PATH_SEPARATOR)? identifier
;
packagePath:                                    identifier (PATH_SEPARATOR identifier)*
;

word:                                           identifier | BOOLEAN | INTEGER
;

islandDefinition:                               ISLAND_OPEN islandContent
;

islandContent:                                  (ISLAND_START | ISLAND_BRACE_OPEN | ISLAND_CONTENT | ISLAND_HASH | ISLAND_BRACE_CLOSE | ISLAND_END)*
;


// =============================================================================
// M3 EXPRESSION RULES (from M3ParserGrammar)
// =============================================================================

nonArrowOrEqualExpression :
                                                (
                                                    atomicExpression
                                                    | notExpression
                                                    | signedExpression
                                                    | expressionsArray
                                                    | (PAREN_OPEN combinedExpression PAREN_CLOSE)
                                                )
;

expression:                                     (
                                                    nonArrowOrEqualExpression
                                                    (
                                                        (propertyOrFunctionExpression)*
                                                        (equalNotEqual)?
                                                    )
                                                )
;


instance:                                       NEW_SYMBOL qualifiedName (LESS_THAN typeArguments? (PIPE multiplicityArguments)? GREATER_THAN)? identifier?
                                                (FILE_NAME COLON INTEGER COMMA INTEGER COMMA INTEGER COMMA INTEGER COMMA INTEGER COMMA INTEGER FILE_NAME_END)? (AT qualifiedName)?
                                                    PAREN_OPEN
                                                        (instancePropertyAssignment (COMMA instancePropertyAssignment)*)?
                                                    PAREN_CLOSE
;
unitInstance:                                   unitInstanceLiteral unitName
;
unitName:                                       qualifiedName TILDE identifier
;
instancePropertyAssignment:                     identifier EQUAL instanceRightSide
;
instanceRightSide:                              instanceAtomicRightSideScalar | instanceAtomicRightSideVector
;
instanceAtomicRightSideScalar:                  instanceAtomicRightSide
;
instanceAtomicRightSideVector:                  BRACKET_OPEN (instanceAtomicRightSide (COMMA instanceAtomicRightSide)* )? BRACKET_CLOSE
;
instanceAtomicRightSide:                        instanceLiteral
                                                | LATEST_DATE
                                                | instance
                                                | qualifiedName
                                                | enumReference
                                                | stereotypeReference
                                                | tagReference
                                                | identifier
;
enumReference:                                  qualifiedName  DOT identifier
;
stereotypeReference:                            qualifiedName AT identifier
;
tagReference:                                   qualifiedName PERCENT identifier
;
propertyReturnType:                             type multiplicity
;
codeBlock:                                      programLine (SEMI_COLON (programLine SEMI_COLON)*)?
;
programLine:                                    combinedExpression | letExpression
;
equalNotEqual:                                  (TEST_EQUAL | TEST_NOT_EQUAL) combinedArithmeticOnly
;
combinedArithmeticOnly:                         expression arithmeticPart*
;
// comparisonExpression handles arithmetic and comparison ops (higher precedence)
comparisonExpression:                           expression arithmeticPart*
;
// expressionPart was previously booleanPart | arithmeticPart - now only booleanPart since comparison moved
expressionPart:                                 booleanPart
;
letExpression:                                  LET identifier EQUAL combinedExpression
;
// combinedExpression now chains comparison expressions with boolean ops (lower precedence)
combinedExpression:                             comparisonExpression booleanPart*
;
expressionsArray:                               BRACKET_OPEN ( expression (COMMA expression)* )? BRACKET_CLOSE
;
propertyOrFunctionExpression:                   propertyExpression | functionExpression | propertyBracketExpression
;
propertyExpression:                             DOT identifier (functionExpressionLatestMilestoningDateParameter | functionExpressionParameters)?
;
propertyBracketExpression:                      BRACKET_OPEN (STRING | INTEGER)  BRACKET_CLOSE
;
functionExpression:                             ARROW qualifiedName functionExpressionParameters (ARROW qualifiedName functionExpressionParameters)*
;
functionExpressionLatestMilestoningDateParameter:
                                                PAREN_OPEN LATEST_DATE (COMMA LATEST_DATE)? PAREN_CLOSE
;
functionExpressionParameters:                   PAREN_OPEN (combinedExpression (COMMA combinedExpression)*)? PAREN_CLOSE
;
atomicExpression:                               dsl
                                                | instanceLiteralToken
                                                | expressionInstance
                                                | unitInstance
                                                | variable
                                                | columnBuilders
                                                | (AT type)
                                                | anyLambda
                                                | instanceReference
;

columnBuilders: TILDE (oneColSpec | colSpecArray)
;
oneColSpec: identifier ((COLON (type multiplicity? | anyLambda | combinedExpression) extraFunction? ))?
;
colSpecArray: (BRACKET_OPEN (oneColSpec(COMMA oneColSpec)*)? BRACKET_CLOSE)
;
extraFunction: (COLON anyLambda)
;

anyLambda : lambdaPipe | lambdaFunction | lambdaParam lambdaPipe
;

instanceReference:                              (PATH_SEPARATOR | qualifiedName | unitName) allOrFunction?
;
lambdaFunction:                                 BRACE_OPEN (lambdaParam (COMMA lambdaParam)* )? lambdaPipe BRACE_CLOSE
;
variable:                                       DOLLAR identifier
;
allOrFunction:                                  allFunction
                                                | allVersionsFunction
                                                | allVersionsInRangeFunction
                                                | allFunctionWithMilestoning
                                                | functionExpressionParameters
;
allFunction:                                    DOT ALL PAREN_OPEN PAREN_CLOSE
;
allVersionsFunction:                            DOT ALL_VERSIONS PAREN_OPEN PAREN_CLOSE
;
allVersionsInRangeFunction:                     DOT ALL_VERSIONS_IN_RANGE PAREN_OPEN buildMilestoningVariableExpression COMMA buildMilestoningVariableExpression PAREN_CLOSE
;
allFunctionWithMilestoning:                     DOT ALL PAREN_OPEN buildMilestoningVariableExpression (COMMA buildMilestoningVariableExpression)? PAREN_CLOSE
;
buildMilestoningVariableExpression:             LATEST_DATE | DATE | variable
;
expressionInstance:                             NEW_SYMBOL (variable | qualifiedName)
                                                (LESS_THAN typeArguments? (PIPE multiplicityArguments)? GREATER_THAN)? (identifier)?
                                                (typeVariableValues)?
                                                PAREN_OPEN
                                                    expressionInstanceParserPropertyAssignment? (COMMA expressionInstanceParserPropertyAssignment)*
                                                PAREN_CLOSE
;
expressionInstanceRightSide:                    expressionInstanceAtomicRightSide
;
expressionInstanceAtomicRightSide:              combinedExpression | expressionInstance | qualifiedName
;
expressionInstanceParserPropertyAssignment:     identifier (DOT identifier)* PLUS? EQUAL expressionInstanceRightSide
;
notExpression:                                  NOT expression
;
signedExpression:                               (MINUS | PLUS) expression
;
lambdaPipe:                                     PIPE codeBlock
;
lambdaParam:                                    identifier lambdaParamType?
;
lambdaParamType:                                COLON type multiplicity
;
primitiveValue:                                 primitiveValueAtomic | primitiveValueVector
;
primitiveValueVector:                           BRACKET_OPEN (primitiveValueAtomic (COMMA primitiveValueAtomic)* )? BRACKET_CLOSE
;
primitiveValueAtomic:                           instanceLiteral | toBytesLiteral | enumReference
;
instanceLiteral:                                instanceLiteralToken | (MINUS INTEGER) | (MINUS FLOAT) | (MINUS DECIMAL) | (PLUS INTEGER) | (PLUS FLOAT) | (PLUS DECIMAL)
;
instanceLiteralToken:                           STRING | INTEGER | FLOAT | DECIMAL | DATE | BOOLEAN | STRICTTIME
;
toBytesLiteral:                                 TO_BYTES_FUNCTION PAREN_OPEN STRING PAREN_CLOSE
;
unitInstanceLiteral:                            (MINUS? INTEGER) | (MINUS? FLOAT) | (MINUS? DECIMAL) | (PLUS INTEGER) | (PLUS FLOAT) | (PLUS DECIMAL)
;
arithmeticPart:                                 (
                                                    PLUS expression (PLUS expression)*
                                                    | (STAR expression (STAR expression)*)
                                                    | (MINUS expression (MINUS expression)*)
                                                    | (DIVIDE expression (DIVIDE expression)*)
                                                    | (LESS_THAN expression)
                                                    | (LESS_OR_EQUAL expression)
                                                    | (GREATER_THAN expression)
                                                    | (GREATER_OR_EQUAL expression)
                                                )
;
booleanPart:                                    (AND comparisonExpression) | (OR  comparisonExpression)
;
functionVariableExpression:                     identifier COLON type multiplicity
;
dsl:                                            dslExtension | dslNavigationPath
;
dslNavigationPath:                              NAVIGATION_PATH_BLOCK
;
dslExtension:                                   ISLAND_OPEN (dslExtensionContent)*
;
dslExtensionContent:                            ISLAND_START | ISLAND_BRACE_OPEN | ISLAND_CONTENT | ISLAND_HASH | ISLAND_BRACE_CLOSE | ISLAND_END
;

// =============================================================================
// GRAPHFETCH TREE (from GraphFetchTreeParserGrammar)
// =============================================================================

graphFetchTree:                                 qualifiedName graphDefinition
;
graphDefinition:                                BRACE_OPEN
                                                    graphPaths
                                                BRACE_CLOSE
;
graphPaths:                                     (graphPath | subTypeGraphPath) (COMMA (graphPath | subTypeGraphPath))*
;
graphPath:                                      graphAlias? identifier propertyParameters? subtype? graphDefinition?
;
subTypeGraphPath:                               subtype graphDefinition
;
graphAlias:                                     STRING COLON
;
// Property parameters for milestoned properties
propertyParameters:                             PAREN_OPEN
                                                    (graphFetchParameter (COMMA graphFetchParameter)*)?
                                                PAREN_CLOSE
;
graphFetchParameter:                            LATEST_DATE | instanceLiteral | variable | enumReference
;
subtype:                                        SUBTYPE_START qualifiedName PAREN_CLOSE
;

// =============================================================================
// RELATION LITERAL (for #>{store::DB.TABLE})
// =============================================================================

relationLiteral:                                qualifiedName DOT identifier
;
type:                                           (qualifiedName (LESS_THAN typeArguments? (PIPE multiplicityArguments)? GREATER_THAN)?) typeVariableValues?
                                                |
                                                (
                                                    BRACE_OPEN
                                                        functionTypePureType? (COMMA functionTypePureType)*
                                                        ARROW type multiplicity
                                                    BRACE_CLOSE
                                                )
                                                |
                                                    relationType
                                                |
                                                unitName
;


relationType :  PAREN_OPEN
                   columnInfo (COMMA columnInfo)*
                PAREN_CLOSE
;

columnInfo: columnName COLON type multiplicity?
;

columnName: identifier
;

typeVariableValues: PAREN_OPEN (instanceLiteral (COMMA instanceLiteral)*)? PAREN_CLOSE
;

functionTypePureType:                           type multiplicity
;
typeAndMultiplicityParameters:                  LESS_THAN ((typeParameters multiplictyParameters?) | multiplictyParameters) GREATER_THAN
;
typeParametersWithContravarianceAndMultiplicityParameters:
                                                LESS_THAN ((contravarianceTypeParameters multiplictyParameters?) | multiplictyParameters) GREATER_THAN
;
typeParameters:                                 typeParameter (COMMA typeParameter)*
;
typeParameter:                                  identifier
;
contravarianceTypeParameters:                   contravarianceTypeParameter (COMMA contravarianceTypeParameter)*
;
contravarianceTypeParameter:                    MINUS? identifier
;
multiplicityArguments:                          multiplicityArgument (COMMA multiplicityArgument)*
;
typeArguments:                                  type (COMMA type)*
;
multiplictyParameters:                          PIPE identifier (COMMA identifier)*
;




multiplicity:                                   BRACKET_OPEN multiplicityArgument BRACKET_CLOSE
;
multiplicityArgument:                           identifier | ((fromMultiplicity DOT_DOT)? toMultiplicity)
;
fromMultiplicity:                               INTEGER
;
toMultiplicity:                                 INTEGER | STAR
;



functionIdentifier:                             qualifiedName PAREN_OPEN (functionTypePureType (COMMA functionTypePureType)*)? PAREN_CLOSE COLON functionTypePureType
;


// =============================================================================
// DOMAIN DEFINITION RULES (from DomainParserGrammar)
// =============================================================================

definition:                                     imports
                                                    elementDefinition*
                                                EOF
;
imports:                                        importStatement*
;
importStatement:                                IMPORT packagePath PATH_SEPARATOR STAR SEMI_COLON
;
elementDefinition:                              (
                                                    profile
                                                    | classDefinition
                                                    | association
                                                    | enumDefinition
                                                    | nativeFunction
                                                    | functionDefinition
                                                    | instance
                                                    | measureDefinition
                                                    | mapping
                                                    | runtime
                                                    | singleConnectionRuntime
                                                    | database
                                                    | relationalDatabaseConnection
                                                )
;


// -------------------------------------- SHARED --------------------------------------

stereotypes:                                    LESS_THAN LESS_THAN stereotype (COMMA stereotype)* GREATER_THAN GREATER_THAN
;
stereotype:                                     qualifiedName DOT identifier
;
taggedValues:                                   BRACE_OPEN taggedValue (COMMA taggedValue)* BRACE_CLOSE
;
taggedValue:                                    qualifiedName DOT identifier EQUAL STRING
;


// -------------------------------------- CLASS --------------------------------------

classDefinition:                                CLASS stereotypes? taggedValues? qualifiedName typeParametersWithContravarianceAndMultiplicityParameters?
                                                (
                                                    (PROJECTS projection)
                                                    |
                                                    (
                                                        (EXTENDS type (COMMA type)*)?
                                                        constraints?
                                                        classBody
                                                    )
                                                )
;
classBody:                                      BRACE_OPEN
                                                    properties
                                                BRACE_CLOSE
;
properties:                                     (property | qualifiedProperty)*
;
property:                                       stereotypes? taggedValues? aggregation? identifier COLON propertyReturnType defaultValue? SEMI_COLON
;
qualifiedProperty:                              stereotypes? taggedValues? identifier qualifiedPropertyBody COLON propertyReturnType  SEMI_COLON
;
qualifiedPropertyBody:                          PAREN_OPEN (functionVariableExpression (COMMA functionVariableExpression)*)? PAREN_CLOSE
                                                    BRACE_OPEN codeBlock BRACE_CLOSE
;
aggregation:                                    PAREN_OPEN aggregationType PAREN_CLOSE
;
aggregationType:                                AGGREGATION_TYPE_COMPOSITE | AGGREGATION_TYPE_SHARED | AGGREGATION_TYPE_NONE
;

defaultValue: EQUAL defaultValueExpression
;

defaultValueExpression: (instanceReference)(propertyExpression) | expressionInstance | instanceLiteralToken | defaultValueExpressionsArray
;

defaultValueExpressionsArray: BRACKET_OPEN ( defaultValueExpression (COMMA defaultValueExpression)* )? BRACKET_CLOSE
;


// -------------------------------------- ASSOCIATION --------------------------------------

association:                                    ASSOCIATION stereotypes? taggedValues? qualifiedName
                                                    (associationProjection | associationBody)
;
associationBody:                                BRACE_OPEN properties BRACE_CLOSE
;
associationProjection:                          PROJECTS qualifiedName LESS_THAN qualifiedName COMMA qualifiedName GREATER_THAN
;


// -------------------------------------- PROFILE --------------------------------------

profile:                                        PROFILE qualifiedName
                                                    BRACE_OPEN
                                                        (
                                                            stereotypeDefinitions
                                                            | tagDefinitions
                                                        )*
                                                    BRACE_CLOSE
;
stereotypeDefinitions:                          (STEREOTYPES COLON BRACKET_OPEN (identifier (COMMA identifier)*)? BRACKET_CLOSE SEMI_COLON)
;
tagDefinitions:                                 (TAGS COLON BRACKET_OPEN (identifier (COMMA identifier)*)? BRACKET_CLOSE SEMI_COLON)
;


// -------------------------------------- ENUM --------------------------------------

enumDefinition:                                 ENUM stereotypes? taggedValues? qualifiedName
                                                    BRACE_OPEN
                                                        (enumValue (COMMA enumValue)*)?
                                                    BRACE_CLOSE
;
enumValue:                                      stereotypes? taggedValues? identifier
;


// -------------------------------------- MEASURE --------------------------------------

measureDefinition:                              MEASURE qualifiedName
                                                    measureBody
;
measureBody:                                    BRACE_OPEN
                                                    (
                                                        (measureExpr* canonicalExpr measureExpr*)
                                                        | nonConvertibleMeasureExpr+
                                                    )
                                                BRACE_CLOSE
;
canonicalExpr:                                  STAR measureExpr
;
measureExpr:                                    qualifiedName COLON unitExpr
;
nonConvertibleMeasureExpr:                      qualifiedName SEMI_COLON
;
unitExpr:                                       identifier ARROW codeBlock
;


// -------------------------------------- FUNCTION --------------------------------------

nativeFunction:                                 NATIVE FUNCTION qualifiedName typeAndMultiplicityParameters? functionTypeSignature SEMI_COLON
;
functionTypeSignature:                          PAREN_OPEN (functionVariableExpression (COMMA functionVariableExpression)*)? PAREN_CLOSE COLON type multiplicity
;
functionDefinition:                             FUNCTION stereotypes? taggedValues? qualifiedName typeAndMultiplicityParameters? functionTypeSignature
                                                constraints?
                                                    BRACE_OPEN
                                                        codeBlock
                                                    BRACE_CLOSE
                                                functionTestSuiteDef?
;
functionTestSuiteDef:                           BRACE_OPEN
                                                    (simpleFunctionTest | simpleFunctionSuite | functionData)*
                                                BRACE_CLOSE
;
simpleFunctionSuite:                            identifier
                                                PAREN_OPEN
                                                    (functionData)*
                                                    simpleFunctionTest (simpleFunctionTest)*
                                                PAREN_CLOSE
;
functionData:                                   elementPointer COLON functionDataValue SEMI_COLON
;
elementPointer:                                 (packageableElementPointerType)? packageableElementPointer
;
packageableElementPointerType:                  PAREN_OPEN VALID_STRING PAREN_CLOSE
;
packageableElementPointer:                      qualifiedName
;
functionDataValue:                              (qualifiedName | externalFormatValue | embeddedData)
;
simpleFunctionTest:                             identifier (STRING)? PIPE identifier PAREN_OPEN functionParams PAREN_CLOSE EQUAL GREATER_THAN (externalFormatValue | primitiveValue) SEMI_COLON
;
externalFormatValue:                            contentType STRING
;
contentType:                                    PAREN_OPEN identifier PAREN_CLOSE
;
functionParams:                                 (primitiveValue (COMMA primitiveValue)*)?
;
embeddedData:                                   identifier ISLAND_OPEN (embeddedDataContent)*
;
embeddedDataContent:                            ISLAND_START | ISLAND_BRACE_OPEN | ISLAND_CONTENT | ISLAND_HASH | ISLAND_BRACE_CLOSE | ISLAND_END
;

// -------------------------------------- CONSTRAINT --------------------------------------

constraints:                                    BRACKET_OPEN
                                                    constraint (COMMA constraint)*
                                                BRACKET_CLOSE
;
constraint:                                     simpleConstraint | complexConstraint
;
simpleConstraint:                               constraintId? combinedExpression
;
complexConstraint:                              identifier
                                                    PAREN_OPEN
                                                        constraintOwner?
                                                        constraintExternalId?
                                                        constraintFunction
                                                        constraintEnforcementLevel?
                                                        constraintMessage?
                                                    PAREN_CLOSE
;
constraintOwner:                                CONSTRAINT_OWNER COLON identifier
;
constraintExternalId:                           CONSTRAINT_EXTERNAL_ID COLON STRING
;
constraintFunction:                             CONSTRAINT_FUNCTION COLON combinedExpression
;
constraintEnforcementLevel:                     CONSTRAINT_ENFORCEMENT COLON constraintEnforcementLevelType
;
constraintEnforcementLevelType:                 CONSTRAINT_ENFORCEMENT_LEVEL_ERROR | CONSTRAINT_ENFORCEMENT_LEVEL_WARN
;
constraintMessage:                              CONSTRAINT_MESSAGE COLON combinedExpression
;
constraintId:                                   identifier COLON
;


// -------------------------------------- PROJECTION --------------------------------------

projection:                                     dsl | treePath
;
treePath:                                       type alias? stereotypes? taggedValues? treePathClassBody
;
treePathClassBody:                              BRACE_OPEN
                                                    simplePropertyFilter?
                                                    (derivedProperty | complexProperty)*
                                                BRACE_CLOSE
;
alias:                                          AS identifier
;
simplePropertyFilter:                           STAR | ((PLUS | MINUS) (BRACKET_OPEN simpleProperty (COMMA simpleProperty)* BRACKET_CLOSE))
;
simpleProperty:                                 propertyRef stereotypes? taggedValues?
;
complexProperty:                                propertyRef alias? stereotypes? taggedValues? treePathClassBody?
;
derivedProperty:                                GREATER_THAN propertyRef BRACKET_OPEN codeBlock BRACKET_CLOSE alias? stereotypes? taggedValues? treePathClassBody?
;
propertyRef:                                    identifier (PAREN_OPEN (treePathPropertyParameterType (COMMA treePathPropertyParameterType)*)? PAREN_CLOSE)*
;
treePathPropertyParameterType:                  type multiplicity
;


// =============================================================================
// MAPPING RULES (from MappingParserGrammar)
// =============================================================================

mapping:                                        MAPPING qualifiedName
                                                    PAREN_OPEN
                                                        (includeMapping)*
                                                        (mappingElement)*
                                                    PAREN_CLOSE
;
includeMapping:                                 INCLUDE qualifiedName
                                                    (BRACKET_OPEN (storeSubPath (COMMA storeSubPath)*)? BRACKET_CLOSE)?
;
storeSubPath:                                   sourceStore ARROW targetStore
;
sourceStore:                                    qualifiedName
;
targetStore:                                    qualifiedName
;  
mappingElement:                                 (STAR)? qualifiedName
                                                    (BRACKET_OPEN mappingElementId BRACKET_CLOSE)?
                                                    (EXTENDS BRACKET_OPEN superClassMappingId BRACKET_CLOSE)?
                                                COLON mappingParserName (mappingElementName)? mappingElementBody
;
mappingElementBody:                             BRACE_OPEN mappingElementBodyContent* BRACE_CLOSE
;
mappingElementBodyContent:                      ~(BRACE_OPEN | BRACE_CLOSE) | BRACE_OPEN mappingElementBodyContent* BRACE_CLOSE
;
mappingElementName:                             word
;
mappingParserName:                              identifier
;
superClassMappingId:                            mappingElementId
;
mappingElementId:                               word
;


// =============================================================================
// RUNTIME RULES (from RuntimeParserGrammar)
// =============================================================================

runtime:                                        RUNTIME qualifiedName
                                                    BRACE_OPEN
                                                        (runtimeMappings | runtimeConnections | connectionStoresList)*
                                                    BRACE_CLOSE
;
singleConnectionRuntime:                        SINGLE_CONNECTION_RUNTIME qualifiedName
                                                    BRACE_OPEN
                                                        (runtimeMappings|singleConnection)*
                                                    BRACE_CLOSE
;
runtimeMappings:                                MAPPINGS COLON
                                                    BRACKET_OPEN
                                                        (qualifiedName (COMMA qualifiedName)*)?
                                                    BRACKET_CLOSE SEMI_COLON
;
runtimeConnections:                             CONNECTIONS COLON
                                                    BRACKET_OPEN
                                                        (storeConnections (COMMA storeConnections)*)?
                                                    BRACKET_CLOSE SEMI_COLON
;
singleConnection:                               CONNECTION COLON packageableElementPointer SEMI_COLON
;
connectionStoresList:                           CONNECTIONSTORES COLON
                                                    BRACKET_OPEN
                                                        (connectionStores (COMMA connectionStores)*)?
                                                    BRACKET_CLOSE SEMI_COLON
;
storeConnections:                               qualifiedName COLON
                                                    BRACKET_OPEN
                                                        (identifiedConnection (COMMA identifiedConnection)*)?
                                                    BRACKET_CLOSE
;
connectionStores:                               connectionRef COLON
                                                    BRACKET_OPEN
                                                        (elementPointer (COMMA elementPointer)*)?
                                                    BRACKET_CLOSE
;
identifiedConnection:                           identifier COLON (packageableElementPointer | embeddedConnection)
;
connectionRef:                                  packageableElementPointer
;
embeddedConnection:                             ISLAND_OPEN (embeddedConnectionContent)*
;
embeddedConnectionContent:                      ISLAND_START | ISLAND_BRACE_OPEN | ISLAND_CONTENT | ISLAND_HASH | ISLAND_BRACE_CLOSE | ISLAND_END
;


// =============================================================================
// DATABASE RULES (from RelationalParserGrammar)
// =============================================================================

database:                                       DATABASE stereotypes? taggedValues? qualifiedName
                                                    PAREN_OPEN
                                                        (includeDatabase)*
                                                        (
                                                            dbSchema
                                                            | dbTable
                                                            | dbView
                                                            | dbJoin
                                                            | dbTabularFunction
                                                            | dbFilter
                                                            | dbMultiGrainFilter
                                                        )*
                                                    PAREN_CLOSE
;
includeDatabase:                                INCLUDE qualifiedName
;
dbSchema:                                       SCHEMA stereotypes? taggedValues? schemaIdentifier
                                                    PAREN_OPEN
                                                        (
                                                            dbTable
                                                            | dbView
                                                            | dbTabularFunction
                                                        )*
                                                    PAREN_CLOSE
;
dbTable:                                        TABLE stereotypes? taggedValues? relationalIdentifier
                                                    PAREN_OPEN
                                                        milestoneSpec?
                                                        (columnDefinition (COMMA columnDefinition)*)?
                                                    PAREN_CLOSE
;
columnDefinition:                               relationalIdentifier stereotypes? taggedValues? identifier (PAREN_OPEN INTEGER (COMMA INTEGER)? PAREN_CLOSE)? (PRIMARY_KEY | NOT_NULL)?
;
milestoneSpec:                                  MILESTONING
                                                    PAREN_OPEN
                                                        (milestoningDef (COMMA milestoningDef)*)?
                                                    PAREN_CLOSE
;
milestoningDef:                                 milestoningType
                                                    PAREN_OPEN
                                                        milestoningSpecification
                                                    PAREN_CLOSE
;
milestoningType:                                identifier
;
milestoningSpecification:                       (relationalIdentifier | COMMA | BOOLEAN | DATE | EQUAL)*
;
dbView:                                         VIEW stereotypes? taggedValues? relationalIdentifier
                                                    PAREN_OPEN
                                                        (viewFilterMapping)?
                                                        (viewGroupBy)?
                                                        (DISTINCT_CMD)?
                                                        (viewColumnMapping (COMMA viewColumnMapping)*)?
                                                    PAREN_CLOSE
;
viewFilterMapping:                              FILTER_CMD (viewFilterMappingJoin | databasePointer)? identifier
;
viewFilterMappingJoin:                          databasePointer joinSequence PIPE databasePointer
;
viewGroupBy:                                    GROUP_BY_CMD
                                                    PAREN_OPEN
                                                        (dbOperation (COMMA dbOperation)*)?
                                                    PAREN_CLOSE
;
viewColumnMapping:                              identifier (BRACKET_OPEN identifier BRACKET_CLOSE)? COLON dbOperation PRIMARY_KEY?
;
dbTabularFunction:                              TABULAR_FUNC relationalIdentifier
                                                    PAREN_OPEN
                                                        (columnDefinition (COMMA columnDefinition)*)?
                                                    PAREN_CLOSE
;
dbFilter:                                       FILTER identifier PAREN_OPEN dbOperation PAREN_CLOSE
;
dbMultiGrainFilter:                             MULTIGRAIN_FILTER identifier PAREN_OPEN dbOperation PAREN_CLOSE
;
dbJoin:                                         JOIN identifier PAREN_OPEN dbOperation PAREN_CLOSE
;

// Database operations
dbOperation:                                    dbBooleanOperation | dbJoinOperation
;
dbBooleanOperation:                             dbAtomicOperation dbBooleanOperationRight?
;
dbBooleanOperationRight:                        dbBooleanOperator dbOperation
;
dbBooleanOperator:                              RELATIONAL_AND | RELATIONAL_OR
;
dbAtomicOperation:                              (
                                                    dbGroupOperation
                                                    | ( databasePointer? dbFunctionOperation )
                                                    | dbColumnOperation
                                                    | dbJoinOperation
                                                    | dbConstant
                                                )
                                                dbAtomicOperationRight?
;
dbAtomicOperationRight:                         (dbAtomicOperator dbAtomicOperation) | dbAtomicSelfOperator
;
dbAtomicOperator:                               EQUAL | TEST_NOT_EQUAL | NOT_EQUAL | GREATER_THAN | LESS_THAN | GREATER_OR_EQUAL | LESS_OR_EQUAL
;
dbAtomicSelfOperator:                           IS_NULL | IS_NOT_NULL
;
dbGroupOperation:                               PAREN_OPEN dbOperation PAREN_CLOSE
;
dbConstant:                                     STRING | INTEGER | FLOAT
;
dbFunctionOperation:                            identifier PAREN_OPEN (dbFunctionOperationArgument (COMMA dbFunctionOperationArgument)*)? PAREN_CLOSE
;
dbFunctionOperationArgument:                    dbOperation | dbFunctionOperationArgumentArray
;
dbFunctionOperationArgumentArray:               BRACKET_OPEN (dbFunctionOperationArgument (COMMA dbFunctionOperationArgument)*)? BRACKET_CLOSE
;
dbColumnOperation:                              databasePointer? dbTableAliasColumnOperation
;
dbTableAliasColumnOperation:                    dbTableAliasColumnOperationWithTarget | dbTableAliasColumnOperationWithScopeInfo
;
dbTableAliasColumnOperationWithTarget:          TARGET DOT relationalIdentifier
;
dbTableAliasColumnOperationWithScopeInfo:       relationalIdentifier (DOT scopeInfo)?
;
dbJoinOperation:                                databasePointer? joinSequence (PIPE (dbBooleanOperation | dbTableAliasColumnOperation))?
;
joinSequence:                                   (PAREN_OPEN identifier PAREN_CLOSE)? joinPointer (GREATER_THAN joinPointerFull)*
;
joinPointer:                                    AT identifier
;
joinPointerFull:                                (PAREN_OPEN identifier PAREN_CLOSE)? databasePointer? joinPointer
;
scopeInfo:                                      relationalIdentifier (DOT relationalIdentifier)?
;
databasePointer:                                BRACKET_OPEN qualifiedName BRACKET_CLOSE
;
relationalIdentifier:                           identifier | QUOTED_STRING
;
schemaIdentifier:                               identifier | QUOTED_STRING
;


// =============================================================================
// RELATIONAL DATABASE CONNECTION (from RelationalDatabaseConnectionParserGrammar)
// =============================================================================

relationalDatabaseConnection:                   RELATIONAL_DATABASE_CONNECTION qualifiedName
                                                BRACE_OPEN
                                                    (
                                                        dbConnectionStore
                                                        | dbConnectionType
                                                        | dbConnectionSpec
                                                        | dbConnectionAuth
                                                        | dbConnectionTimezone
                                                        | dbConnectionQuoteIdentifiers
                                                    )*
                                                BRACE_CLOSE
;
dbConnectionStore:                              STORE COLON qualifiedName SEMI_COLON
;
dbConnectionType:                               TYPE COLON identifier SEMI_COLON
;
dbConnectionSpec:                               RELATIONAL_DATASOURCE_SPEC COLON dbConnectionSpecValue SEMI_COLON
;
dbConnectionAuth:                               RELATIONAL_AUTH_STRATEGY COLON dbConnectionAuthValue SEMI_COLON
;
dbConnectionTimezone:                           DB_TIMEZONE COLON TIMEZONE_VALUE SEMI_COLON
;
dbConnectionQuoteIdentifiers:                   QUOTE_IDENTIFIERS COLON BOOLEAN SEMI_COLON
;
dbConnectionSpecValue:                          identifier dbConnectionValueBody?
;
dbConnectionAuthValue:                          identifier dbConnectionValueBody?
;
dbConnectionValueBody:                          BRACE_OPEN dbConnectionBodyContent* BRACE_CLOSE
;
dbConnectionBodyContent:                        ~(BRACE_OPEN | BRACE_CLOSE) | BRACE_OPEN dbConnectionBodyContent* BRACE_CLOSE
;

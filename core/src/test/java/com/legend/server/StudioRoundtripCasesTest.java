package com.legend.server;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Cases of legend-studio's manual grammar roundtrip suite (packages/legend-manual-tests/src/__tests__/roundtrip-grammar
 * /cases), each as Studio runs it: the text parsed to JSON without source information, the JSON printed back in the
 * STANDARD style byte for byte, and the JSON compiled, as legend-engine 4.145.0 answers all three (neema2/legend-lite#113,
 * the fork's studio-compatibility-dev). Each case is the suite's file, verbatim.
 */
class StudioRoundtripCasesTest {

    private static void roundtrips(String text) {
        PureV1Api.Answer json = PureV1Api.grammarToJsonModel(text, false);
        assertEquals(200, json.status(), json.json());
        PureV1Api.Answer printed = PureV1Api.jsonToGrammarModel(json.json(), "STANDARD");
        assertEquals(200, printed.status(), printed.json());
        assertEquals(text, printed.json());
        PureV1Api.Answer compiled = PureV1Api.compile(json.json());
        assertEquals("{\"message\":\"OK\",\"defects\":[]}", compiled.json());
        assertEquals(200, compiled.status());
    }

    /**
     * A View's ~filter through joins, each with its join type: {@code ~filter [db] (INNER) @J | [db] F} and
     * {@code ~filter [db] @J > (OUTER) [db]@K | [db] F}. Lite failed at the parse, "[49:48] expected identifier, got
     * PAREN_OPEN" (2026-11-16).
     */
    @Test
    void relationalDatabaseOperationsAndJoinTypes() {
        roundtrips("""
###Relational
Database model::store::OperationsDatabase
(
  Table PersonTable
  (
    ID INTEGER PRIMARY KEY,
    FIRSTNAME VARCHAR(200),
    LASTNAME VARCHAR(200),
    AGE INTEGER,
    SALARY DOUBLE,
    STATUS VARCHAR(20),
    FIRMID INTEGER,
    ADDRESSID INTEGER,
    MANAGERID INTEGER
  )
  Table FirmTable
  (
    ID INTEGER PRIMARY KEY,
    LEGALNAME VARCHAR(200),
    ADDRESSID INTEGER,
    TYPE VARCHAR(20)
  )
  Table AddressTable
  (
    ID INTEGER PRIMARY KEY,
    STREET VARCHAR(200),
    CITY VARCHAR(100),
    ZIPCODE VARCHAR(10)
  )

  View PersonOperationsView
  (
    ~filter ActivePersonFilter
    id: PersonTable.ID PRIMARY KEY,
    firstName: PersonTable.FIRSTNAME,
    ageGroup: case(PersonTable.AGE < 18, 'minor', PersonTable.AGE >= 65, 'senior', 'adult'),
    salaryOrNull: case(PersonTable.SALARY > 0.5, PersonTable.SALARY, sqlNull()),
    hasManager: case(PersonTable.MANAGERID is null, 'N', 'Y'),
    isMinorOrUnknown: case(PersonTable.AGE is null or PersonTable.AGE < 18, 'Y', 'N'),
    statusCode: case(PersonTable.STATUS = 'ACTIVE', 1, PersonTable.STATUS != 'INACTIVE', 2, PersonTable.STATUS <> 'DELETED', 3, sqlNull()),
    fullName: concat(PersonTable.FIRSTNAME, ' ', PersonTable.LASTNAME),
    firmName: [model::store::OperationsDatabase]@Person_Firm | FirmTable.LEGALNAME,
    firmCity: [model::store::OperationsDatabase] (INNER) @Person_Firm > (OUTER) [model::store::OperationsDatabase]@Firm_Address | AddressTable.CITY,
    personCity: [model::store::OperationsDatabase] (OUTER) @Person_Address | AddressTable.CITY,
    managerFirmName: [model::store::OperationsDatabase]@Person_Manager > (INNER) [model::store::OperationsDatabase]@Person_Firm | FirmTable.LEGALNAME
  )
  View PersonInFirmTypeView
  (
    ~filter [model::store::OperationsDatabase] (INNER) @Person_Firm | [model::store::OperationsDatabase]FirmTypeFilter
    id: PersonTable.ID PRIMARY KEY,
    lastName: PersonTable.LASTNAME
  )
  View PersonWithAddressView
  (
    ~filter [model::store::OperationsDatabase] @Person_Firm > (OUTER) [model::store::OperationsDatabase]@Firm_Address | [model::store::OperationsDatabase]CityFilter
    id: PersonTable.ID PRIMARY KEY
  )

  Join Person_Firm(PersonTable.FIRMID = FirmTable.ID)
  Join Firm_Address(FirmTable.ADDRESSID = AddressTable.ID and AddressTable.STREET is not null)
  Join Person_Address(PersonTable.ADDRESSID = AddressTable.ID and (AddressTable.CITY <> 'Unknown' or AddressTable.ZIPCODE != '00000'))
  Join Person_Manager(PersonTable.MANAGERID = {target}.ID and {target}.ID != PersonTable.ID)

  Filter ActivePersonFilter(PersonTable.STATUS <> 'INACTIVE' and PersonTable.AGE >= 18 and PersonTable.AGE <= 120)
  Filter NullFilter(PersonTable.FIRSTNAME is null or PersonTable.LASTNAME is not null)
  Filter NotEqualFilter(PersonTable.STATUS != 'DELETED')
  Filter GroupedFilter((PersonTable.AGE > 18 and PersonTable.AGE < 65) or (PersonTable.STATUS = 'ACTIVE' and PersonTable.SALARY >= 1000.5))
  Filter NestedGroupFilter(PersonTable.ID > 0 and (PersonTable.STATUS = 'ACTIVE' or (PersonTable.STATUS = 'ON_LEAVE' and PersonTable.AGE < 60)))
  Filter FirmTypeFilter(FirmTable.TYPE <> 'LLC' and FirmTable.LEGALNAME is not null)
  Filter CityFilter(AddressTable.CITY is not null and AddressTable.CITY != '')
)
                """);
    }

    /**
     * A relational property mapping's {@code if(c, 'LARGE', sqlNull())}: the engine types the store's NULL literal as
     * {@code Other}, absorbed by the other branch's type. Lite failed at the compile, "property 'sizeBucket' of
     * 'model::domain::Trade': expected String, got meta::pure::metamodel::type::Any" (2026-11-16).
     */
    @Test
    void relationalMappingJoinChainsAndDistinct() {
        roundtrips("""
###Relational
Database model::store::TradeDB
(
  Schema trading
  (
    Table TradeTable
    (
      ID INTEGER PRIMARY KEY,
      ACCOUNT_ID INTEGER,
      QUANTITY INTEGER,
      STATUS_CODE VARCHAR(10),
      PRODUCT_NAME VARCHAR(200)
    )
    Table AccountTable
    (
      ID INTEGER PRIMARY KEY,
      FIRM_ID INTEGER,
      ACCOUNT_NAME VARCHAR(200),
      ACTIVE_FLAG VARCHAR(1)
    )
    Table FirmTable
    (
      ID INTEGER PRIMARY KEY,
      LEGAL_NAME VARCHAR(200),
      REGION VARCHAR(20)
    )
  )

  Join TradeAccount(trading.TradeTable.ACCOUNT_ID = trading.AccountTable.ID)
  Join AccountFirm(trading.AccountTable.FIRM_ID = trading.FirmTable.ID)

  Filter ActiveAccountFilter(trading.AccountTable.ACTIVE_FLAG = 'Y')
)


###Pure
Enum model::domain::TradeStatus
{
  OPEN,
  CLOSED,
  UNKNOWN
}

Class model::domain::Trade
{
  id: Integer[1];
  quantity: Integer[1];
  description: String[0..1];
  accountName: String[0..1];
  firmName: String[0..1];
  firmRegion: String[0..1];
  status: model::domain::TradeStatus[0..1];
  sizeBucket: String[0..1];
}

Class model::domain::ActiveTrade
{
  id: Integer[1];
  accountName: String[0..1];
}


###Mapping
Mapping model::mapping::TradeMapping
(
  *model::domain::Trade[trade]: Relational
  {
    ~distinct
    ~primaryKey
    (
      [model::store::TradeDB]trading.TradeTable.ID
    )
    ~mainTable [model::store::TradeDB]trading.TradeTable
    +accountId: Integer[0..1]: [model::store::TradeDB]trading.TradeTable.ACCOUNT_ID,
    id: [model::store::TradeDB]trading.TradeTable.ID,
    quantity: [model::store::TradeDB]trading.TradeTable.QUANTITY,
    description: concat([model::store::TradeDB]trading.TradeTable.PRODUCT_NAME, ' - ', [model::store::TradeDB]trading.TradeTable.STATUS_CODE),
    accountName: [model::store::TradeDB]@TradeAccount | [model::store::TradeDB]trading.AccountTable.ACCOUNT_NAME,
    firmName: [model::store::TradeDB]@TradeAccount > (OUTER) [model::store::TradeDB]@AccountFirm | [model::store::TradeDB]trading.FirmTable.LEGAL_NAME,
    firmRegion: [model::store::TradeDB]@TradeAccount > (INNER) [model::store::TradeDB]@AccountFirm | [model::store::TradeDB]trading.FirmTable.REGION,
    status: EnumerationMapping TradeStatusMapping: case(equal([model::store::TradeDB]trading.TradeTable.STATUS_CODE, 'OPEN'), 'O', equal([model::store::TradeDB]trading.TradeTable.STATUS_CODE, 'CLOSED'), 'C', 'U'),
    sizeBucket: if(greaterThan([model::store::TradeDB]trading.TradeTable.QUANTITY, 1000), 'LARGE', sqlNull())
  }
  *model::domain::ActiveTrade[activeTrade]: Relational
  {
    ~filter [model::store::TradeDB] (INNER) @TradeAccount | [model::store::TradeDB]ActiveAccountFilter
    ~primaryKey
    (
      [model::store::TradeDB]trading.TradeTable.ID
    )
    ~mainTable [model::store::TradeDB]trading.TradeTable
    id: [model::store::TradeDB]trading.TradeTable.ID,
    accountName: [model::store::TradeDB]@TradeAccount | [model::store::TradeDB]trading.AccountTable.ACCOUNT_NAME
  }

  model::domain::TradeStatus: EnumerationMapping TradeStatusMapping
  {
    OPEN: ['O'],
    CLOSED: ['C'],
    UNKNOWN: ['U']
  }
)
                """);
    }

    /**
     * A MongoDBConnection naming two servers: legend-engine 4.145.0 compiles the list and seeds its client with all of
     * them. Lite failed at the compile, 500, "UnsupportedOperationException: MongoDBConnection
     * 'model::conn::MongoConnection' names 2 servers: one server is supported (a replica set's list is not yet)"
     * (2026-11-16).
     */
    @Test
    void unknownElementsStoresAndConnections() {
        roundtrips("""
###Deephaven
Deephaven model::warehouse::TradeStore
(
    Table trades
    (
      id: INT,
      symbol: STRING,
      isBuy: BOOLEAN,
      tradeTime: DATETIME,
      price: FLOAT,
      amount: DOUBLE,
      settled: TIMESTAMP,
      notional: DECIMAL(10, 2)
    )
)

DeephavenApp model::TradeApp
{
  applicationName: 'TradesApp';
  function: model::answer():Integer[1];
  description: 'A sample Deephaven app';
  ownership: Deployment { identifier: '12345' };
}


###Elasticsearch
Elasticsearch7Cluster model::warehouse::SearchStore
{
  indices: [
    index1: {
      properties: [
        prop1: Keyword,
        prop2: Text,
        prop3: Date,
        prop4: Integer,
        prop5: Boolean
      ];
    },
    'index-quoted': {
      properties: [
        'prop-quoted': Keyword {
          fields: [
            nested1: Text
          ];
        }
      ];
    }
  ];
}



###MongoDB
Database model::warehouse::MongoStore
(
  Collection Person
  (
    validationLevel: strict;
    validationAction: error;
    jsonSchema: {
      "bsonType": "object",
      "title": "Record of Person",
      "properties": {
        "name": {
          "bsonType": "string",
          "description": "name of the person",
          "minLength": 2
        }
      },
      "additionalProperties": false
    };
  )
)


###Pure
Class model::Person
{
  name: String[1];
}

function model::answer(): Integer[1]
{
  42
}


###ExternalFormat
SchemaSet model::PersonSchemaSet
{
  format: JSON;
  schemas: [
    {
      location: 'model/Person.json';
      content: '{"title":"model::Person","type":"object","properties":{"name":{"type":"string"}},"required":["name"]}';
    }
  ];
}

Binding model::PersonBinding
{
  schemaSet: model::PersonSchemaSet;
  contentType: 'application/json';
  modelIncludes: [
    model::Person
  ];
}


###Mapping
Mapping model::MongoMapping
(
  *model::Person[person]: MongoDB
  {
    ~mainCollection [model::warehouse::MongoStore] Person
    ~binding model::PersonBinding
  }
)


###Connection
MongoDBConnection model::conn::MongoConnection
{
  database: legend_db;
  store: model::warehouse::MongoStore;
  serverURLs: [localhost:27071, myhost.example.com:27071];
  authentication: # UserPassword {
    username: 'mongo_ro';
    password: SystemPropertiesSecret
    {
      systemPropertyName: 'sys.prop.name';
    };
  }#;
}
                """);
    }
}

package com.gs.legend.parser;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class QueryParserDebugTest {

    @Test
    void handRolledParserSmoke() {
        // Verify the hand-rolled parser handles key constructs
        assertNotNull(PureQueryParser.parseQuery("|Person.all()->graphFetch(#{ Person { firstName, lastName } }#)"));
        assertNotNull(PureQueryParser.parseQuery("|%2024-06-15->year()"));
        assertNotNull(PureQueryParser.parseQuery("#TDS\nname, age\nAlice, 30\n#->cast(@Relation<(name:String, age:Integer)>)"));
    }
}

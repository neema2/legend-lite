"""changed.py's element comparison: which edits list one service, and which list every testable."""
import unittest

from changed import compare, elements

_BASE = """###Pure
Class <<meta::pure::profiles::temporal.businesstemporal>> {meta::pure::profiles::doc.doc = 'x'} a::Trade
{
  id: String[1];
}

function a::f(x: String[1]): String[1] { $x }
function a::f(x: Integer[1]): Integer[1] { $x }

###Data
Data a::Seed
{
  Relational #{ default.T: 'ID\\n1\\n'; }#
}

###Service
Service stress::One
{
  pattern: '/one';
  testSuites: [ s: { tests: [ t: { asserts: [ a: EqualToJson
    #{ expected: ExternalFormat #{ data: '[1]'; }#; }# ] } ] } ]
}
// a comment between two services
Service stress::Two
{
  pattern: '/two';
  testSuites: [ s: { tests: [ t: { asserts: [ a: EqualToJson
    #{ expected: ExternalFormat #{ data: '[2]'; }#; }# ] } ] } ]
}
"""


class ElementsTest(unittest.TestCase):

    def test_each_element_by_path_past_stereotypes_and_overloads_by_header(self):
        keys = set(elements(_BASE))
        self.assertEqual({"a::Trade", "a::Seed", "stress::One", "stress::Two",
                          "function a::f(x: String[1]): String[1]", "function a::f(x: Integer[1]): Integer[1]"}, keys)

    def test_an_expected_result_change_lists_its_service_alone(self):
        after = _BASE.replace("'[2]'", "'[2,3]'")
        self.assertEqual((["stress::Two"], []), compare(elements(_BASE), elements(after)))

    def test_a_new_service_is_listed_and_a_new_class_lists_nothing(self):
        after = _BASE + "Service stress::Three\n{\n  testSuites: [ ]\n}\n###Pure\nClass a::New\n{\n}\n"
        self.assertEqual((["stress::Three"], []), compare(elements(_BASE), elements(after)))

    def test_a_seed_change_names_the_seed(self):
        after = _BASE.replace("'ID\\n1\\n'", "'ID\\n1\\n2\\n'")
        self.assertEqual(([], ["a::Seed"]), compare(elements(_BASE), elements(after)))

    def test_a_deleted_class_is_a_change_and_a_deleted_service_is_not(self):
        after = _BASE.replace("Service stress::One", "Service stress::Gone").replace(
            "Class <<meta::pure::profiles::temporal.businesstemporal>> {meta::pure::profiles::doc.doc = 'x'} a::Trade",
            "Class a::Renamed")
        services, others = compare(elements(_BASE), elements(after))
        self.assertEqual((["stress::Gone"], ["a::Trade"]), (services, others))

    def test_comments_line_endings_and_trailing_space_change_nothing(self):
        after = _BASE.replace("// a comment between two services", "// another comment").replace("\n", "  \r\n")
        self.assertEqual(([], []), compare(elements(_BASE), elements(after)))


if __name__ == "__main__":
    unittest.main()
